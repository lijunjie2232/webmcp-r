//! High-level tools: single-page parsing and the multi-engine deep search.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use futures::future::join_all;
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};
use tokio::time::Instant;
use url::Url;

use crate::browser::{BrowserManager, FetchOptions};
use crate::search::{relevance_score, SearchHit, SearchManager};

/// Total wall-clock budget for `deep_search`, leaving headroom under typical
/// 30s MCP tool-call timeouts. Overridable via `WEBMCP_DEEP_SEARCH_TIMEOUT_MS`.
const DEFAULT_DEEP_SEARCH_BUDGET_MS: u64 = 22_000;
const DEEP_SEARCH_ENGINE_TIMEOUT_MS: u64 = 8_000;
const DEEP_SEARCH_NAV_TIMEOUT_MS: u64 = 8_000;
const DEEP_SEARCH_PARSE_TIMEOUT_MS: u64 = 10_000;

#[derive(Clone, Debug)]
pub struct ParseOptions {
    pub max_content_length: usize,
    pub parse_timeout_ms: u64,
    pub max_retries: u32,
    pub navigation_timeout_ms: u64,
    pub wait_until: String,
}

impl Default for ParseOptions {
    fn default() -> Self {
        Self {
            max_content_length: 50_000,
            parse_timeout_ms: 60_000,
            max_retries: 1,
            navigation_timeout_ms: 30_000,
            wait_until: "load".to_string(),
        }
    }
}

pub struct PageParser {
    browser: Arc<BrowserManager>,
}

impl PageParser {
    pub fn new(browser: Arc<BrowserManager>) -> Self {
        Self { browser }
    }

    pub async fn parse(&self, url: &str, opts: &ParseOptions) -> Result<Value> {
        let mut last_error: Option<anyhow::Error> = None;

        for attempt in 0..=opts.max_retries {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
            }
            log(&format!("Parsing {url} (attempt {}/{})", attempt + 1, opts.max_retries + 1));

            let fetch_options = FetchOptions {
                max_content_length: opts.max_content_length,
                navigation_timeout_ms: opts.navigation_timeout_ms,
                settle_ms: settle_ms(&opts.wait_until),
            };
            let fetch = self.browser.fetch_page(url, &fetch_options);

            match tokio::time::timeout(Duration::from_millis(opts.parse_timeout_ms), fetch).await {
                Ok(Ok(page)) => {
                    return Ok(json!({
                        "title": page.title,
                        "content": page.content,
                        "url": page.url,
                        "statusCode": page.status,
                        "isPdf": page.is_pdf,
                        "tag": if page.is_pdf { "pdf" } else { "html" },
                    }))
                }
                Ok(Err(err)) => last_error = Some(err),
                Err(_) => {
                    last_error = Some(anyhow!(
                        "parse timeout after {}ms for {url}",
                        opts.parse_timeout_ms
                    ))
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("parse failed for {url}")))
    }
}

/// Obscura's high-level `goto` always waits for `load`. For `networkidle`
/// callers we settle a little longer; other strategies are already covered.
fn settle_ms(wait_until: &str) -> u64 {
    match wait_until {
        "networkidle" | "networkidle0" => 3_000,
        _ => 0,
    }
}

pub struct DeepSearch {
    search: Arc<SearchManager>,
    parser: Arc<PageParser>,
    engines: Vec<String>,
    max_results: usize,
    parser_num: usize,
}

impl DeepSearch {
    pub fn new(
        search: Arc<SearchManager>,
        parser: Arc<PageParser>,
        engines: Vec<String>,
        max_results: usize,
        parser_num: usize,
    ) -> Self {
        Self {
            search,
            parser,
            engines,
            max_results,
            parser_num,
        }
    }

    pub async fn run(
        &self,
        keyword: &str,
        engines: Option<Vec<String>>,
        max_results: Option<usize>,
        parser_num: Option<usize>,
    ) -> Vec<Value> {
        let engines = engines
            .filter(|list| !list.is_empty())
            .unwrap_or_else(|| self.engines.clone());
        let max = max_results.unwrap_or(self.max_results);
        let concurrency = parser_num.unwrap_or(self.parser_num).max(1);

        let budget_ms = deep_search_budget_ms();
        let deadline = Instant::now() + Duration::from_millis(budget_ms);
        let search_timeout_ms = DEEP_SEARCH_ENGINE_TIMEOUT_MS.min((budget_ms / 2).max(1_000));

        let search = &self.search;
        let futures = engines.iter().map(|engine| async move {
            (
                engine.clone(),
                search
                    .search_with_timeout(keyword, engine, search_timeout_ms)
                    .await,
            )
        });

        let mut raw_hits: Vec<(SearchHit, String)> = Vec::new();
        for (engine, result) in join_all(futures).await {
            match result {
                Ok(response) => raw_hits.extend(
                    response
                        .results
                        .into_iter()
                        .take(max)
                        .map(|hit| (hit, engine.clone())),
                ),
                Err(err) => log(&format!("engine {engine} failed: {err}")),
            }
        }

        let hits = dedupe_and_rank_hits(keyword, raw_hits);
        if hits.is_empty() {
            return Vec::new();
        }

        let mut results: Vec<Option<Value>> = vec![None; hits.len()];
        let parser = &self.parser;

        let mut pending = stream::iter(hits.iter().enumerate())
            .map(|(idx, ranked)| async move {
                let remaining_ms = remaining_ms_until(deadline);
                if remaining_ms < 500 {
                    return (
                        idx,
                        snippet_fallback(ranked, "deep_search time budget exceeded"),
                    );
                }

                let nav_ms = DEEP_SEARCH_NAV_TIMEOUT_MS.min(remaining_ms);
                let parse_ms = DEEP_SEARCH_PARSE_TIMEOUT_MS.min(remaining_ms);
                let opts = ParseOptions {
                    max_retries: 0,
                    navigation_timeout_ms: nav_ms,
                    parse_timeout_ms: parse_ms,
                    ..ParseOptions::default()
                };

                let doc = match parser.parse(&ranked.hit.url, &opts).await {
                    Ok(page) => {
                        let mut obj = json!({
                            "title": page
                                .get("title")
                                .and_then(|v| v.as_str())
                                .filter(|title| !title.is_empty())
                                .unwrap_or(ranked.hit.title.as_str()),
                            "abstract": ranked.hit.snippet,
                            "url": ranked.hit.url,
                            "content": page.get("content").cloned().unwrap_or(json!("")),
                            "engine": ranked.engines[0],
                        });
                        if ranked.engines.len() > 1 {
                            obj["engines"] = json!(ranked.engines);
                        }
                        obj
                    }
                    Err(err) => snippet_fallback(ranked, &err.to_string()),
                };
                (idx, doc)
            })
            .buffer_unordered(concurrency);

        loop {
            match tokio::time::timeout_at(deadline, pending.next()).await {
                Ok(Some((idx, doc))) => results[idx] = Some(doc),
                Ok(None) => break,
                Err(_) => {
                    log(&format!(
                        "deep_search reached {}ms budget; returning collected results",
                        budget_ms
                    ));
                    break;
                }
            }
        }
        drop(pending);

        results
            .into_iter()
            .enumerate()
            .map(|(idx, doc)| {
                doc.unwrap_or_else(|| {
                    snippet_fallback(&hits[idx], "deep_search time budget exceeded")
                })
            })
            .collect()
    }
}

struct RankedHit {
    hit: SearchHit,
    engines: Vec<String>,
    relevance: usize,
    original_rank: usize,
}

fn deep_search_budget_ms() -> u64 {
    std::env::var("WEBMCP_DEEP_SEARCH_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&ms| ms >= 2_000)
        .unwrap_or(DEFAULT_DEEP_SEARCH_BUDGET_MS)
}

fn remaining_ms_until(deadline: Instant) -> u64 {
    deadline
        .saturating_duration_since(Instant::now())
        .as_millis() as u64
}

fn snippet_fallback(ranked: &RankedHit, error: &str) -> Value {
    let mut obj = json!({
        "title": ranked.hit.title,
        "abstract": ranked.hit.snippet,
        "url": ranked.hit.url,
        "content": "",
        "engine": ranked.engines[0],
        "error": error,
    });
    if ranked.engines.len() > 1 {
        obj["engines"] = json!(ranked.engines);
    }
    obj
}

fn normalize_url_key(raw: &str) -> String {
    const TRACKING_PARAMS: [&str; 12] = [
        "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
        "fbclid", "gclid", "yclid", "msclkid", "mc_cid", "mc_eid", "ref",
    ];
    let trimmed = raw.trim();
    if let Ok(mut parsed) = Url::parse(trimmed) {
        parsed.set_fragment(None);
        let kept: Vec<(String, String)> = parsed
            .query_pairs()
            .filter(|(k, _)| !TRACKING_PARAMS.contains(&k.to_ascii_lowercase().as_str()))
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        if kept.is_empty() {
            parsed.set_query(None);
        } else {
            let mut pairs = parsed.query_pairs_mut();
            pairs.clear();
            for (k, v) in kept {
                pairs.append_pair(&k, &v);
            }
        }
        let mut s = parsed.to_string();
        if s.ends_with('/') && parsed.path() != "/" {
            s.pop();
        }
        return s;
    }
    trimmed
        .split('#')
        .next()
        .unwrap_or(trimmed)
        .trim_end_matches('/')
        .to_string()
}

fn dedupe_and_rank_hits(keyword: &str, hits: Vec<(SearchHit, String)>) -> Vec<RankedHit> {
    let mut index_by_key: HashMap<String, usize> = HashMap::new();
    let mut unique: Vec<RankedHit> = Vec::with_capacity(hits.len());

    for (hit, engine) in hits {
        let key = normalize_url_key(&hit.url);
        if let Some(&idx) = index_by_key.get(&key) {
            let existing = &mut unique[idx];
            if !existing.engines.contains(&engine) {
                existing.engines.push(engine);
            }
            if hit.snippet.len() > existing.hit.snippet.len() {
                existing.hit.snippet = hit.snippet;
                existing.relevance = relevance_score(keyword, &existing.hit);
            }
        } else {
            let relevance = relevance_score(keyword, &hit);
            let original_rank = unique.len();
            index_by_key.insert(key, original_rank);
            unique.push(RankedHit {
                hit,
                engines: vec![engine],
                relevance,
                original_rank,
            });
        }
    }

    unique.sort_by(|a, b| {
        b.engines
            .len()
            .cmp(&a.engines.len())
            .then_with(|| b.relevance.cmp(&a.relevance))
            .then_with(|| a.original_rank.cmp(&b.original_rank))
    });
    unique
}

fn log(message: &str) {
    eprintln!("[webmcp] {message}");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(url: &str) -> SearchHit {
        SearchHit {
            title: "t".to_string(),
            url: url.to_string(),
            snippet: "s".to_string(),
        }
    }

    #[test]
    fn dedupes_and_ranks_by_consensus_and_relevance() {
        let input = vec![
            (hit("https://example.com/a"), "bing".to_string()),
            (hit("https://example.com/b"), "bing".to_string()),
            (hit("https://example.com/b#section"), "duckduckgo".to_string()),
            (hit("https://example.com/c"), "duckduckgo".to_string()),
        ];
        let out = dedupe_and_rank_hits("test", input);
        let urls: Vec<&str> = out.iter().map(|r| r.hit.url.as_str()).collect();
        assert_eq!(
            urls,
            vec![
                "https://example.com/b",
                "https://example.com/a",
                "https://example.com/c",
            ]
        );
        assert_eq!(out[0].engines, vec!["bing", "duckduckgo"]);
    }
}
