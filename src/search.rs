//! Multi-engine search: builds the SERP URL, runs the engine-specific
//! extraction script inside Obscura, then normalises redirect wrappers.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use base64::engine::general_purpose::{STANDARD_NO_PAD, URL_SAFE_NO_PAD};
use base64::Engine as _;
use futures::future::join_all;
use serde::Serialize;
use serde_json::{json, Value};
use url::Url;

use crate::browser::{now_iso, BrowserManager};

const SEARCH_SETTLE_MS: u64 = 500;
const SEARCH_TIMEOUT_MS: u64 = 12_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Engine {
    Google,
    Bing,
    DuckDuckGo,
    YahooJapan,
}

impl Engine {
    pub fn parse(name: &str) -> Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "google" => Ok(Self::Google),
            "bing" => Ok(Self::Bing),
            "duckduckgo" => Ok(Self::DuckDuckGo),
            "yahoojapan" => Ok(Self::YahooJapan),
            other => Err(anyhow!("Unsupported search engine: {other}")),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Google => "Google",
            Self::Bing => "Bing",
            Self::DuckDuckGo => "DuckDuckGo",
            Self::YahooJapan => "YahooJapan",
        }
    }

    fn base_url(self) -> &'static str {
        match self {
            Self::Google => "https://www.google.com/search",
            Self::Bing => "https://www.bing.com/search",
            Self::DuckDuckGo => "https://duckduckgo.com/html/",
            Self::YahooJapan => "https://search.yahoo.co.jp/search",
        }
    }

    fn query_param(self) -> &'static str {
        match self {
            Self::YahooJapan => "p",
            _ => "q",
        }
    }

    fn locale_params(self, locale: &str) -> Vec<(&'static str, String)> {
        let mut parts = locale.split(['-', '_']);
        let lang = parts.next().unwrap_or("").to_ascii_lowercase();
        let region = parts.next().unwrap_or("").to_ascii_uppercase();
        if lang.is_empty() {
            return Vec::new();
        }
        match self {
            Self::Google => {
                let mut params = vec![("hl", lang)];
                if !region.is_empty() {
                    params.push(("gl", region.to_ascii_lowercase()));
                }
                params
            }
            Self::Bing => {
                let mut params = vec![("setlang", lang)];
                if !region.is_empty() {
                    params.push(("cc", region));
                }
                params
            }
            Self::DuckDuckGo => ddg_region_code(locale)
                .map(|kl| vec![("kl", kl.to_string())])
                .unwrap_or_default(),
            Self::YahooJapan => Vec::new(),
        }
    }

    fn extract_js(self) -> &'static str {
        match self {
            Self::Google => GOOGLE_JS,
            Self::Bing => BING_JS,
            Self::DuckDuckGo => DUCKDUCKGO_JS,
            Self::YahooJapan => YAHOO_JP_JS,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SearchHit {
    pub title: String,
    pub url: String,
    pub snippet: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct SearchResponse {
    pub engine: String,
    pub keyword: String,
    pub results: Vec<SearchHit>,
    pub timestamp: String,
}

pub struct SearchManager {
    browser: Arc<BrowserManager>,
}

impl SearchManager {
    pub fn new(browser: Arc<BrowserManager>) -> Self {
        Self { browser }
    }

    pub async fn search(&self, keyword: &str, engine_name: &str) -> Result<SearchResponse> {
        self.search_with_timeout(keyword, engine_name, SEARCH_TIMEOUT_MS)
            .await
    }

    pub async fn search_with_timeout(
        &self,
        keyword: &str,
        engine_name: &str,
        timeout_ms: u64,
    ) -> Result<SearchResponse> {
        let engine = Engine::parse(engine_name)?;
        let url = build_url(engine, keyword, &self.browser.locale());
        log(&format!("Searching {} for \"{}\"", engine.name(), keyword));

        let value = self
            .browser
            .evaluate_page(&url, engine.extract_js(), timeout_ms, SEARCH_SETTLE_MS)
            .await?;

        let raw_results = engine.postprocess(parse_hits(&value));
        let before = raw_results.len();
        let results = filter_irrelevant_hits(keyword, raw_results);
        if results.len() < before {
            log(&format!(
                "{}: filtered {} irrelevant result(s) for \"{}\" (kept {})",
                engine.name(),
                before - results.len(),
                keyword,
                results.len()
            ));
        }
        Ok(SearchResponse {
            engine: engine.name().to_string(),
            keyword: keyword.to_string(),
            results,
            timestamp: now_iso(),
        })
    }

    /// Run several engines concurrently. Failed engines are reported per key,
    /// matching the original `search_multiple` shape.
    pub async fn search_multiple(&self, keyword: &str, engines: &[String]) -> Value {
        let futures = engines
            .iter()
            .map(|engine| async move { (engine.clone(), self.search(keyword, engine).await) });

        let mut out = serde_json::Map::new();
        for (engine, result) in join_all(futures).await {
            match result {
                Ok(response) => {
                    out.insert(engine, json!(response));
                }
                Err(err) => {
                    out.insert(
                        engine.clone(),
                        json!({
                            "error": err.to_string(),
                            "engine": engine,
                            "keyword": keyword,
                            "timestamp": now_iso(),
                        }),
                    );
                }
            }
        }
        Value::Object(out)
    }
}

impl Engine {
    /// Unwrap search-result redirect URLs and drop ads/internal links.
    fn postprocess(self, hits: Vec<SearchHit>) -> Vec<SearchHit> {
        match self {
            Self::Google => hits
                .into_iter()
                .map(|mut hit| {
                    hit.url = decode_google_url(&hit.url);
                    hit
                })
                .filter(|hit| {
                    hit.url.starts_with("http")
                        && !hit.url.contains("google.com/search")
                        && !hit.url.contains("google.com/goto?")
                })
                .collect(),
            Self::Bing => hits
                .into_iter()
                .filter(|hit| !hit.url.contains("/aclick?"))
                .map(|mut hit| {
                    hit.url = decode_bing_url(&hit.url);
                    hit.snippet = clean_bing_snippet(&hit.snippet);
                    hit
                })
                .filter(|hit| hit.url.starts_with("http") && !is_bing_internal(&hit.url))
                .collect(),
            Self::DuckDuckGo => hits
                .into_iter()
                .filter(|hit| !hit.url.contains("duckduckgo.com/y.js"))
                .map(|mut hit| {
                    hit.url = decode_ddg_url(&hit.url);
                    hit
                })
                .filter(|hit| hit.url.starts_with("http") && !hit.url.contains("duckduckgo.com"))
                .collect(),
            Self::YahooJapan => hits
                .into_iter()
                .filter(|hit| hit.url.starts_with("http"))
                .collect(),
        }
    }
}

fn ddg_region_code(locale: &str) -> Option<&'static str> {
    match locale.to_ascii_lowercase().replace('_', "-").as_str() {
        "ja-jp" | "ja" => Some("jp-jp"),
        "en-us" => Some("us-en"),
        "en-gb" => Some("uk-en"),
        "zh-cn" => Some("cn-zh"),
        "ko-kr" => Some("kr-kr"),
        "de-de" => Some("de-de"),
        "fr-fr" => Some("fr-fr"),
        "es-es" => Some("es-es"),
        "it-it" => Some("it-it"),
        "pt-br" => Some("br-pt"),
        "ru-ru" => Some("ru-ru"),
        _ => None,
    }
}

fn build_url(engine: Engine, keyword: &str, locale: &str) -> String {
    let mut url = Url::parse(engine.base_url()).expect("static search engine URL");
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair(engine.query_param(), keyword);
        for (key, value) in engine.locale_params(locale) {
            pairs.append_pair(key, &value);
        }
    }
    url.to_string()
}

fn parse_hits(value: &Value) -> Vec<SearchHit> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let title = item.get("title")?.as_str()?.trim().to_string();
                    let url = item.get("url")?.as_str()?.trim().to_string();
                    if title.is_empty() || url.is_empty() {
                        return None;
                    }
                    Some(SearchHit {
                        title,
                        url,
                        snippet: item
                            .get("snippet")
                            .and_then(|s| s.as_str())
                            .unwrap_or("")
                            .to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn decode_bing_url(raw: &str) -> String {
    let Ok(url) = Url::parse(raw) else {
        return raw.to_string();
    };
    let Some(encoded) = url
        .query_pairs()
        .find(|(key, _)| key == "u")
        .map(|(_, value)| value.into_owned())
    else {
        return raw.to_string();
    };

    if raw.contains("/ck/a?") {
        let cleaned = encoded.strip_prefix("a1").unwrap_or(&encoded);
        if let Ok(bytes) = STANDARD_NO_PAD
            .decode(cleaned)
            .or_else(|_| URL_SAFE_NO_PAD.decode(cleaned))
        {
            if let Ok(decoded) = String::from_utf8(bytes) {
                if decoded.starts_with("http") {
                    return decoded;
                }
                if decoded.starts_with('/') {
                    if let Ok(base) = Url::parse("https://www.bing.com") {
                        if let Ok(joined) = base.join(&decoded) {
                            return joined.to_string();
                        }
                    }
                }
            }
        }
    } else if raw.contains("/aclick?") && encoded.starts_with("http") {
        return encoded;
    }

    raw.to_string()
}

fn decode_ddg_url(raw: &str) -> String {
    let Ok(url) = Url::parse(raw) else {
        return raw.to_string();
    };
    if !raw.contains("/l/") {
        return raw.to_string();
    }
    url.query_pairs()
        .find(|(key, _)| key == "uddg")
        .map(|(_, value)| value.into_owned())
        .unwrap_or_else(|| raw.to_string())
}

fn decode_google_url(raw: &str) -> String {
    let Ok(url) = Url::parse(raw) else {
        return raw.to_string();
    };
    if !raw.contains("google.com/url?") {
        return raw.to_string();
    }
    url.query_pairs()
        .find(|(key, _)| key == "q" || key == "url")
        .map(|(_, value)| value.into_owned())
        .filter(|target| target.starts_with("http"))
        .unwrap_or_else(|| raw.to_string())
}

fn clean_bing_snippet(snippet: &str) -> String {
    let mut s = snippet.trim();
    for suffix in ["…詳細を表示", "...詳細を表示", "詳細を表示", "…詳細"] {
        if let Some(stripped) = s.strip_suffix(suffix) {
            s = stripped.trim_end();
        }
    }
    s.to_string()
}

fn is_bing_internal(url: &str) -> bool {
    const INTERNAL: [&str; 5] = [
        "/shop?",
        "/videos/search",
        "/images/search",
        "/news/search",
        "/search?",
    ];
    url.contains("bing.com") && INTERNAL.iter().any(|path| url.contains(path))
}

fn normalize_for_match(input: &str) -> String {
    input
        .chars()
        .map(|c| {
            // Fold full-width ASCII variants (e.g. ２０２６ -> 2026)
            if ('\u{FF01}'..='\u{FF5E}').contains(&c) {
                char::from_u32(c as u32 - 0xFEE0).unwrap_or(c)
            } else if c == '\u{3000}' {
                ' '
            } else {
                c
            }
        })
        .collect::<String>()
        .to_ascii_lowercase()
}

fn is_date_or_numeric_term(term: &str) -> bool {
    let stripped: String = term
        .chars()
        .filter(|c| !matches!(c, '0'..='9' | '年' | '月' | '日' | '時' | '分' | '-' | '/' | '.' | ':' | '～' | '~'))
        .collect();
    stripped.is_empty()
}

fn is_stop_term(term: &str) -> bool {
    if term.is_empty() {
        return true;
    }
    if term.is_ascii() && term.len() <= 1 {
        return true;
    }
    matches!(
        term,
        "the" | "and" | "for" | "with" | "from" | "that" | "this" | "are" | "was" | "were"
            | "of" | "in" | "to" | "on" | "at" | "by" | "an" | "or" | "is" | "as"
    )
}

/// Extract `(all_terms, content_terms)` from a search query.
/// `content_terms` excludes purely numeric/date tokens (like `2026` or `2026年4月`)
/// so engines that silently relax multi-word queries down to a year token (e.g.
/// Bing returning generic 2026 calendars) are caught reliably.
pub fn extract_query_terms(keyword: &str) -> (Vec<String>, Vec<String>) {
    let normalized = normalize_for_match(keyword);
    let mut all_terms = Vec::new();
    let mut content_terms = Vec::new();

    for raw in normalized.split(|c: char| {
        c.is_whitespace()
            || matches!(
                c,
                '・' | '/' | '、' | '。' | ',' | '「' | '」' | '『' | '』' | '(' | ')' | '（' | '）' | '"' | '\'' | '|'
            )
    }) {
        let term = raw.trim();
        if is_stop_term(term) {
            continue;
        }
        let owned = term.to_string();
        if !all_terms.contains(&owned) {
            if !is_date_or_numeric_term(&owned) {
                content_terms.push(owned.clone());
            }
            all_terms.push(owned);
        }
    }

    (all_terms, content_terms)
}

/// Count how many distinct query terms appear in a hit's title, snippet, or URL.
pub fn relevance_score(keyword: &str, hit: &SearchHit) -> usize {
    let (all_terms, content_terms) = extract_query_terms(keyword);
    if all_terms.is_empty() {
        return 0;
    }
    let haystack = normalize_for_match(&format!("{} {} {}", hit.title, hit.snippet, hit.url));
    let terms = if !content_terms.is_empty() {
        &content_terms
    } else {
        &all_terms
    };
    terms.iter().filter(|t| haystack.contains(t.as_str())).count()
}

/// Drop clearly off-topic hits (such as Bing's soft-block query-relaxation pages)
/// when a multi-term query has non-numeric content terms and a hit matches none.
pub fn filter_irrelevant_hits(keyword: &str, hits: Vec<SearchHit>) -> Vec<SearchHit> {
    let (all_terms, content_terms) = extract_query_terms(keyword);
    if all_terms.len() < 2 || content_terms.is_empty() {
        return hits;
    }
    hits.into_iter()
        .filter(|hit| {
            let haystack =
                normalize_for_match(&format!("{} {} {}", hit.title, hit.snippet, hit.url));
            content_terms
                .iter()
                .any(|term| haystack.contains(term.as_str()))
        })
        .collect()
}

fn log(message: &str) {
    eprintln!("[webmcp] {message}");
}

const GOOGLE_JS: &str = r#"(() => {
  const out = [];
  const seen = new Set();
  const container = document.querySelector('#rso') || document.querySelector('#search') || document.body;
  if (!container) return out;
  container.querySelectorAll('a h3').forEach(h3 => {
    const link = h3.closest('a[href]');
    if (!link) return;
    const href = link.href;
    const title = (h3.textContent || '').replace(/\s+/g, ' ').trim();
    const isGoogleRedirect = href.includes('google.com/url?') || href.includes('google.com/goto?');
    const isGoogleInternal = href.includes('google.com') && !isGoogleRedirect;
    if (!href || !title || isGoogleInternal || seen.has(href)) return;
    seen.add(href);
    let snippet = '';
    const card = h3.closest('div.N54PNb, div.MjjYud, div.g, div[data-hveid]');
    if (card) {
      for (const sel of ['.VwiC3b', '[data-sncf="1"]', '[data-sncf]', '.IsZvec', '.lEBKkf', 'span.aCOpRe']) {
        const el = card.querySelector(sel);
        if (el) {
          const t = (el.textContent || '').replace(/\s+/g, ' ').trim();
          if (t.length > 15) { snippet = t.substring(0, 350); break; }
        }
      }
    }
    out.push({ title: title.substring(0, 200), url: href, snippet });
  });
  return out.slice(0, 10);
})()"#;

const BING_JS: &str = r#"(() => {
  const out = [];
  const seen = new Set();
  const extractSnippet = (container, title) => {
    if (!container) return '';
    for (const sel of ['.b_caption p', '.b_lineclamp2', '.b_lineclamp3', '.b_lineclamp4', '.b_algoSlug', 'p']) {
      const el = container.querySelector(sel);
      if (el) {
        const text = (el.textContent || '').replace(/\s+/g, ' ').trim();
        if (text.length > 15 && text !== title) return text.substring(0, 350);
      }
    }
    return '';
  };

  // Strategy 1: Standard organic results inside li.b_algo (avoids sidebar/carousel/ad h2s)
  document.querySelectorAll('li.b_algo').forEach(li => {
    const link = li.querySelector('h2 a[href]');
    if (!link) return;
    const href = link.href;
    const title = (link.textContent || '').replace(/\s+/g, ' ').trim();
    if (!href || !title || href.includes('/aclick?') || seen.has(href)) return;
    const isRedirect = href.includes('/ck/a?');
    const isBingNav = href.includes('bing.com') && !isRedirect;
    if (isBingNav) return;
    seen.add(href);
    out.push({ title: title.substring(0, 200), url: href, snippet: extractSnippet(li, title) });
  });

  // Strategy 2: Fallback if Bing changes li.b_algo markup
  if (out.length === 0) {
    document.querySelectorAll('#b_results h2 a[href], h2 a[href]').forEach(link => {
      const href = link.href;
      const title = (link.textContent || '').replace(/\s+/g, ' ').trim();
      if (!href || title.length <= 5 || href.includes('/aclick?') || seen.has(href)) return;
      const isRedirect = href.includes('/ck/a?');
      const isBingNav = href.includes('bing.com') && !isRedirect;
      if (isBingNav) return;
      seen.add(href);
      const container = link.closest('li, div');
      out.push({ title: title.substring(0, 200), url: href, snippet: extractSnippet(container, title) });
    });
  }
  return out.slice(0, 10);
})()"#;

const DUCKDUCKGO_JS: &str = r#"(() => {
  const out = [];
  const seen = new Set();
  document.querySelectorAll('.result').forEach(el => {
    if (el.classList.contains('result--ad') || el.querySelector('.badge--ad')) return;
    const titleEl = el.querySelector('.result__a');
    const snippetEl = el.querySelector('.result__snippet');
    if (!titleEl) return;
    const rawHref = titleEl.getAttribute('href') || titleEl.href;
    if (!rawHref || rawHref.includes('duckduckgo.com/y.js') || seen.has(rawHref)) return;
    seen.add(rawHref);
    let finalUrl = rawHref;
    try {
      if (rawHref.includes('uddg=')) {
        const uddg = new URL(rawHref, location.href).searchParams.get('uddg');
        if (uddg) finalUrl = uddg;
      } else if (rawHref.startsWith('//')) {
        finalUrl = 'https:' + rawHref;
      }
    } catch (e) {}
    if (finalUrl.includes('duckduckgo.com')) return;
    out.push({
      title: (titleEl.textContent || '').replace(/\s+/g, ' ').trim().substring(0, 200),
      url: finalUrl,
      snippet: snippetEl ? (snippetEl.textContent || '').replace(/\s+/g, ' ').trim().substring(0, 350) : ''
    });
  });
  return out.slice(0, 10);
})()"#;

const YAHOO_JP_JS: &str = r#"(() => {
  const out = [];
  const seen = new Set();
  const isInternalYahooUrl = (href) => {
    if (!href || !href.startsWith('http')) return true;
    return (
      href.includes('search.yahoo.co.jp') ||
      href.includes('auctions.yahoo.co.jp') ||
      href.includes('shopping.yahoo.co.jp') ||
      href.includes('rdsig.yahoo.co.jp') ||
      href.includes('yahoo.co.jp/search') ||
      href.includes('advantage.yahoo.co.jp') ||
      href.includes('advertising.yahoo.co.jp') ||
      href.includes('privacy.yahoo.co.jp') ||
      href.includes('about.yahoo.co.jp') ||
      href.includes('help.yahoo.co.jp') ||
      href.includes('login.yahoo.co.jp')
    );
  };

  // Strategy 1: Target organic result cards so sitelinks (#rtoc-*) and breadcrumbs are excluded
  document.querySelectorAll('.sw-Card.Algo, .sw-CardBase').forEach(card => {
    const link = card.querySelector('.sw-Card__title a[href], a.sw-Card__titleInner[href]');
    if (!link) return;
    const href = link.href;
    if (isInternalYahooUrl(href) || seen.has(href)) return;
    const h3 = card.querySelector('.sw-Card__title h3, h3.sw-Card__titleMain, h3');
    let title = h3 ? (h3.textContent || '').replace(/\s+/g, ' ').trim() : '';
    if (!title) title = (link.textContent || '').trim().split('\n')[0].trim();
    if (!title || title.length < 2) return;
    seen.add(href);
    let snippet = '';
    for (const sel of ['.sw-Card__summary', '.sw-Card__section--summary', '.fs-small', '.fc-falcon', '.snippet', 'p']) {
      const el = card.querySelector(sel);
      if (el) {
        const text = (el.textContent || '').replace(/\s+/g, ' ').trim();
        if (text.length > 15 && text !== title) { snippet = text.substring(0, 350); break; }
      }
    }
    out.push({ title: title.substring(0, 200), url: href, snippet });
  });

  // Strategy 2: Fallback if Yahoo Japan changes card markup
  if (out.length === 0) {
    const container = document.querySelector('#web') || document.body;
    container.querySelectorAll('a[href]').forEach(link => {
      const href = link.href;
      if (isInternalYahooUrl(href) || href.includes('yahoo.co.jp') || seen.has(href)) return;
      const h3 = link.querySelector('h3');
      const title = (h3 ? h3.textContent : link.textContent || '').replace(/\s+/g, ' ').trim();
      if (title.length < 8 || title.length > 250) return;
      seen.add(href);
      let snippet = '';
      const parent = link.closest('li, div[data-yjcontainer], .searchResult, section');
      if (parent) {
        const p = parent.querySelector('p');
        if (p) snippet = (p.textContent || '').replace(/\s+/g, ' ').trim().substring(0, 350);
      }
      out.push({ title: title.substring(0, 200), url: href, snippet });
    });
  }
  return out.slice(0, 10);
})()"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_language_aware_urls() {
        assert_eq!(
            build_url(Engine::Google, "rust 検索", "ja-JP"),
            "https://www.google.com/search?q=rust+%E6%A4%9C%E7%B4%A2&hl=ja&gl=jp"
        );
        assert_eq!(
            build_url(Engine::Bing, "rust", "ja-JP"),
            "https://www.bing.com/search?q=rust&setlang=ja&cc=JP"
        );
        assert_eq!(
            build_url(Engine::DuckDuckGo, "rust", "ja-JP"),
            "https://duckduckgo.com/html/?q=rust&kl=jp-jp"
        );
        assert_eq!(
            build_url(Engine::YahooJapan, "x", "ja-JP"),
            "https://search.yahoo.co.jp/search?p=x"
        );
    }

    #[test]
    fn filters_bing_soft_block_calendar_results() {
        let query = "2026年4月 自転車 法改正 施行";
        let hits = vec![
            SearchHit {
                title: "2026年(令和8年)カレンダー｜祝日・六曜".to_string(),
                url: "https://bestcalendar.jp/2026".to_string(),
                snippet: "2026年の年間カレンダー。祝日・六曜を網羅。".to_string(),
            },
            SearchHit {
                title: "2026 - Wikipedia".to_string(),
                url: "https://en.wikipedia.org/wiki/2026".to_string(),
                snippet: "2026 was a common year".to_string(),
            },
            SearchHit {
                title: "2026年4月から自転車の交通違反に「青切符」を導入！".to_string(),
                url: "https://www.gov-online.go.jp/article/202410/entry-6604.html".to_string(),
                snippet: "令和8年（2026年）4月1日から自転車にも青切符制度が適用されます。".to_string(),
            },
        ];
        let filtered = filter_irrelevant_hits(query, hits);
        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered[0].url,
            "https://www.gov-online.go.jp/article/202410/entry-6604.html"
        );
    }

    #[test]
    fn preserves_single_term_and_numeric_queries() {
        let hits = vec![SearchHit {
            title: "2026年カレンダー".to_string(),
            url: "https://bestcalendar.jp/2026".to_string(),
            snippet: "カレンダー".to_string(),
        }];
        assert_eq!(filter_irrelevant_hits("2026", hits.clone()).len(), 1);
        assert_eq!(filter_irrelevant_hits("2026年 4月", hits).len(), 1);
    }

    #[test]
    fn decodes_ddg_redirect() {
        assert_eq!(
            decode_ddg_url("https://duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Fa&rut=1"),
            "https://example.com/a"
        );
        assert_eq!(decode_ddg_url("https://example.com/plain"), "https://example.com/plain");
    }

    #[test]
    fn decodes_bing_redirect() {
        let raw = "https://www.bing.com/ck/a?!&&p=abc&u=a1aHR0cHM6Ly9leGFtcGxlLmNvbS9wYWdl&ntb=1";
        assert_eq!(decode_bing_url(raw), "https://example.com/page");
    }

    #[test]
    fn filters_bing_internal_links() {
        assert!(is_bing_internal("https://www.bing.com/images/search?q=x"));
        assert!(!is_bing_internal("https://example.com/image"));
    }
}
