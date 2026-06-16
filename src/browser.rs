//! Obscura-backed browser manager.
//!
//! Replaces the Playwright + CloakBrowser layer of the original JavaScript
//! project: every page is a fresh Obscura `Page` inside one shared `Browser`.

use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use obscura::{Browser, ResourceType};
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::oneshot;

pub fn now_iso() -> String {
    OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_default()
}

/// Readable text extraction evaluated inside the page.
///
/// Obscura's `innerText` folds `<style>` contents into the result and does not
/// emit block line breaks, so walk the DOM ourselves. Skips chrome/navigation
/// nodes, prefers a main-content container, and preserves `http(s)` hyperlinks
/// inline as Markdown `[text](url)` so LLMs can follow deeper references.
pub const EXTRACT_PAGE_JS: &str = r#"(() => {
  const mainSelectors = ['main','article','[role="main"]','#main-content','.main-content','#entry-content','.entry-content','#post-content','.post-content','#content','.content','.article','.post','section.content','div.content-area'];
  let root = null;
  for (const s of mainSelectors) {
    const candidate = document.querySelector(s);
    if (candidate && (candidate.textContent || '').trim().length >= 200) { root = candidate; break; }
  }
  if (!root) root = document.body;
  if (!root) return { title: document.title, content: '' };
  const blocks = new Set(['P','DIV','SECTION','ARTICLE','MAIN','ASIDE','LI','UL','OL','TABLE','TR','TD','TH','H1','H2','H3','H4','H5','H6','PRE','BLOCKQUOTE','BR','HR','FORM','FIELDSET','FIGURE','FIGCAPTION','DL','DT','DD']);
  const skip = new Set(['SCRIPT','STYLE','NOSCRIPT','TEMPLATE','SVG','IMG','NAV','HEADER','FOOTER','IFRAME','CANVAS','VIDEO','AUDIO']);
  const pageBase = (location.href || '').split('#')[0];
  const resolveLink = (el) => {
    const raw = (el.getAttribute && el.getAttribute('href')) || '';
    if (!raw || raw.startsWith('#') || /^(javascript|mailto|tel):/i.test(raw)) return '';
    let abs = '';
    try { abs = new URL(raw, location.href).href; } catch (e) { abs = el.href || ''; }
    if (!/^https?:\/\//i.test(abs)) return '';
    if (abs.split('#')[0] === pageBase && abs.includes('#')) return '';
    return abs;
  };
  const collectText = (node, buf) => {
    if (node.nodeType === 3) { buf.push(node.nodeValue); return; }
    if (node.nodeType !== 1) return;
    if (skip.has(node.tagName)) return;
    for (let c = node.firstChild; c; c = c.nextSibling) collectText(c, buf);
  };
  const out = [];
  const walk = (node) => {
    if (node.nodeType === 3) { out.push(node.nodeValue); return; }
    if (node.nodeType !== 1) return;
    const tag = node.tagName;
    if (skip.has(tag)) return;
    if (tag === 'A') {
      const href = resolveLink(node);
      const buf = [];
      for (let c = node.firstChild; c; c = c.nextSibling) collectText(c, buf);
      const label = buf.join(' ').replace(/[ \t\r\n\u00a0]+/g, ' ').trim();
      if (href && label) {
        out.push(label.includes(href) ? label : '[' + label + '](' + href + ')');
      } else if (label) {
        out.push(label);
      }
      return;
    }
    const isBlock = blocks.has(tag);
    if (isBlock) out.push('\n');
    for (let c = node.firstChild; c; c = c.nextSibling) walk(c);
    if (isBlock) out.push('\n');
  };
  for (let c = root.firstChild; c; c = c.nextSibling) walk(c);
  const content = out.join('').replace(/[ \t\u00a0]+/g, ' ')
    .split('\n').map(l => l.trim())
    .filter(l => l.length > 0)
    .filter((l, i, a) => i === 0 || l !== a[i - 1])
    .join('\n');
  return { title: document.title, content };
})()"#;

#[derive(Clone, Debug)]
pub struct FetchOptions {
    pub max_content_length: usize,
    pub navigation_timeout_ms: u64,
    pub settle_ms: u64,
}

impl Default for FetchOptions {
    fn default() -> Self {
        Self {
            max_content_length: 50_000,
            navigation_timeout_ms: 30_000,
            settle_ms: 0,
        }
    }
}

pub struct FetchResult {
    pub url: String,
    pub title: String,
    pub content: String,
    pub status: u16,
    pub is_pdf: bool,
}

#[derive(Clone)]
struct DocumentResponse {
    url: String,
    status: u16,
    content_type: String,
    body: Vec<u8>,
}

enum WorkerJob {
    Fetch {
        language_script: String,
        url: String,
        opts: FetchOptions,
        reply: oneshot::Sender<Result<FetchResult>>,
    },
    Evaluate {
        language_script: String,
        url: String,
        js: String,
        navigation_timeout_ms: u64,
        settle_ms: u64,
        reply: oneshot::Sender<Result<Value>>,
    },
}

const DEFAULT_WORKER_THREADS: usize = 6;

pub struct BrowserManager {
    tx: mpsc::Sender<WorkerJob>,
    locale: Mutex<String>,
}

impl BrowserManager {
    pub fn new(locale: &str) -> Result<Self> {
        // Validate that Obscura can construct a Browser before spawning workers.
        let initial_browser = Arc::new(
            Browser::builder()
                .stealth(cfg!(feature = "stealth"))
                .build()
                .map_err(|e| anyhow!(e.to_string()))?,
        );

        let worker_count = std::thread::available_parallelism()
            .map(|n| n.get().clamp(2, DEFAULT_WORKER_THREADS))
            .unwrap_or(DEFAULT_WORKER_THREADS);

        let (tx, rx) = mpsc::channel::<WorkerJob>();
        let rx = Arc::new(Mutex::new(rx));

        for idx in 0..worker_count {
            let rx = rx.clone();
            let browser = if idx == 0 {
                initial_browser.clone()
            } else {
                Arc::new(
                    Browser::builder()
                        .stealth(cfg!(feature = "stealth"))
                        .build()
                        .map_err(|e| anyhow!(e.to_string()))?,
                )
            };
            std::thread::Builder::new()
                .name(format!("webmcp-page-{idx}"))
                .spawn(move || worker_loop(browser, rx))
                .map_err(|e| anyhow!("failed to spawn browser worker thread: {e}"))?;
        }

        Ok(Self {
            tx,
            locale: Mutex::new(locale.to_string()),
        })
    }

    pub fn locale(&self) -> String {
        self.locale.lock().unwrap().clone()
    }

    pub fn set_locale(&self, locale: &str) {
        *self.locale.lock().unwrap() = locale.to_string();
    }

    /// Best-effort `navigator.language` override. Obscura's high-level API has
    /// no per-context Accept-Language knob; the search URL builders pass the
    /// language explicitly where the engine supports it.
    fn language_script(&self) -> String {
        // Only allow characters that are valid in a language tag; the value
        // comes from the MCP client and is embedded in a JS string.
        let tag: String = self
            .locale()
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
            .collect();
        format!(
            "try{{Object.defineProperty(navigator,'language',{{get:function(){{return '{tag}';}}}});\
             Object.defineProperty(navigator,'languages',{{get:function(){{return ['{tag}'];}}}});}}catch(e){{}}"
        )
    }

    /// Navigate to `url` and extract title/readable text (or PDF text).
    ///
    /// Dispatches onto a dedicated worker thread running its own `current_thread`
    /// Tokio runtime (required by `deno_unsync` inside Obscura) so concurrent
    /// page parses run across multiple CPU cores without blocking timers.
    pub async fn fetch_page(&self, url: &str, opts: &FetchOptions) -> Result<FetchResult> {
        check_url(url)?;
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(WorkerJob::Fetch {
                language_script: self.language_script(),
                url: url.to_string(),
                opts: opts.clone(),
                reply,
            })
            .map_err(|_| anyhow!("browser worker pool shut down"))?;
        rx.await
            .map_err(|_| anyhow!("browser worker dropped response"))?
    }

    /// Navigate and run a JavaScript extraction expression, returning its value.
    pub async fn evaluate_page(
        &self,
        url: &str,
        js: &str,
        navigation_timeout_ms: u64,
        settle_ms: u64,
    ) -> Result<Value> {
        check_url(url)?;
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(WorkerJob::Evaluate {
                language_script: self.language_script(),
                url: url.to_string(),
                js: js.to_string(),
                navigation_timeout_ms,
                settle_ms,
                reply,
            })
            .map_err(|_| anyhow!("browser worker pool shut down"))?;
        rx.await
            .map_err(|_| anyhow!("browser worker dropped response"))?
    }
}

fn worker_loop(browser: Arc<Browser>, rx: Arc<Mutex<mpsc::Receiver<WorkerJob>>>) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("[webmcp] failed to build worker runtime: {err}");
            return;
        }
    };

    loop {
        let job = {
            let guard = match rx.lock() {
                Ok(guard) => guard,
                Err(_) => return,
            };
            match guard.recv() {
                Ok(job) => job,
                Err(_) => return,
            }
        };

        match job {
            WorkerJob::Fetch {
                language_script,
                url,
                opts,
                reply,
            } => {
                if reply.is_closed() {
                    continue;
                }
                let result = rt.block_on(fetch_page_inner(
                    browser.clone(),
                    language_script,
                    url,
                    opts,
                ));
                let _ = reply.send(result);
            }
            WorkerJob::Evaluate {
                language_script,
                url,
                js,
                navigation_timeout_ms,
                settle_ms,
                reply,
            } => {
                if reply.is_closed() {
                    continue;
                }
                let result = rt.block_on(evaluate_page_inner(
                    browser.clone(),
                    language_script,
                    url,
                    js,
                    navigation_timeout_ms,
                    settle_ms,
                ));
                let _ = reply.send(result);
            }
        }
    }
}

async fn fetch_page_inner(
    browser: Arc<Browser>,
    language_script: String,
    url: String,
    opts: FetchOptions,
) -> Result<FetchResult> {
    let mut page = browser
        .new_page()
        .await
        .map_err(|e| anyhow!(e.to_string()))?;
    page.add_preload_script(&language_script);

    let captured: Arc<Mutex<Vec<DocumentResponse>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = captured.clone();
    page.on_response(Arc::new(move |info, resp| {
        if info.resource_type != ResourceType::Document {
            return;
        }
        let content_type = resp.content_type().unwrap_or("").to_string();
        let looks_like_pdf = content_type.to_ascii_lowercase().contains("application/pdf")
            || info
                .url
                .path()
                .to_ascii_lowercase()
                .ends_with(".pdf");
        let body = if looks_like_pdf {
            resp.body.clone()
        } else {
            Vec::new()
        };
        sink.lock().unwrap().push(DocumentResponse {
            url: info.url.to_string(),
            status: resp.status,
            content_type,
            body,
        });
    }));

    let nav = tokio::time::timeout(
        Duration::from_millis(opts.navigation_timeout_ms),
        page.goto(&url),
    )
    .await;
    let nav_error = match nav {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(anyhow!(e.to_string())),
        Err(_) => Some(anyhow!(
            "navigation timeout after {}ms",
            opts.navigation_timeout_ms
        )),
    };

    if opts.settle_ms > 0 && nav_error.is_none() {
        page.settle(opts.settle_ms).await;
    }

    let final_url = page.url();
    let doc = {
        let list = captured.lock().unwrap();
        list.iter()
            .rev()
            .find(|d| d.url == final_url)
            .or_else(|| list.last())
            .cloned()
    };
    let status = doc.as_ref().map(|d| d.status).unwrap_or(0);
    let is_pdf = doc
        .as_ref()
        .map(|d| d.content_type.to_ascii_lowercase().contains("application/pdf"))
        .unwrap_or(false)
        || (url
            .split('?')
            .next()
            .unwrap_or("")
            .to_ascii_lowercase()
            .ends_with(".pdf")
            && doc
                .as_ref()
                .map(|d| d.body.starts_with(b"%PDF-"))
                .unwrap_or(false));

    // Mirror the original: keep partial content on navigation error only if
    // the document response actually arrived.
    if let Some(err) = nav_error {
        if doc.is_none() {
            return Err(err);
        }
    }

    if is_pdf {
        let body = doc.as_ref().map(|d| d.body.clone()).unwrap_or_default();
        let text = pdf_text(&body)?;
        let title = url.rsplit('/').next().unwrap_or("PDF Document").to_string();
        return Ok(FetchResult {
            url: final_url,
            title,
            content: truncate(text, opts.max_content_length),
            status,
            is_pdf: true,
        });
    }

    if status >= 400 {
        bail!("HTTP {status}");
    }

    let value = page.evaluate(EXTRACT_PAGE_JS);
    let title = value
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let content = value
        .get("content")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    Ok(FetchResult {
        url: final_url,
        title,
        content: truncate(content, opts.max_content_length),
        status,
        is_pdf: false,
    })
}

async fn evaluate_page_inner(
    browser: Arc<Browser>,
    language_script: String,
    url: String,
    js: String,
    navigation_timeout_ms: u64,
    settle_ms: u64,
) -> Result<Value> {
    let mut page = browser
        .new_page()
        .await
        .map_err(|e| anyhow!(e.to_string()))?;
    page.add_preload_script(&language_script);

    match tokio::time::timeout(Duration::from_millis(navigation_timeout_ms), page.goto(&url)).await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => bail!("{}", e),
        Err(_) => bail!("navigation timeout after {navigation_timeout_ms}ms"),
    }

    if settle_ms > 0 {
        page.settle(settle_ms).await;
    }

    Ok(page.evaluate(&js))
}

fn check_url(url: &str) -> Result<()> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        bail!("Only http(s) URLs are supported: {url}");
    }
    Ok(())
}

fn truncate(text: String, max: usize) -> String {
    if text.chars().count() > max {
        let head: String = text.chars().take(max).collect();
        format!("{head}...\n[Content truncated due to length]")
    } else {
        text
    }
}

fn pdf_text(bytes: &[u8]) -> Result<String> {
    if bytes.is_empty() {
        bail!("empty PDF response body");
    }
    if !bytes.starts_with(b"%PDF-") {
        bail!("invalid PDF signature");
    }
    let text = pdf_extract::extract_text_from_mem(bytes).map_err(|e| anyhow!(e.to_string()))?;
    Ok(text
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n"))
}

#[cfg(test)]
mod tests {
    use super::truncate;

    #[test]
    fn truncates_at_char_boundary() {
        let out = truncate("日本語テスト".to_string(), 3);
        assert!(out.starts_with("日本語"));
        assert!(out.ends_with("[Content truncated due to length]"));
        assert_eq!(truncate("short".to_string(), 10), "short");
    }
}
