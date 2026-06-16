//! Minimal MCP (JSON-RPC 2.0 over stdio) server and tool definitions.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::browser::BrowserManager;
use crate::search::SearchManager;
use crate::tools::{DeepSearch, PageParser, ParseOptions};

pub struct SearchServer {
    pub browser: Arc<BrowserManager>,
    pub manager: Arc<SearchManager>,
    pub parser: Arc<PageParser>,
    pub deep: DeepSearch,
}

impl SearchServer {
    pub fn new(locale: &str, engines: Vec<String>, max_results: usize, parser_num: usize) -> Result<Self> {
        let browser = Arc::new(BrowserManager::new(locale)?);
        let manager = Arc::new(SearchManager::new(browser.clone()));
        let parser = Arc::new(PageParser::new(browser.clone()));
        let deep = DeepSearch::new(
            manager.clone(),
            parser.clone(),
            engines,
            max_results,
            parser_num,
        );
        Ok(Self {
            browser,
            manager,
            parser,
            deep,
        })
    }

    pub fn set_language(&self, language: &str) -> Value {
        self.browser.set_locale(language);
        log(&format!("Language set to {language}"));
        json!({
            "success": true,
            "language": language,
            "message": format!("Language set to {language}"),
        })
    }

    pub async fn handle_tool_call(&mut self, tool: &str, args: &Value) -> Result<Value> {
        let language = string_arg(args, "language");
        if let Some(language) = &language {
            self.set_language(language);
        }
        let keyword = args
            .get("query")
            .or_else(|| args.get("keyword"))
            .and_then(|v| v.as_str());

        match tool {
            "search" => {
                let keyword = keyword.ok_or_else(|| anyhow!("Missing query parameter"))?;
                let engine = string_arg(args, "engine").unwrap_or_else(|| "yahoojapan".into());
                log(&format!("Calling search with args: {args}"));
                let response = self.manager.search(keyword, &engine).await?;
                Ok(serde_json::to_value(response)?)
            }
            "search_multiple" => {
                let keyword = keyword.ok_or_else(|| anyhow!("Missing query parameter"))?;
                let engines = string_list_arg(args, "engines")
                    .ok_or_else(|| anyhow!("Missing or invalid engines parameter"))?;
                log(&format!("Calling search_multiple with args: {args}"));
                Ok(self.manager.search_multiple(keyword, &engines).await)
            }
            "deep_search" => {
                let keyword = keyword.ok_or_else(|| anyhow!("Missing query parameter"))?;
                let engines = string_list_arg(args, "engines")
                    .ok_or_else(|| anyhow!("Missing or invalid engines parameter"))?;
                let max_results = usize_arg(args, "max_results");
                let parser_num = usize_arg(args, "parser_num");
                log(&format!("Calling deep_search with args: {args}"));
                Ok(Value::Array(
                    self.deep
                        .run(keyword, Some(engines), max_results, parser_num)
                        .await,
                ))
            }
            "get_available_engines" => Ok(json!({
                "engines": ["google", "bing", "duckduckgo", "yahoojapan"],
            })),
            "set_language" => {
                let language =
                    language.ok_or_else(|| anyhow!("Missing or invalid language parameter"))?;
                Ok(self.set_language(&language))
            }
            "parse_url" => {
                let url = args
                    .get("url")
                    .or_else(|| args.get("query"))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow!("Missing required parameter: url"))?;
                let defaults = ParseOptions::default();
                let opts = ParseOptions {
                    max_content_length: usize_arg(args, "maxContentLength")
                        .unwrap_or(defaults.max_content_length),
                    parse_timeout_ms: usize_arg(args, "parseTimeout")
                        .map(|n| n as u64)
                        .unwrap_or(defaults.parse_timeout_ms),
                    max_retries: usize_arg(args, "maxRetries")
                        .map(|n| n as u32)
                        .unwrap_or(defaults.max_retries),
                    navigation_timeout_ms: usize_arg(args, "navigationTimeout")
                        .map(|n| n as u64)
                        .unwrap_or(defaults.navigation_timeout_ms),
                    wait_until: string_arg(args, "waitUntil").unwrap_or(defaults.wait_until),
                };
                self.parser.parse(url, &opts).await
            }
            other => Err(anyhow!("Unknown tool: {other}")),
        }
    }
}

/// Run the MCP stdio loop until stdin closes.
pub async fn serve(server: &mut SearchServer) -> Result<()> {
    log("MCP server started (stdio)");
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    let mut stdout = tokio::io::stdout();

    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(response) = handle_line(server, line).await {
            stdout.write_all(response.to_string().as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }

    server.cleanup();
    Ok(())
}

pub async fn handle_line(server: &mut SearchServer, line: &str) -> Option<Value> {
    let request: Value = match serde_json::from_str(line) {
        Ok(value) => value,
        Err(_) => {
            return Some(json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": { "code": -32700, "message": "Parse error" },
            }))
        }
    };

    let id = request.get("id").cloned();
    let method = request.get("method").and_then(|v| v.as_str()).unwrap_or("");

    if id.is_none() && method != "initialize" {
        // JSON-RPC notification: never respond.
        return None;
    }
    let id = id.unwrap_or(Value::Null);

    match method {
        "initialize" => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "webmcp", "version": "1.0.0" },
            },
        })),
        "ping" => Some(json!({ "jsonrpc": "2.0", "id": id, "result": {} })),
        "tools/list" => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": { "tools": tools() },
        })),
        "tools/call" => {
            let params = request.get("params").cloned().unwrap_or(Value::Null);
            let name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let args = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            log(&format!("Tool call: {name}"));

            match server.handle_tool_call(name, &args).await {
                Ok(result) => Some(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "content": [{
                            "type": "text",
                            "text": serde_json::to_string_pretty(&result).unwrap_or_default(),
                        }],
                    },
                })),
                Err(err) => Some(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32000, "message": err.to_string() },
                })),
            }
        }
        other => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": -32601, "message": format!("Method not found: {other}") },
        })),
    }
}

impl SearchServer {
    pub fn cleanup(&self) {
        log("MCP server shutting down");
    }
}

fn string_arg(args: &Value, key: &str) -> Option<String> {
    args.get(key)
        .and_then(|v| v.as_str())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn string_list_arg(args: &Value, key: &str) -> Option<Vec<String>> {
    Some(
        args.get(key)?
            .as_array()?
            .iter()
            .filter_map(|v| v.as_str())
            .map(|v| v.trim().to_ascii_lowercase())
            .filter(|v| !v.is_empty())
            .collect(),
    )
}

fn usize_arg(args: &Value, key: &str) -> Option<usize> {
    args.get(key).and_then(|v| v.as_u64()).map(|v| v as usize)
}

fn log(message: &str) {
    eprintln!("[webmcp] {message}");
}

fn tools() -> Value {
    json!([
        {
            "name": "search",
            "description": "ウェブ検索を実行し、検索結果を取得します。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "検索キーワード" },
                    "engine": {
                        "type": "string",
                        "enum": ["google", "bing", "duckduckgo", "yahoojapan"],
                        "description": "検索エンジン（デフォルト: yahoojapan）",
                        "default": "yahoojapan"
                    },
                    "language": {
                        "type": "string",
                        "description": "検索言語（例: ja-JP, en-US）",
                        "default": "ja-JP"
                    }
                },
                "required": ["query"]
            }
        },
        {
            "name": "search_multiple",
            "description": "複数の検索エンジンで同時にウェブ検索を実行し、結果を統合します。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "検索キーワード" },
                    "engines": {
                        "type": "array",
                        "items": { "type": "string", "enum": ["google", "bing", "duckduckgo", "yahoojapan"] },
                        "description": "検索エンジンのリスト"
                    },
                    "language": {
                        "type": "string",
                        "description": "検索言語（例: ja-JP, en-US）",
                        "default": "ja-JP"
                    }
                },
                "required": ["query", "engines"]
            }
        },
        {
            "name": "deep_search",
            "description": "複数の検索エンジンで同時にウェブ検索を実行し、結果を統合します。さらに、各検索結果のページ内容も取得します。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "検索キーワード" },
                    "engines": {
                        "type": "array",
                        "items": { "type": "string", "enum": ["google", "bing", "duckduckgo", "yahoojapan"] },
                        "description": "検索エンジンのリスト"
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "各エンジンから取得する最大検索結果数",
                        "default": 5
                    },
                    "language": {
                        "type": "string",
                        "description": "検索言語（例: ja-JP, en-US）"
                    },
                    "parser_num": {
                        "type": "integer",
                        "description": "同時に解析するページ数",
                        "default": 5
                    }
                },
                "required": ["query", "engines", "language"]
            }
        },
        {
            "name": "get_available_engines",
            "description": "利用可能な検索エンジンのリストを取得します。",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "set_language",
            "description": "検索言語を設定します。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "language": {
                        "type": "string",
                        "description": "検索言語（例: ja-JP, en-US, zh-CN）",
                        "default": "ja-JP"
                    }
                },
                "required": ["language"]
            }
        },
        {
            "name": "parse_url",
            "description": "指定されたURLのページを解析し、タイトルと内容を抽出します。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "url": { "type": "string", "description": "解析するURL" },
                    "maxContentLength": {
                        "type": "integer",
                        "description": "抽出する最大文字数",
                        "default": 50000
                    },
                    "parseTimeout": {
                        "type": "integer",
                        "description": "パース処理のタイムアウト（ミリ秒）",
                        "default": 60000
                    },
                    "maxRetries": {
                        "type": "integer",
                        "description": "パース失敗時の最大リトライ回数",
                        "default": 1
                    },
                    "navigationTimeout": {
                        "type": "integer",
                        "description": "ページ遷移のタイムアウト（ミリ秒）",
                        "default": 30000
                    },
                    "waitUntil": {
                        "type": "string",
                        "enum": ["load", "domcontentloaded", "networkidle0", "networkidle2", "commit"],
                        "description": "ページ読み込み完了条件",
                        "default": "load"
                    },
                    "language": {
                        "type": "string",
                        "description": "言語設定（例: ja-JP, en-US）",
                        "default": "ja-JP"
                    },
                    "headless": {
                        "type": "boolean",
                        "description": "ヘッドレスモードで実行するか",
                        "default": true
                    }
                },
                "required": ["url"]
            }
        }
    ])
}
