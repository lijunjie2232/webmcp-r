//! webmcp — search MCP server backed by the Obscura browser engine.

mod browser;
mod mcp;
mod search;
mod tools;

use anyhow::{anyhow, Result};

use mcp::SearchServer;

const VERSION: &str = "1.0.0";

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("Error: {err:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let locale = std::env::var("SEARCH_LOCALE").unwrap_or_else(|_| "ja-JP".to_string());
    let timezone = std::env::var("SEARCH_TIMEZONE").unwrap_or_else(|_| "Asia/Tokyo".to_string());
    // Must be set before the first page initialises V8/ICU.
    std::env::set_var("OBSCURA_TIMEZONE", timezone);

    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str).unwrap_or("help") {
        "serve" => {
            let mut server = SearchServer::new(&locale, default_engines(), 5, 5)?;
            mcp::serve(&mut server).await
        }
        "search" => {
            let keyword = args
                .get(1)
                .ok_or_else(|| anyhow!("Usage: webmcp search <keyword> [engine]"))?;
            let engine = args
                .get(2)
                .filter(|arg| !arg.starts_with("--"))
                .cloned()
                .unwrap_or_else(|| "yahoojapan".to_string());
            let language = flag(&args, "--language").unwrap_or(locale);

            let server = SearchServer::new(&language, default_engines(), 5, 5)?;
            let response = server.manager.search(keyword, &engine).await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
            Ok(())
        }
        "deep-search" | "deep_search" => {
            let keyword = args
                .get(1)
                .ok_or_else(|| anyhow!("Usage: webmcp deep-search <keyword> [--engines a,b]"))?;
            let engines = flag(&args, "--engines")
                .map(|list| {
                    list.split(',')
                        .map(|engine| engine.trim().to_ascii_lowercase())
                        .filter(|engine| !engine.is_empty())
                        .collect::<Vec<_>>()
                })
                .filter(|list| !list.is_empty())
                .unwrap_or_else(default_engines);
            let max_results = flag(&args, "--max-results")
                .and_then(|value| value.parse().ok())
                .unwrap_or(5);
            let parser_num = flag(&args, "--parsers")
                .and_then(|value| value.parse().ok())
                .unwrap_or(5);
            let language = flag(&args, "--language").unwrap_or(locale);

            let server = SearchServer::new(&language, engines.clone(), max_results, parser_num)?;
            let documents = server
                .deep
                .run(
                    keyword,
                    Some(engines),
                    Some(max_results),
                    Some(parser_num),
                )
                .await;
            println!("{}", serde_json::to_string_pretty(&documents)?);
            Ok(())
        }
        "engines" => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "engines": ["google", "bing", "duckduckgo", "yahoojapan"]
                }))?
            );
            Ok(())
        }
        "version" | "-V" | "--version" => {
            println!("{VERSION}");
            Ok(())
        }
        "help" | "-h" | "--help" => {
            usage();
            Ok(())
        }
        other => Err(anyhow!("Unknown command: {other}\n\n{}", usage_text())),
    }
}

fn default_engines() -> Vec<String> {
    ["google", "bing", "duckduckgo", "yahoojapan"]
        .iter()
        .map(|engine| engine.to_string())
        .collect()
}

fn flag(args: &[String], name: &str) -> Option<String> {
    let index = args.iter().position(|arg| arg == name)?;
    args.get(index + 1).cloned()
}

fn usage() {
    println!("{}", usage_text());
}

fn usage_text() -> String {
    format!(
        "webmcp v{VERSION} - Search MCP server (Obscura engine)\n\n\
Usage: webmcp <command> [options]\n\n\
Commands:\n\
  serve                       Start the MCP server (stdio, JSON-RPC)\n\
  search <keyword> [engine]   Search with one engine (default: yahoojapan)\n\
  deep-search <keyword>       Search several engines and fetch result pages\n\
    --engines <a,b>           Engines (default: all)\n\
    --max-results <n>         Results per engine (default: 5)\n\
    --parsers <n>             Pages parsed concurrently (default: 5)\n\
  engines                     List available search engines\n\n\
Environment:\n\
  SEARCH_LOCALE               Default language (default: ja-JP)\n\
  SEARCH_TIMEZONE             Obscura timezone (default: Asia/Tokyo)"
    )
}
