use clap::{Parser, Subcommand};
use colored::*;
use mysql::prelude::*;
use rustyline::error::ReadlineError;
use rustyline::completion::Completer;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::Helper;
use rusqlite::types::Value as SqliteValue;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rumqttc::{
    AsyncClient, Event, MqttOptions, QoS,
};
use tokio::sync::oneshot;
use uuid::Uuid;

const VERSION: &str = env!("CARGO_PKG_VERSION");

// ─── CLI ───────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "dbcli",
    version = VERSION,
    about = "A CLI tool for reading/writing SQLite and MySQL databases"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Sqlite {
        #[arg(short, long)]
        database: PathBuf,
        #[arg(short, long)]
        query: Option<String>,
        #[arg(short = 'p', long, action = clap::ArgAction::Append)]
        params: Vec<String>,
    },
    Mysql {
        #[arg(short, long)]
        url: String,
        #[arg(short, long)]
        query: Option<String>,
        #[arg(short = 'p', long, action = clap::ArgAction::Append)]
        params: Vec<String>,
    },
    Agent {
        #[arg(short, long)]
        broker: String,
        #[arg(short, long)]
        id: String,
        #[arg(short, long)]
        user: Option<String>,
        #[arg(short = 'w', long)]
        password: Option<String>,
        #[arg(long)]
        tls: bool,
        #[arg(short = 'd', long)]
        database: Option<String>,
        #[arg(short = 't', long, default_value = "sqlite")]
        db_type: String,
    },
}

// ─── DB types ──────────────────────────────────────────────────────

enum DbType {
    Sqlite(rusqlite::Connection),
    Mysql(mysql::Pool),
}

impl DbType {
    fn label(&self) -> String {
        match self {
            DbType::Sqlite(conn) => conn
                .path()
                .map(|p| {
                    PathBuf::from(p)
                        .file_name()
                        .map(|f| f.to_string_lossy().to_string())
                        .unwrap_or_else(|| "sqlite".to_string())
                })
                .unwrap_or_else(|| "sqlite".to_string()),
            DbType::Mysql(_) => "mysql".to_string(),
        }
    }

    fn kind(&self) -> &str {
        match self {
            DbType::Sqlite(_) => "SQLite",
            DbType::Mysql(_) => "MySQL",
        }
    }
}

// ─── MQTT Types ─────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
struct MqttCmd {
    request_id: String,
    action: String,
    db_type: String,
    sql: String,
    params: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MqttResp {
    request_id: String,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    affected: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChatMsg {
    from: String,
    to: String,
    msg: String,
    timestamp: u64,
}

struct MqttState {
    client: Option<AsyncClient>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<MqttResp>>>>,
    broker_url: String,
    client_id: String,
    mqtt_user: String,
    mqtt_pass: String,
    mqtt_tls: bool,
    _runtime: Option<tokio::runtime::Runtime>,
    chat_enabled: bool,
    chat_queue: Arc<Mutex<Vec<ChatMsg>>>,
}

struct AgentState {
    running: bool,
    agent_id: String,
    stop_tx: Option<std::sync::mpsc::Sender<()>>,
}

// ─── AI State ─────────────────────────────────────────────────────

#[derive(Clone)]
struct AiState {
    api_key: Option<String>,
    base_url: String,
    model: String,
    history: Vec<AiMessage>,
    chat_active: bool,
    chat_target: String,
    tavily_key: Option<String>,
    search_enabled: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct AiMessage {
    role: String,
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<ToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct ToolCall {
    id: String,
    #[serde(rename = "type")]
    call_type: String,
    function: ToolFunction,
}

#[derive(Clone, Serialize, Deserialize)]
struct ToolFunction {
    name: String,
    arguments: String,
}

#[derive(Deserialize)]
struct AiResponse {
    choices: Vec<AiChoice>,
}

#[derive(Deserialize)]
struct AiChoice {
    message: AiMessage,
    #[allow(dead_code)]
    finish_reason: Option<String>,
}

// ─── SQL Highlighting ──────────────────────────────────────────────

const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
    "DELETE", "CREATE", "TABLE", "DROP", "ALTER", "INDEX", "VIEW",
    "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "FULL",
    "ON", "AND", "OR", "NOT", "IN", "AS", "IS", "NULL",
    "ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET",
    "DISTINCT", "LIKE", "BETWEEN", "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END",
    "BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION",
    "IF", "CONFLICT", "RENAME", "COLUMN", "ADD", "PRAGMA",
    "EXPLAIN", "ANALYZE", "VACUUM", "ATTACH", "DETACH",
    "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "CONSTRAINT",
    "UNIQUE", "CHECK", "DEFAULT", "AUTOINCREMENT", "INCREMENT",
    "INTEGER", "TEXT", "REAL", "BLOB", "NUMERIC", "BOOLEAN", "DATE", "TIMESTAMP",
    "INT", "BIGINT", "SMALLINT", "VARCHAR", "CHAR", "FLOAT", "DOUBLE", "DECIMAL",
    "SHOW", "TABLES", "DATABASES", "COLUMNS", "DESCRIBE", "STATUS",
    "GRANT", "REVOKE", "TRUNCATE", "UPSERT", "RETURNING",
    "ASC", "DESC", "NULLS", "FIRST", "LAST",
    "NATURAL", "USING", "INTERSECT", "EXCEPT", "UNION", "ALL",
    "RECURSIVE", "WINDOW", "OVER", "PARTITION",
];

const SQL_FUNCTIONS: &[&str] = &[
    "COUNT", "SUM", "AVG", "MIN", "MAX", "ABS", "LENGTH", "UPPER", "LOWER",
    "TRIM", "LTRIM", "RTRIM", "SUBSTR", "SUBSTRING", "REPLACE", "ROUND",
    "RANDOM", "TYPEOF", "COALESCE", "NULLIF", "IFNULL", "IIF", "CAST",
    "DATE", "TIME", "DATETIME", "JULIANDAY", "STRFTIME",
    "NOW", "CURDATE", "CURRENT_DATE", "CURRENT_TIMESTAMP",
    "CONCAT", "CONCAT_WS", "HEX", "UNHEX", "QUOTE", "ZEROBLOB",
    "LAST_INSERT_ROWID", "CHANGES", "TOTAL_CHANGES",
    "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "LAG", "LEAD",
    "TRUE", "FALSE",
];

fn highlight_sql(line: &str) -> String {
    let mut result = String::with_capacity(line.len() * 2);
    let chars: Vec<char> = line.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let c = chars[i];

        // Single-quoted string
        if c == '\'' {
            result.push_str(&"'".green().to_string());
            i += 1;
            while i < len {
                let ch = chars[i];
                if ch == '\'' {
                    if i + 1 < len && chars[i + 1] == '\'' {
                        // escaped quote
                        result.push_str(&"''".green().to_string());
                        i += 2;
                    } else {
                        result.push_str(&"'".green().to_string());
                        i += 1;
                        break;
                    }
                } else {
                    result.push_str(&ch.to_string().green());
                    i += 1;
                }
            }
            continue;
        }

        // Double-quoted identifier
        if c == '"' {
            result.push_str(&"\"".blue().to_string());
            i += 1;
            while i < len {
                let ch = chars[i];
                if ch == '"' {
                    if i + 1 < len && chars[i + 1] == '"' {
                        result.push_str(&"\"\"".blue().to_string());
                        i += 2;
                    } else {
                        result.push_str(&"\"".blue().to_string());
                        i += 1;
                        break;
                    }
                } else {
                    result.push_str(&ch.to_string().blue());
                    i += 1;
                }
            }
            continue;
        }

        // Line comment
        if c == '-' && i + 1 < len && chars[i + 1] == '-' {
            let comment: String = chars[i..].iter().collect();
            result.push_str(&comment.dimmed().to_string());
            break;
        }

        // Block comment
        if c == '/' && i + 1 < len && chars[i + 1] == '*' {
            let remaining: String = chars[i..].iter().collect();
            if let Some(end) = remaining.find("*/") {
                let comment: String = chars[i..i + end + 2].iter().collect();
                result.push_str(&comment.dimmed().to_string());
                i += end + 2;
            } else {
                result.push_str(&remaining.dimmed().to_string());
                break;
            }
            continue;
        }

        // Number
        if c.is_ascii_digit() {
            let start = i;
            while i < len && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            let num: String = chars[start..i].iter().collect();
            result.push_str(&num.cyan().to_string());
            continue;
        }

        // Word (keyword / function / identifier)
        if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let upper = word.to_uppercase();

            if SQL_KEYWORDS.contains(&upper.as_str()) {
                result.push_str(&word.to_uppercase().blue().bold().to_string());
            } else if SQL_FUNCTIONS.contains(&upper.as_str()) {
                result.push_str(&word.magenta().to_string());
            } else {
                // parameter placeholder
                if word.starts_with('?') || word.starts_with('@') || word.starts_with('$') {
                    result.push_str(&word.red().bold().to_string());
                } else {
                    result.push_str(&word);
                }
            }
            continue;
        }

        // Operators
        if matches!(c, '=' | '<' | '>' | '!' | '+' | '-' | '*' | '/' | '%' | '|') {
            result.push_str(&c.to_string().red().to_string());
            i += 1;
            continue;
        }

        // Default
        result.push(c);
        i += 1;
    }

    result
}

// ─── rustyline Helper ──────────────────────────────────────────────

struct DbHelper {}

impl Completer for DbHelper {
    type Candidate = String;
}
impl Hinter for DbHelper {
    type Hint = String;
    fn hint(&self, _line: &str, _pos: usize, _ctx: &rustyline::Context<'_>) -> Option<String> {
        None
    }
}
impl Validator for DbHelper {}
impl Helper for DbHelper {}

impl Highlighter for DbHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        Cow::Owned(highlight_sql(line))
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> Cow<'b, str> {
        Cow::Owned(colorize_prompt(prompt))
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _forced: bool) -> bool {
        true
    }
}

fn colorize_prompt(prompt: &str) -> String {
    if prompt == "?" {
        return "? ".to_string();
    }
    prompt.to_string()
}

fn extract_sql_from_ai_response(text: &str) -> Option<String> {
    // Extract SQL from ```sql ... ``` code blocks
    if let Some(start) = text.find("```sql") {
        let after_tag = start + 6;
        if let Some(end) = text[after_tag..].find("```") {
            let sql = text[after_tag..after_tag + end].trim();
            if !sql.is_empty() {
                return Some(sql.to_string());
            }
        }
    }
    // Also try ``` without "sql" tag
    if let Some(start) = text.find("```") {
        let after_tag = start + 3;
        // skip language tag on same line
        let line_end = text[after_tag..].find('\n').unwrap_or(0);
        let content_start = after_tag + line_end;
        if let Some(end) = text[content_start..].find("```") {
            let sql = text[content_start..content_start + end].trim();
            if !sql.is_empty() && (sql.to_uppercase().contains("SELECT")
                || sql.to_uppercase().contains("INSERT")
                || sql.to_uppercase().contains("UPDATE")
                || sql.to_uppercase().contains("DELETE")
                || sql.to_uppercase().contains("CREATE")
                || sql.to_uppercase().contains("DROP")
                || sql.to_uppercase().contains("ALTER")
                || sql.to_uppercase().contains("PRAGMA")
                || sql.to_uppercase().contains("SHOW")) {
                return Some(sql.to_string());
            }
        }
    }
    None
}

// ─── Connection ────────────────────────────────────────────────────

fn connect_sqlite(path: &str) -> Result<DbType, String> {
    let conn = rusqlite::Connection::open(path)
        .map_err(|e| format!("Failed to open SQLite database '{}': {}", path, e))?;
    Ok(DbType::Sqlite(conn))
}

fn connect_mysql(url: &str) -> Result<DbType, String> {
    let pool = mysql::Pool::new(url)
        .map_err(|e| format!("Failed to connect to MySQL '{}': {}", url, e))?;
    Ok(DbType::Mysql(pool))
}

// ─── MQTT Client ────────────────────────────────────────────────────

fn parse_broker_host_port(default_host: &str, default_port: u16) -> (String, u16) {
    if default_host.starts_with('[') {
        if let Some(close) = default_host.find(']') {
            let h = &default_host[1..close];
            let rest = default_host[close+1..].trim_start_matches(':');
            let p: u16 = rest.parse().unwrap_or(default_port);
            (h.to_string(), p)
        } else {
            (default_host.trim_start_matches('[').trim_end_matches(']').to_string(), default_port)
        }
    } else {
        let mut parts = default_host.rsplitn(2, ':');
        let port_str = parts.next().unwrap_or("");
        let host_str = parts.next().unwrap_or(default_host);
        let p: u16 = port_str.parse().unwrap_or(default_port);
        (host_str.to_string(), p)
    }
}

fn mqtt_connect_opts(broker_url: &str, client_id: &str, user: &str, pass: &str, tls: bool) -> Result<MqttOptions, String> {
    let url = broker_url.trim_end_matches('/');
    let default_host = url
        .replace("mqtt://", "")
        .replace("mqtts://", "");
    let default_port = if tls { 8883 } else { 1883 };
    let (host, port) = parse_broker_host_port(&default_host, default_port);

    let mut opts = MqttOptions::new(client_id, &host, port);
    opts.set_credentials(user, pass);
    opts.set_keep_alive(std::time::Duration::from_secs(30));

    if tls {
        opts.set_transport(rumqttc::Transport::Tls(
            rumqttc::TlsConfiguration::default(),
        ));
    }

    Ok(opts)
}

fn mqtt_connect(
    mqtt_state: &mut MqttState,
    broker_url: &str,
    user: &str,
    pass: &str,
    tls: bool,
) -> Result<(), String> {
    let client_id = format!("dbcli-{}", &Uuid::new_v4().to_string()[..8]);
    let opts = mqtt_connect_opts(broker_url, &client_id, user, pass, tls)?;
    let (client, mut event_loop) = AsyncClient::new(opts, 100);

    let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    rt.block_on(async {
        // Wait for ConnAck BEFORE subscribing (MQTT protocol requirement)
        match tokio::time::timeout(std::time::Duration::from_secs(5), event_loop.poll()).await {
            Ok(Ok(Event::Incoming(rumqttc::Packet::ConnAck(_)))) => {}
            Ok(Ok(_)) => return Err("Unexpected response from broker".to_string()),
            Ok(Err(e)) => return Err(format!("MQTT error: {}", e)),
            Err(_) => return Err("Connection timeout".to_string()),
        }
        client
            .subscribe("dbcli/resp/#", QoS::AtLeastOnce)
            .await
            .map_err(|e| format!("Subscribe error: {}", e))?;
        client
            .subscribe(format!("dbcli/chat/{}", client_id), QoS::AtLeastOnce)
            .await
            .map_err(|e| format!("Chat subscribe error: {}", e))?;
        client
            .subscribe("dbcli/chat/broadcast", QoS::AtLeastOnce)
            .await
            .map_err(|e| format!("Chat broadcast subscribe error: {}", e))?;
        Ok(())
    })?;

    // Spawn background event loop on the persistent runtime
    let pending = mqtt_state.pending.clone();
    let chat_queue = mqtt_state.chat_queue.clone();
    rt.spawn(async move {
        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(1), event_loop.poll()).await {
                Ok(Ok(Event::Incoming(rumqttc::Packet::Publish(p)))) => {
                    let topic = p.topic.clone();
                    if topic.starts_with("dbcli/chat/") {
                        if let Ok(msg) = serde_json::from_slice::<ChatMsg>(&p.payload) {
                            chat_queue.lock().unwrap_or_else(|e| e.into_inner()).push(msg);
                        }
                    } else {
                        if let Ok(resp) = serde_json::from_slice::<MqttResp>(&p.payload) {
                            if let Some(tx) = pending.lock().unwrap_or_else(|e| e.into_inner()).remove(&resp.request_id) {
                                let _ = tx.send(resp);
                            }
                        }
                    }
                }
                Ok(Ok(_)) => {}
                Ok(Err(_)) => {}
                Err(_) => {}
            }
        }
    });

    mqtt_state.client = Some(client);
    mqtt_state.broker_url = broker_url.to_string();
    mqtt_state.client_id = client_id;
    mqtt_state.mqtt_user = user.to_string();
    mqtt_state.mqtt_pass = pass.to_string();
    mqtt_state.mqtt_tls = tls;
    mqtt_state._runtime = Some(rt);

    Ok(())
}

fn mqtt_exec_remote(
    mqtt_state: &mut MqttState,
    agent_id: &str,
    db_type: &str,
    sql: &str,
    params: &[String],
) -> Result<MqttResp, String> {
    let client = mqtt_state
        .client
        .as_ref()
        .ok_or("Not connected. Use .mqtt connect first.")?;

    let request_id = Uuid::new_v4().to_string();
    let cmd = MqttCmd {
        request_id: request_id.clone(),
        action: "exec".to_string(),
        db_type: db_type.to_string(),
        sql: sql.to_string(),
        params: params.to_vec(),
    };

    let payload = serde_json::to_vec(&cmd).map_err(|e| e.to_string())?;
    let topic = format!("dbcli/cmd/{}", agent_id);

    let (tx, rx) = oneshot::channel();
    mqtt_state
        .pending
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(request_id.clone(), tx);

    let rt = mqtt_state._runtime.as_ref().ok_or("No runtime available")?;
    rt.block_on(async {
        client
            .publish(topic, QoS::AtLeastOnce, false, payload)
            .await
            .map_err(|e| format!("Publish error: {}", e))?;
        Ok::<(), String>(())
    })?;

    // Wait for response with timeout
    rt.block_on(async {
        match tokio::time::timeout(std::time::Duration::from_secs(30), rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err("Response channel closed".to_string()),
            Err(_) => {
                mqtt_state.pending.lock().unwrap_or_else(|e| e.into_inner()).remove(&request_id);
                Err("Timeout waiting for agent response (30s)".to_string())
            }
        }
    })
}

fn mqtt_disconnect(mqtt_state: &mut MqttState) {
    if let Some(client) = mqtt_state.client.take() {
        if let Some(rt) = mqtt_state._runtime.as_ref() {
            rt.block_on(async {
                let _ = client.disconnect().await;
            });
        }
    }
    mqtt_state.pending.lock().unwrap_or_else(|e| e.into_inner()).clear();
    mqtt_state._runtime = None;
}

// ─── AI Functions ──────────────────────────────────────────────────

fn escape_sqlite_table(table: &str) -> String {
    table.replace('"', "\"\"")
}

fn escape_mysql_table(table: &str) -> String {
    table.replace('`', "``")
}

fn ai_get_schema(db_type: &Option<DbType>) -> String {
    match db_type {
        Some(DbType::Sqlite(conn)) => {
            let mut schema = String::new();
            if let Ok(mut stmt) = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name") {
                let tables: Vec<String> = match stmt.query_map([], |row| row.get(0)) {
                    Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
                    Err(_) => Vec::new(),
                };
                for table in &tables {
                    schema.push_str(&format!("\nTable: {}\n", table));
                    let safe_name = escape_sqlite_table(table);
                    if let Ok(mut cols) = conn.prepare(&format!("PRAGMA table_info(\"{}\")", safe_name)) {
                        let columns: Vec<String> = match cols.query_map([], |row| {
                            let name: String = row.get(1)?;
                            let ctype: String = row.get(2)?;
                            let notnull: bool = row.get(3)?;
                            let default: Option<String> = row.get(4)?;
                            Ok(format!("  {} {} {}{}", name, ctype, if notnull { "NOT NULL" } else { "" }, default.map(|d| format!(" DEFAULT {}", d)).unwrap_or_default()))
                        }) {
                            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
                            Err(_) => Vec::new(),
                        };
                        schema.push_str(&columns.join("\n"));
                        schema.push('\n');
                    }
                }
            }
            if schema.is_empty() { "No tables found.".to_string() } else { schema }
        }
        Some(DbType::Mysql(pool)) => {
            let mut schema = String::new();
            if let Ok(mut conn) = pool.get_conn() {
                if let Ok(tables) = conn.exec::<String, _, _>("SHOW TABLES", ()) {
                    for table in &tables {
                        schema.push_str(&format!("\nTable: {}\n", table));
                        let safe_name = escape_mysql_table(table);
                        if let Ok(mut result) = conn.exec_iter(format!("SHOW CREATE TABLE `{}`", safe_name).as_str(), ()) {
                            for row in (&mut result).flatten() {
                                let create: mysql::Value = row.get(1).unwrap_or(mysql::Value::NULL);
                                if let mysql::Value::Bytes(b) = create {
                                    schema.push_str(&String::from_utf8_lossy(&b));
                                    schema.push('\n');
                                }
                            }
                        }
                    }
                }
            }
            if schema.is_empty() { "No tables found.".to_string() } else { schema }
        }
        None => "No database connected.".to_string(),
    }
}

fn get_web_search_tool() -> Value {
    json!({
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Search the web for real-time information. Use this when you need current data, facts, or news that you don't know.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query"
                    }
                },
                "required": ["query"]
            }
        }
    })
}

async fn search_duckduckgo(query: &str) -> Result<String, String> {
    let url = format!("https://html.duckduckgo.com/html/?q={}", urlencoding::encode(query));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client.get(&url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .send().await.map_err(|e| e.to_string())?;
    let html = resp.text().await.map_err(|e| e.to_string())?;
    
    let mut results = Vec::new();
    let mut start = 0;
    while let Some(pos) = html[start..].find("class=\"result__a\"") {
        let abs_pos = start + pos;
        if let Some(title_start) = html[abs_pos..].find('>') {
            let title_begin = abs_pos + title_start + 1;
            if let Some(title_end) = html[title_begin..].find("</a>") {
                let title = html[title_begin..title_begin + title_end]
                    .replace("<br>", " ").replace("<b>", "").replace("</b>", "").trim().to_string();
                if !title.is_empty() && results.len() < 5 {
                    results.push(title);
                }
            }
        }
        start = abs_pos + 20;
        if results.len() >= 5 { break; }
    }
    
    if results.is_empty() {
        Ok(format!("No results found for: {}", query))
    } else {
        Ok(results.into_iter().enumerate().map(|(i, r)| format!("{}. {}", i + 1, r)).collect::<Vec<_>>().join("\n"))
    }
}

async fn search_tavily(query: &str, api_key: &str) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| e.to_string())?;
    let body = json!({
        "api_key": api_key,
        "query": query,
        "max_results": 5,
        "include_answer": true,
    });
    let resp = client.post("https://api.tavily.com/search")
        .header("Content-Type", "application/json")
        .json(&body)
        .send().await.map_err(|e| e.to_string())?;
    
    let status = resp.status();
    let text = resp.text().await.map_err(|e| e.to_string())?;
    if !status.is_success() {
        return Err(format!("Tavily API error {}: {}", status, text));
    }
    
    let data: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    let mut output = String::new();
    
    if let Some(answer) = data.get("answer").and_then(|a| a.as_str()) {
        output.push_str(&format!("Answer: {}\n\n", answer));
    }
    
    if let Some(results) = data.get("results").and_then(|r| r.as_array()) {
        for (i, r) in results.iter().enumerate().take(5) {
            let title = r.get("title").and_then(|t| t.as_str()).unwrap_or("");
            let content = r.get("content").and_then(|c| c.as_str()).unwrap_or("");
            let url = r.get("url").and_then(|u| u.as_str()).unwrap_or("");
            output.push_str(&format!("{}. {} - {}\n   {}\n", i + 1, title, url, content));
        }
    }
    
    if output.is_empty() {
        Ok(format!("No results found for: {}", query))
    } else {
        Ok(output)
    }
}

async fn execute_web_search(query: &str, ai_state: &AiState) -> String {
    if let Some(ref key) = ai_state.tavily_key {
        match search_tavily(query, key).await {
            Ok(results) => return results,
            Err(e) => eprintln!("  {} Tavily failed: {}, trying DuckDuckGo...", "!".yellow(), e),
        }
    }
    match search_duckduckgo(query).await {
        Ok(results) => results,
        Err(e) => format!("Search failed: {}", e),
    }
}

async fn ai_call_api_with_tools(ai_state: &AiState, messages: &[AiMessage]) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .user_agent("dbcli/0.1")
        .build()
        .map_err(|e| format!("Failed to create HTTP client: {}", e))?;
    let base = ai_state.base_url.trim_end_matches('/');
    let url = format!("{}/chat/completions", base);
    
    let tools = if ai_state.search_enabled {
        Some(json!([get_web_search_tool()]))
    } else {
        None
    };
    
    let mut body = json!({
        "model": ai_state.model,
        "messages": messages,
        "temperature": 0.7,
        "max_tokens": 4096,
    });
    if let Some(t) = tools {
        body["tools"] = t;
    }
    
    let api_key = ai_state.api_key.as_ref().ok_or("API key not configured. Use `.ai config` to set it.")?;
    let resp = client.post(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("API request failed: {}", e))?;
    
    let status = resp.status();
    let text = resp.text().await.map_err(|e| format!("Failed to read response: {}", e))?;
    
    if !status.is_success() {
        return Err(format!("API error {}: {}", status, text));
    }
    
    let ai_resp: AiResponse = serde_json::from_str(&text)
        .map_err(|e| format!("Failed to parse response: {}", e))?;
    
    let choice = ai_resp.choices.first()
        .ok_or_else(|| "No response from AI".to_string())?;
    
    let msg = &choice.message;
    
    if let Some(ref tool_calls) = msg.tool_calls {
        if !tool_calls.is_empty() {
            return Err(format!("TOOL_CALL:{}", serde_json::to_string(tool_calls).map_err(|e| format!("Tool call serialization failed: {}", e))?));
        }
    }
    
    msg.content.clone().ok_or_else(|| "Empty response from AI".to_string())
}

fn ai_sync_call(ai_state: &AiState, messages: &[AiMessage]) -> Result<String, String> {
    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("Failed to create runtime: {}", e))?;
    rt.block_on(async {
        let mut all_messages = messages.to_vec();
        let max_rounds = 5;
        
        for _ in 0..max_rounds {
            match ai_call_api_with_tools(ai_state, &all_messages).await {
                Ok(reply) => return Ok(reply),
                Err(e) => {
                    if e.starts_with("TOOL_CALL:") {
                        let tool_calls: Vec<ToolCall> = serde_json::from_str(e.strip_prefix("TOOL_CALL:").unwrap())
                            .map_err(|e| format!("Failed to parse tool calls: {}", e))?;
                        
                        let assistant_msg = AiMessage {
                            role: "assistant".to_string(),
                            content: None,
                            tool_calls: Some(tool_calls.clone()),
                            tool_call_id: None,
                        };
                        all_messages.push(assistant_msg);
                        
                        for tc in &tool_calls {
                            if tc.function.name == "web_search" {
                                let args: Value = serde_json::from_str(&tc.function.arguments).unwrap_or_default();
                                let query = args.get("query").and_then(|q| q.as_str()).unwrap_or("");
                                eprintln!("  🔍 Searching: {}", query.cyan());
                                let results = execute_web_search(query, ai_state).await;
                                all_messages.push(AiMessage {
                                    role: "tool".to_string(),
                                    content: Some(results),
                                    tool_calls: None,
                                    tool_call_id: Some(tc.id.clone()),
                                });
                            }
                        }
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        Err("Too many tool calling rounds".to_string())
    })
}

// ─── SQLite ────────────────────────────────────────────────────────

fn sqlite_value_to_json(val: SqliteValue) -> Value {
    match val {
        SqliteValue::Null => Value::Null,
        SqliteValue::Integer(i) => json!(i),
        SqliteValue::Real(f) => json!(f),
        SqliteValue::Text(s) => json!(s),
        SqliteValue::Blob(b) => json!(format!("0x{}", hex::encode(&b))),
    }
}

fn parse_sqlite_param(s: &str) -> Box<dyn rusqlite::types::ToSql> {
    if s.eq_ignore_ascii_case("null") {
        Box::new(None::<String>)
    } else if let Ok(i) = s.parse::<i64>() {
        Box::new(i)
    } else if let Ok(f) = s.parse::<f64>() {
        Box::new(f)
    } else {
        Box::new(s.to_string())
    }
}

fn exec_sqlite(
    conn: &rusqlite::Connection,
    query: &str,
    params: &[String],
) -> Result<(String, bool), String> {
    let kw = query.trim_start().to_uppercase();
    let is_query = kw.starts_with("SELECT")
        || kw.starts_with("PRAGMA")
        || kw.starts_with("EXPLAIN")
        || kw.starts_with("WITH");

    if is_query {
        let mut stmt = conn
            .prepare(query)
            .map_err(|e| format!("Failed to prepare query: {}", e))?;
        let param_values: Vec<Box<dyn rusqlite::types::ToSql>> =
            params.iter().map(|p| parse_sqlite_param(p)).collect();
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|p| p.as_ref()).collect();
        let columns: Vec<String> = stmt
            .column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let mut rows = stmt
            .query(param_refs.as_slice())
            .map_err(|e| format!("Failed to execute query: {}", e))?;
        let mut results: Vec<Value> = Vec::new();
        while let Some(row) = rows
            .next()
            .map_err(|e| format!("Failed to fetch row: {}", e))?
        {
            let mut obj = serde_json::Map::new();
            for (i, col) in columns.iter().enumerate() {
                let val: SqliteValue = row.get(i).unwrap_or(SqliteValue::Null);
                obj.insert(col.clone(), sqlite_value_to_json(val));
            }
            results.push(Value::Object(obj));
        }
        Ok((serde_json::to_string_pretty(&results).map_err(|e| format!("JSON error: {}", e))?, true))
    } else {
        let param_values: Vec<Box<dyn rusqlite::types::ToSql>> =
            params.iter().map(|p| parse_sqlite_param(p)).collect();
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|p| p.as_ref()).collect();
        let affected = conn
            .execute(query, param_refs.as_slice())
            .map_err(|e| format!("Failed to execute statement: {}", e))?;
        Ok((format!("{}", affected), false))
    }
}

fn sqlite_tables(conn: &rusqlite::Connection) -> Result<String, String> {
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .map_err(|e| format!("{}", e))?;
    let tables: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .map_err(|e| format!("{}", e))?
        .filter_map(|r| r.ok())
        .collect();
    let mut out = String::new();
    for (i, t) in tables.iter().enumerate() {
        out.push_str(&format!(
            "  {:>3}  {}\n",
            format!("{}.", i + 1).dimmed(),
            t
        ));
    }
    if tables.is_empty() {
        out.push_str(&format!("  {}\n", "(no tables)".dimmed()));
    }
    out.push_str(&format!("  {}\n", format!("{} table(s)", tables.len()).dimmed()));
    Ok(out)
}

fn sqlite_schema(conn: &rusqlite::Connection, table: &str) -> Result<String, String> {
    let mut stmt = conn
        .prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name=?1")
        .map_err(|e| format!("{}", e))?;
    let mut rows = stmt
        .query_map([table], |row| row.get::<_, String>(0))
        .map_err(|e| format!("{}", e))?;
    match rows.next() {
        Some(Ok(sql)) => Ok(sql),
        _ => Err(format!("No such table: {}", table)),
    }
}

// ─── MySQL ─────────────────────────────────────────────────────────

fn parse_mysql_param(s: &str) -> mysql::Value {
    if s.eq_ignore_ascii_case("null") {
        mysql::Value::NULL
    } else if let Ok(i) = s.parse::<i64>() {
        mysql::Value::Int(i)
    } else if let Ok(f) = s.parse::<f64>() {
        mysql::Value::Double(f)
    } else {
        mysql::Value::Bytes(s.as_bytes().to_vec())
    }
}

fn exec_mysql(
    conn: &mut mysql::PooledConn,
    query: &str,
    params: &[String],
) -> Result<(String, bool), String> {
    let kw = query.trim_start().to_uppercase();
    let is_query = kw.starts_with("SELECT")
        || kw.starts_with("SHOW")
        || kw.starts_with("EXPLAIN")
        || kw.starts_with("DESCRIBE")
        || kw.starts_with("WITH");

    let param_values: Vec<mysql::Value> = params.iter().map(|p| parse_mysql_param(p)).collect();
    let param_refs: Vec<&dyn ToValue> = param_values.iter().map(|v| v as &dyn ToValue).collect();

    if is_query {
        let mut result = conn
            .exec_iter(query, param_refs.as_slice())
            .map_err(|e| format!("Failed to execute query: {}", e))?;
        let columns: Vec<String> = result
            .columns()
            .as_ref()
            .iter()
            .map(|c| c.name_str().to_string())
            .collect();
        let mut results: Vec<Value> = Vec::new();
        for row_result in &mut result {
            let row = row_result.map_err(|e| format!("Failed to fetch row: {}", e))?;
            let mut obj = serde_json::Map::new();
            for (i, col) in columns.iter().enumerate() {
                let val: mysql::Value = row.get(i).unwrap_or(mysql::Value::NULL);
                obj.insert(col.clone(), mysql_value_to_json(val));
            }
            results.push(Value::Object(obj));
        }
        Ok((serde_json::to_string_pretty(&results).map_err(|e| format!("JSON error: {}", e))?, true))
    } else {
        conn.exec_drop(query, param_refs.as_slice())
            .map_err(|e| format!("Failed to execute statement: {}", e))?;
        let affected: u64 = conn
            .query_first("SELECT ROW_COUNT()")
            .map_err(|e| format!("Failed to get row count: {}", e))?
            .unwrap_or(0);
        Ok((format!("{}", affected), false))
    }
}

fn mysql_tables(conn: &mut mysql::PooledConn) -> Result<String, String> {
    let tables: Vec<String> = conn
        .exec("SHOW TABLES", ())
        .map_err(|e| format!("{}", e))?;
    let mut out = String::new();
    for (i, t) in tables.iter().enumerate() {
        out.push_str(&format!(
            "  {:>3}  {}\n",
            format!("{}.", i + 1).dimmed(),
            t
        ));
    }
    if tables.is_empty() {
        out.push_str(&format!("  {}\n", "(no tables)".dimmed()));
    }
    out.push_str(&format!("  {}\n", format!("{} table(s)", tables.len()).dimmed()));
    Ok(out)
}

fn mysql_schema(conn: &mut mysql::PooledConn, table: &str) -> Result<String, String> {
    let safe_name = escape_mysql_table(table);
    let mut result = conn
        .exec_iter(format!("SHOW CREATE TABLE `{}`", safe_name).as_str(), ())
        .map_err(|e| format!("{}", e))?;
    for row_result in &mut result {
        let row = row_result.map_err(|e| format!("{}", e))?;
        let create: mysql::Value = row.get(1).unwrap_or(mysql::Value::NULL);
        if let mysql::Value::Bytes(b) = create {
            return Ok(String::from_utf8_lossy(&b).to_string());
        }
    }
    Err(format!("No such table: {}", table))
}

fn mysql_value_to_json(val: mysql::Value) -> Value {
    match val {
        mysql::Value::NULL => Value::Null,
        mysql::Value::Bytes(b) => String::from_utf8(b)
            .map(Value::String)
            .unwrap_or_else(|orig| json!(format!("0x{}", hex::encode(orig.as_bytes())))),
        mysql::Value::Int(i) => json!(i),
        mysql::Value::UInt(u) => json!(u),
        mysql::Value::Float(f) => json!(f),
        mysql::Value::Double(d) => json!(d),
        mysql::Value::Date(y, mo, d, h, mi, s, micro) => {
            json!(format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}", y, mo, d, h, mi, s, micro))
        }
        mysql::Value::Time(neg, d, h, m, s, micro) => {
            let sign = if neg { "-" } else { "" };
            json!(format!("{}{}d {:02}:{:02}:{:02}.{:06}", sign, d, h, m, s, micro))
        }
    }
}

// ─── UI / Output ───────────────────────────────────────────────────

fn print_banner() {
    println!(
        "{}",
        r#"
  _____ _______       ____  ____
 |  __ \___  / |     | __ )/ ___|
 | |  | | / /| |     |  _ \\___ \
 | |  | / /_| | ___ | |_) |___) |
 | |  / / _` |___|||  __/ ___) |
 |_| /_/ \_\_|    |_|   |____/"#
            .green()
    );
    println!("  {}", format!("v{}", VERSION).dimmed());
}

macro_rules! help_cmd {
    ($cmd:expr, $desc:expr) => {
        println!("    {:<30} {}", $cmd.cyan(), $desc.dimmed());
    };
}

fn print_help() {
    let divider = "─────────────────────────────────────────";
    println!();
    println!("  {}", "dbcli".cyan().bold());
    println!("  {}", divider.dimmed());
    println!();
    println!("  {}", "Connection".bold());
    println!();
    help_cmd!(".connect sqlite <file>", "Connect to a SQLite database file");
    help_cmd!(".connect mysql <url>", "Connect to MySQL");
    println!(
        "    {}",
        "e.g. .connect mysql mysql://root:pass@127.0.0.1:3306/mydb".dimmed()
    );
    help_cmd!(".disconnect", "Disconnect current database");
    help_cmd!(".status", "Show connection status");
    println!();
    println!("  {}", "Schema".bold());
    println!();
    help_cmd!(".tables (.t)", "List all tables in current database");
    help_cmd!(".schema <table> (.s)", "Show CREATE TABLE statement");
    println!();
    println!("  {}", "Output".bold());
    println!();
    help_cmd!(".output <file> (.o)", "Redirect query results to file (append)");
    help_cmd!(".output off", "Stop redirecting, print to stdout");
    help_cmd!(".output", "Show current output destination");
    println!();
    println!("  {}", "General".bold());
    println!();
    help_cmd!(".help (.h ?)", "Show this help message");
    help_cmd!(".clear (.cls)", "Clear the screen");
    help_cmd!(".quit (.exit)", "Exit dbcli");
    println!();
    println!("  {}", "MQTT Remote".bold());
    println!();
    help_cmd!(".mqtt connect <broker>", "Connect to MQTT broker");
    help_cmd!(".mqtt disconnect", "Disconnect from MQTT broker");
    help_cmd!(".mqtt status", "Show MQTT connection status");
    help_cmd!(".mqtt use <agent_id>", "Remote mode: SQL auto-executes on agent");
    help_cmd!(".mqtt use local", "Switch back to local mode");
    help_cmd!(".mqtt chat on/off", "Enable/disable auto chat display");
    help_cmd!(".mqtt chat with <id> (.w)", "Enter chat mode with target");
    help_cmd!(".mqtt chat send <id> <msg> (.s)", "Send message to a client/agent");
    help_cmd!(".mqtt chat broadcast <msg>", "Send message to all online");
    help_cmd!(".mqtt chat (.m)", "Show pending chat messages");
    help_cmd!(".mqtt exec <agent> \"<sql>\"", "Execute SQL on remote agent");
    help_cmd!(".mqtt agent start <id>", "Start local agent (serve commands)");
    help_cmd!(".mqtt agent stop", "Stop local agent");
    help_cmd!(".mqtt agents (.a)", "Show known agents");
    println!();
    println!("  {}", "AI Assistant (Agnes AI)".bold());
    println!();
    help_cmd!(".ai connect <key>", "Connect to Agnes AI API");
    help_cmd!(".ai config", "Show AI configuration");
    help_cmd!(".ai sql <description>", "Natural language �?SQL (auto-execute)");
    help_cmd!(".ai analyze", "Analyze recent query results");
    help_cmd!(".ai diagnose", "Diagnose database schema issues");
    help_cmd!(".ai optimize <sql>", "Get SQL optimization suggestions");
    help_cmd!(".ai explain <sql>", "Get SQL explanation");
    help_cmd!(".ai report <sql>", "Generate Markdown report from SQL");
    help_cmd!(".ai schema <description>", "Generate CREATE TABLE from description");
    help_cmd!(".ai chat [name]", "Enter AI chat mode");
    help_cmd!(".ai search on/off", "Enable/disable web search");
    help_cmd!(".ai search tavily <key>", "Set Tavily API key for better search");
    println!();
    println!("  {}", "CLI usage:".dimmed());
    println!("    {}", "Start MQTT agent: dbcli agent --broker <url> --id <name> --user <u> --pass <p>".dimmed());
    println!();
    println!("  {}", divider.dimmed());
    println!();
    println!("  {}", "SQL tips:".dimmed());
    println!("    * Keywords, strings, and functions are syntax-highlighted");
    println!("    * Statements end with a semicolon {}", ";".cyan());
    println!("    * Multi-line input is supported");
    println!("    * History is saved between sessions");
    println!();
}

fn make_plain_prompt(output_file: &Option<PathBuf>, chat_target: &Option<String>, ai_chat: &Option<String>) -> String {
    if let Some(target) = ai_chat {
        return format!("[AI:{}]", target);
    }
    match chat_target {
        Some(target) => format!("[{}]", target),
        None => match output_file {
            Some(f) => format!("dbcli>>{}", f.display()),
            None => "dbcli>>".to_string(),
        },
    }
}

fn write_output(output_file: &Option<PathBuf>, text: &str) {
    match output_file {
        Some(path) => {
            let mut file = match OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                Ok(f) => f,
                Err(e) => {
                    println!(
                        "  {} Cannot write to '{}': {}",
                        "x".red().bold(),
                        path.display(),
                        e
                    );
                    return;
                }
            };
            if let Err(e) = writeln!(file, "{}", text) {
                println!("  {} Write error: {}", "x".red().bold(), e);
            }
        }
        None => print!("{}", text),
    }
}

fn exec_on_db(
    db_type: &Option<DbType>,
    query: &str,
    output_file: &Option<PathBuf>,
) -> Result<(), String> {
    let start = Instant::now();
    let result = match db_type {
        Some(DbType::Sqlite(conn)) => exec_sqlite(conn, query, &[]),
        Some(DbType::Mysql(pool)) => pool
            .get_conn()
            .map_err(|e| e.to_string())
            .and_then(|mut c| exec_mysql(&mut c, query, &[])),
        None => Err("No connection. Use .connect to connect to a database.".into()),
    };
    let elapsed = start.elapsed();

    match result {
        Ok((text, is_select)) => {
            if is_select {
                let row_count = serde_json::from_str::<Vec<Value>>(&text)
                    .map(|v| v.len())
                    .unwrap_or(0);
                write_output(output_file, &text);
                if output_file.is_some() {
                    println!(
                        "  {} {} rows written in {:.2?}",
                        ">>".green(),
                        row_count.to_string().cyan(),
                        elapsed
                    );
                } else {
                    println!();
                    println!(
                        "  {} {}",
                        "---".dimmed(),
                        format!("{} row(s) in {:.2?}", row_count, elapsed).dimmed()
                    );
                    println!();
                }
            } else {
                let affected: u64 = text.parse().unwrap_or(0);
                println!(
                    "  {} {}",
                    "+".green(),
                    format!("{} row(s) affected", affected).green()
                );
            }
        }
        Err(e) => {
            println!();
            println!("  {} {}", "x".red().bold(), e.red());
            println!();
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn handle_dot_command(
    cmd: &str,
    db_type: &mut Option<DbType>,
    output_file: &mut Option<PathBuf>,
    mqtt_state: &mut MqttState,
    agent_state: &mut AgentState,
    remote_agent: &mut Option<String>,
    chat_target: &mut Option<String>,
    ai_state: &mut AiState,
) -> bool {
    let trimmed = cmd.trim();
    let parts: Vec<&str> = trimmed.splitn(3, ' ').filter(|s| !s.is_empty()).collect();
    match parts[0] {
        ".quit" | ".exit" => {
            if agent_state.running {
                if let Some(tx) = agent_state.stop_tx.take() {
                    let _ = tx.send(());
                }
                agent_state.running = false;
                println!("  {} Stopped agent '{}'.", "-".dimmed(), agent_state.agent_id.cyan());
            }
            if mqtt_state.client.is_some() {
                mqtt_disconnect(mqtt_state);
                println!("  {} Disconnected from MQTT broker.", "-".dimmed());
            }
            println!();
            println!("  {}", "Thank you for using dbcli. Goodbye!".dimmed());
            println!();
            return true;
        }
        ".help" | ".h" | "?" => print_help(),
        ".clear" | ".cls" => {
            print!("\x1B[2J\x1B[1;1H");
        }
        ".status" => {
            println!();
            println!("  {}", "Connection Status".bold());
            println!("  {}", "─────────────────────────────".dimmed());
            match db_type {
                Some(db) => {
                    println!("  {:<12} {}", "Type:".dimmed(), db.kind().cyan());
                    println!("  {:<12} {}", "Name:".dimmed(), db.label().white().bold());
                }
                None => {
                    println!("  {:<12} {}", "Status:".dimmed(), "Not connected".red());
                    println!(
                        "  {}",
                        "Use .connect <sqlite|mysql> <target> to connect.".dimmed()
                    );
                }
            }
            println!("  {}", "─────────────────────────────".dimmed());
            match output_file {
                Some(f) => {
                    println!(
                        "  {:<12} {}",
                        "Output:".dimmed(),
                        f.display().to_string().green()
                    );
                }
                None => {
                    println!("  {:<12} {}", "Output:".dimmed(), "stdout".dimmed());
                }
            }
            println!();
        }
        ".disconnect" => {
            *db_type = None;
            println!("  {} {}", ">".green(), "Disconnected.".dimmed());
        }
        ".connect" => {
            if parts.len() < 3 {
                println!();
                println!("  {}", "Usage:".bold());
                println!("    {} <file>           (SQLite)", ".connect sqlite".cyan());
                println!("    {} <url>            (MySQL)", ".connect mysql".cyan());
                println!();
                println!("  {}", "Examples:".bold());
                println!("    {}", ".connect sqlite ./data/mydb.db".dimmed());
                println!(
                    "    {}",
                    ".connect mysql mysql://root:pass@127.0.0.1:3306/mydb".dimmed()
                );
                println!();
                return false;
            }
            let kind = parts[1];
            let target = parts[2].trim();
            println!("  {} Connecting to {}...", "->".dimmed(), target.white());
            let new_db = match kind {
                "sqlite" | "sql" | "sq" => connect_sqlite(target),
                "mysql" | "my" | "m" => connect_mysql(target),
                _ => {
                    println!(
                        "  {} Unknown type '{}'. Use {} or {}.",
                        "x".red().bold(),
                        kind.red(),
                        "sqlite".cyan(),
                        "mysql".cyan()
                    );
                    return false;
                }
            };
            match new_db {
                Ok(db) => {
                    println!(
                        "  {} {} ({})",
                        "+".green().bold(),
                        db.label().cyan().bold(),
                        db.kind().dimmed()
                    );
                    *db_type = Some(db);
                }
                Err(e) => {
                    println!("  {} {}", "x".red().bold(), e.red());
                }
            }
        }
        ".output" => match parts.get(1) {
            None => match output_file {
                Some(f) => {
                    println!("  {} {}", ">>".green(), f.display().to_string().cyan());
                }
                None => {
                    println!("  {} {}", ">>".dimmed(), "stdout".dimmed());
                }
            },
            Some(target) => {
                let target = target.trim();
                if target.eq_ignore_ascii_case("off")
                    || target.eq_ignore_ascii_case("stdout")
                    || target == "-"
                {
                    *output_file = None;
                    println!("  {} Output -> {}", "+".green(), "stdout".green());
                } else {
                    match OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(target)
                    {
                        Ok(_) => {
                            *output_file = Some(PathBuf::from(target));
                            println!("  {} Output -> {}", "+".green(), target.cyan());
                        }
                        Err(e) => {
                            println!(
                                "  {} Cannot open '{}': {}",
                                "x".red().bold(),
                                target.red(),
                                e
                            );
                        }
                    }
                }
            }
        },
        ".mqtt" => {
            let sub = parts.get(1).map(|s| s.trim()).unwrap_or("");
            match sub {
                "connect" | "c" => {
                    if parts.len() < 3 {
                        println!("  {} Usage: {} <broker_url> --user <user> --pass <pass> [--tls]",
                            "!".yellow(), ".mqtt connect".cyan());
                        println!("  {}", "e.g. .mqtt connect mqtt://broker.emqxsl.cn:1883 --user myuser --pass mypass".dimmed());
                        return false;
                    }
                    let rest = parts[2].trim();
                    let url_part: Vec<&str> = rest.splitn(2, " --").collect();
                    let broker_url = url_part[0].trim().to_string();

                    let mut user = String::new();
                    let mut pass = String::new();
                    let mut tls = false;

                    if url_part.len() > 1 {
                        let flags: Vec<&str> = url_part[1].split(" --").collect();
                        for flag in &flags {
                            let kv: Vec<&str> = flag.splitn(2, ' ').collect();
                            match kv[0] {
                                "user" | "u" => {
                                    user = kv.get(1).unwrap_or(&"").to_string();
                                }
                                "pass" | "w" | "password" => {
                                    pass = kv.get(1).unwrap_or(&"").to_string();
                                }
                                "tls" | "ssl" => {
                                    tls = true;
                                }
                                _ => {}
                            }
                        }
                    }

                    println!("  {} Connecting to MQTT broker {}...", "->".dimmed(), broker_url.white());
                    match mqtt_connect(mqtt_state, &broker_url, &user, &pass, tls) {
                        Ok(()) => {
                            println!("  {} Connected to {} ({})", "+".green().bold(),
                                mqtt_state.broker_url.cyan().bold(), mqtt_state.client_id.dimmed());
                        }
                        Err(e) => {
                            println!("  {} {}", "x".red().bold(), e.red());
                        }
                    }
                }
                "disconnect" | "d" => {
                    mqtt_disconnect(mqtt_state);
                    *remote_agent = None;
                    println!("  {} {}", ">".green(), "MQTT disconnected.".dimmed());
                }
                "use" => {
                    let target = parts.get(2).map(|s| s.trim()).unwrap_or("");
                    if target.is_empty() || target == "local" {
                        *remote_agent = None;
                        println!("  {} Remote mode off. SQL executes locally.", "+".green());
                    } else {
                        *remote_agent = Some(target.to_string());
                        println!("  {} Remote mode on. SQL executes on {}", "+".green(), target.cyan().bold());
                    }
                }
                "chat" => {
                    let chat_rest = parts.get(2).map(|s| s.trim()).unwrap_or("");
                    let chat_args: Vec<&str> = chat_rest.splitn(2, ' ').collect();
                    match chat_args[0] {
                        "on" => {
                            mqtt_state.chat_enabled = true;
                            println!("  {} Chat display enabled.", "+".green());
                        }
                        "off" => {
                            mqtt_state.chat_enabled = false;
                            println!("  {} Chat display disabled.", "+".green());
                        }
                        "with" | "w" => {
                            if chat_args.len() < 2 || chat_args[1].trim().is_empty() {
                                *chat_target = None;
                                println!("  {} Exited chat mode.", "+".green());
                            } else {
                                let target = chat_args[1].trim().to_string();
                                *chat_target = Some(target.clone());
                                println!("  {} Entered chat mode with {}. Type !help for commands.",
                                    "+".green(), target.cyan().bold());
                            }
                        }
                        "send" | "s" => {
                            if chat_args.len() < 2 {
                                println!("  {} Usage: {} <target> <message>", "!".yellow(), ".mqtt chat send".cyan());
                                return false;
                            }
                            let send_parts: Vec<&str> = chat_args[1].splitn(2, ' ').collect();
                            if send_parts.len() < 2 {
                                println!("  {} Usage: {} <target> <message>", "!".yellow(), ".mqtt chat send".cyan());
                                return false;
                            }
                            let target_id = send_parts[0];
                            let msg_text = send_parts[1];
                            if let Some(ref client) = mqtt_state.client {
                                let chat_msg = ChatMsg {
                                    from: mqtt_state.client_id.clone(),
                                    to: target_id.to_string(),
                                    msg: msg_text.to_string(),
                                    timestamp: std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default().as_secs(),
                                };
                                let topic = format!("dbcli/chat/{}", target_id);
                                let rt = match mqtt_state._runtime.as_ref() {
                                    Some(rt) => rt,
                                    None => { println!("  {} No MQTT runtime.", "x".red().bold()); return false; }
                                };
                                let payload = match serde_json::to_vec(&chat_msg) {
                                    Ok(p) => p,
                                    Err(e) => { println!("  {} Serialization error: {}", "x".red().bold(), e); return false; }
                                };
                                rt.block_on(async {
                                    let _ = client.publish(topic, QoS::AtMostOnce, false, payload).await;
                                });
                                println!("  {} [you -> {}] {}", "->".dimmed(), target_id.cyan(), msg_text);
                            } else {
                                println!("  {} Not connected. Use .mqtt connect first.", "x".red().bold());
                            }
                        }
                        "broadcast" | "b" => {
                            if chat_args.len() < 2 || chat_args[1].trim().is_empty() {
                                println!("  {} Usage: {} <message>", "!".yellow(), ".mqtt chat broadcast".cyan());
                                return false;
                            }
                            let msg_text = chat_args[1];
                            if let Some(ref client) = mqtt_state.client {
                                let chat_msg = ChatMsg {
                                    from: mqtt_state.client_id.clone(),
                                    to: "broadcast".to_string(),
                                    msg: msg_text.to_string(),
                                    timestamp: std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default().as_secs(),
                                };
                                let rt = match mqtt_state._runtime.as_ref() {
                                    Some(rt) => rt,
                                    None => { println!("  {} No MQTT runtime.", "x".red().bold()); return false; }
                                };
                                let payload = match serde_json::to_vec(&chat_msg) {
                                    Ok(p) => p,
                                    Err(e) => { println!("  {} Serialization error: {}", "x".red().bold(), e); return false; }
                                };
                                rt.block_on(async {
                                    let _ = client.publish("dbcli/chat/broadcast".to_string(), QoS::AtMostOnce, false, payload).await;
                                });
                                println!("  {} [broadcast] {}", "->".dimmed(), msg_text);
                            } else {
                                println!("  {} Not connected. Use .mqtt connect first.", "x".red().bold());
                            }
                        }
                        _ => {
                            // Show pending chat messages
                            let msgs: Vec<ChatMsg> = mqtt_state.chat_queue.lock().unwrap_or_else(|e| e.into_inner()).drain(..).collect();
                            if msgs.is_empty() {
                                println!("  {}", "No new messages.".dimmed());
                            } else {
                                for m in &msgs {
                                    println!("  {} [{}] {}", "<".green(), m.from.cyan(), m.msg);
                                }
                            }
                        }
                    }
                }
                "status" | "s" => {
                    println!();
                    println!("  {}", "MQTT Status".bold());
                    println!("  {}", "─────────────────────────────".dimmed());
                    if mqtt_state.client.is_some() {
                        println!("  {:<12} {}", "Status:".dimmed(), "Connected".green());
                        println!("  {:<12} {}", "Broker:".dimmed(), mqtt_state.broker_url.cyan());
                        println!("  {:<12} {}", "Client ID:".dimmed(), mqtt_state.client_id.white());
                        let pending_count = mqtt_state.pending.lock().unwrap_or_else(|e| e.into_inner()).len();
                        println!("  {:<12} {} pending", "Requests:".dimmed(), pending_count);
                    } else {
                        println!("  {:<12} {}", "Status:".dimmed(), "Not connected".red());
                        println!("  {}", "Use .mqtt connect <broker> to connect.".dimmed());
                    }
                    println!("  {}", "─────────────────────────────".dimmed());
                    println!();
                }
                "exec" | "e" => {
                    if parts.len() < 3 {
                        println!("  {} Usage: {} <agent_id> \"<sql>\" [--db sqlite|mysql]",
                            "!".yellow(), ".mqtt exec".cyan());
                        return false;
                    }
                    let rest = parts[2].trim();
                    // Find agent_id (first word)
                    let first_space = rest.find(' ').unwrap_or(rest.len());
                    let agent_id = rest[..first_space].trim();
                    let after_id = rest[first_space..].trim();
                    // Extract SQL from quotes or take rest as-is
                    let (sql, db_kind) = if after_id.starts_with('"') || after_id.starts_with('\'') {
                        let quote = after_id.chars().next().unwrap();
                        // Find closing quote, skipping doubled quotes (e.g. O''Brien)
                        let bytes = after_id.as_bytes();
                        let mut i = 1; // skip opening quote
                        let end = loop {
                            if i >= bytes.len() { break after_id.len(); }
                            if bytes[i] == quote as u8 {
                                if i + 1 < bytes.len() && bytes[i + 1] == quote as u8 {
                                    i += 2; // skip escaped quote
                                } else {
                                    break i + 1; // found closing quote
                                }
                            } else {
                                i += 1;
                            }
                        };
                        let sql_part = after_id[1..end].trim();
                        let after_sql = after_id[end..].trim();
                        let mut dk = "sqlite";
                        if after_sql.starts_with("--db") {
                            let dv: Vec<&str> = after_sql.splitn(2, ' ').collect();
                            if let Some(v) = dv.get(1) { dk = v; }
                        }
                        (sql_part, dk)
                    } else {
                        let mut dk = "sqlite";
                        if after_id.starts_with("--db") {
                            let dv: Vec<&str> = after_id.splitn(2, ' ').collect();
                            if let Some(v) = dv.get(1) { dk = v; }
                        }
                        (after_id, dk)
                    };
                    if agent_id.is_empty() || sql.is_empty() {
                        println!("  {} Usage: {} <agent_id> \"<sql>\"",
                            "!".yellow(), ".mqtt exec".cyan());
                        return false;
                    }

                    println!("  {} Executing on {}...", "->".dimmed(), agent_id.white());
                    let start = Instant::now();
                    match mqtt_exec_remote(mqtt_state, agent_id, db_kind, sql, &[]) {
                        Ok(resp) => {
                            if resp.ok {
                                if let Some(rows) = &resp.rows {
                                    let json_str = serde_json::to_string_pretty(rows).unwrap_or_default();
                                    println!();
                                    println!("{}", json_str);
                                    println!();
                                    println!("  {} {} in {:.2?}",
                                        "---".dimmed(),
                                        format!("{} row(s)", rows.len()).dimmed(),
                                        start.elapsed());
                                } else if let Some(cols) = &resp.columns {
                                    println!("  {} Columns: {:?}", "+".green(), cols);
                                }
                                println!();
                            } else {
                                println!("  {} {}", "x".red().bold(),
                                    resp.error.unwrap_or_else(|| "Unknown error".to_string()).red());
                            }
                        }
                        Err(e) => {
                            println!("  {} {}", "x".red().bold(), e.red());
                        }
                    }
                }
                "agent" => {
                    let sub2_full = parts.get(2).map(|s| s.trim()).unwrap_or("");
                    let sub2_parts: Vec<&str> = sub2_full.splitn(2, ' ').collect();
                    let sub2 = sub2_parts[0];
                    let sub2_arg = sub2_parts.get(1).map(|s| s.trim()).unwrap_or("");
                    match sub2 {
                        "start" => {
                            if agent_state.running {
                                println!("  {} Agent '{}' is already running.",
                                    "!".yellow(), agent_state.agent_id.cyan());
                                return false;
                            }

                            // Parse: .mqtt agent start <agent_id> [broker] [--user u] [--pass p] [--tls] [-d db]
                            let args: Vec<&str> = sub2_arg.split_whitespace().collect();
                            let agent_id = args.first().unwrap_or(&"default").to_string();

                            // Check if broker is provided inline
                            let mut inline_broker = String::new();
                            let mut inline_user = String::new();
                            let mut inline_pass = String::new();
                            let mut inline_tls = false;
                            let mut inline_db = String::new();

                            let mut i = 1;
                            while i < args.len() {
                                match args[i] {
                                    "--user" | "-u" => {
                                        if let Some(v) = args.get(i + 1) { inline_user = v.to_string(); i += 2; } else { i += 1; }
                                    }
                                    "--pass" | "-w" | "--password" => {
                                        if let Some(v) = args.get(i + 1) { inline_pass = v.to_string(); i += 2; } else { i += 1; }
                                    }
                                    "--tls" | "--ssl" => { inline_tls = true; i += 1; }
                                    "-d" | "--database" => {
                                        if let Some(v) = args.get(i + 1) { inline_db = v.to_string(); i += 2; } else { i += 1; }
                                    }
                                    _ => {
                                        // If it starts with mqtt:// it's the broker URL
                                        if args[i].starts_with("mqtt://") || args[i].starts_with("mqtts://") {
                                            inline_broker = args[i].to_string();
                                        }
                                        i += 1;
                                    }
                                }
                            }

                            // Connect MQTT if broker provided or not connected yet
                            if !inline_broker.is_empty() || mqtt_state.client.is_none() {
                                let broker = if !inline_broker.is_empty() { inline_broker } else {
                                    println!("  {} No broker specified. Use: .mqtt agent start <id> <broker_url> --user <u> --pass <p>",
                                        "x".red().bold());
                                    return false;
                                };
                                let user = if !inline_user.is_empty() { inline_user } else { mqtt_state.mqtt_user.clone() };
                                let pass = if !inline_pass.is_empty() { inline_pass } else { mqtt_state.mqtt_pass.clone() };
                                let tls = if inline_tls { true } else { mqtt_state.mqtt_tls };

                                println!("  {} Connecting to MQTT broker {}...", "->".dimmed(), broker.white());
                                if let Err(e) = mqtt_connect(mqtt_state, &broker, &user, &pass, tls) {
                                    println!("  {} {}", "x".red().bold(), e.red());
                                    return false;
                                }
                            }

                            // Connect database if provided
                            if !inline_db.is_empty() && db_type.is_none() {
                                let db_result = if inline_db.contains("://") {
                                    connect_mysql(&inline_db)
                                } else {
                                    connect_sqlite(&inline_db)
                                };
                                match db_result {
                                    Ok(db) => {
                                        println!("  {} {} ({})", "+".green().bold(), db.label().cyan().bold(), db.kind().dimmed());
                                        *db_type = Some(db);
                                    }
                                    Err(e) => {
                                        println!("  {} {}", "x".red().bold(), e.red());
                                        return false;
                                    }
                                }
                            }

                            if mqtt_state.client.is_none() {
                                println!("  {} Not connected to MQTT broker.",
                                    "x".red().bold());
                                return false;
                            }
                            if db_type.is_none() {
                                println!("  {} No database connected. Use .connect or -d <path>.",
                                    "x".red().bold());
                                return false;
                            }

                            // Clone DB connection for agent thread
                            let agent_db = match db_type.as_ref().unwrap() {
                                DbType::Sqlite(conn) => {
                                    match conn.path() {
                                        Some(path) => {
                                            match rusqlite::Connection::open(path) {
                                                Ok(c) => DbType::Sqlite(c),
                                                Err(e) => {
                                                    println!("  {} Failed to open DB: {}", "x".red().bold(), e.to_string().red());
                                                    return false;
                                                }
                                            }
                                        }
                                        None => {
                                            // In-memory database — open separate in-memory connection
                                            match rusqlite::Connection::open(":memory:") {
                                                Ok(c) => DbType::Sqlite(c),
                                                Err(e) => {
                                                    println!("  {} Failed to open DB: {}", "x".red().bold(), e.to_string().red());
                                                    return false;
                                                }
                                            }
                                        }
                                    }
                                }
                                DbType::Mysql(pool) => DbType::Mysql(pool.clone()),
                            };

                            // Capture broker info as owned strings (no borrows from mqtt_state)
                            let broker_host = mqtt_state.broker_url
                                .replace("mqtt://", "").replace("mqtts://", "")
                                .trim_end_matches('/')
                                .to_string();
                            let mqtt_user = mqtt_state.mqtt_user.clone();
                            let mqtt_pass = mqtt_state.mqtt_pass.clone();
                            let mqtt_tls = mqtt_state.mqtt_tls;
                            let a_id = agent_id.clone();
                            let (stop_tx, stop_rx) = std::sync::mpsc::channel();

                            std::thread::spawn(move || {
                                let rt = match tokio::runtime::Runtime::new() {
                                    Ok(rt) => rt,
                                    Err(e) => { eprintln!("  Agent runtime error: {}", e); return; }
                                };
                                rt.block_on(async move {
                                    // Create own MQTT connection
                                    let client_id = format!("agent-{}-{}", a_id, &Uuid::new_v4().to_string()[..8]);
                                    let default_port = if mqtt_tls { 8883 } else { 1883 };
                                    let (host, port) = parse_broker_host_port(&broker_host, default_port);

                                    let mut opts = MqttOptions::new(&client_id, &host, port);
                                    opts.set_credentials(&mqtt_user, &mqtt_pass);
                                    opts.set_keep_alive(std::time::Duration::from_secs(30));
                                    if mqtt_tls {
                                        opts.set_transport(rumqttc::Transport::Tls(
                                            rumqttc::TlsConfiguration::default(),
                                        ));
                                    }

                                    let (client, mut event_loop) = AsyncClient::new(opts, 100);
                                    let topic = format!("dbcli/cmd/{}", a_id);

                                    // Wait for connack BEFORE subscribing (MQTT protocol)
                                    match tokio::time::timeout(
                                        std::time::Duration::from_secs(5), event_loop.poll()
                                    ).await {
                                        Ok(Ok(Event::Incoming(rumqttc::Packet::ConnAck(_)))) => {}
                                        _ => {
                                            eprintln!("  {} Agent failed to connect to broker", "x".red().bold());
                                            return;
                                        }
                                    }

                                    // Subscribe after ConnAck
                                    if let Err(e) = client.subscribe(&topic, QoS::AtLeastOnce).await {
                                        eprintln!("  {} Agent subscribe error: {}", "x".red().bold(), e);
                                        return;
                                    }
                                    println!("  {} Agent '{}' connected and listening on {}",
                                        "+".green().bold(), a_id.cyan(), topic.dimmed());

                                    let local_db = Arc::new(Mutex::new(Some(agent_db)));

                                    // Spawn heartbeat
                                    let hb_client = client.clone();
                                    let hb_id = a_id.clone();
                                    tokio::spawn(async move {
                                        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
                                        loop {
                                            interval.tick().await;
                                            let payload = json!({
                                                "agent_id": hb_id,
                                                "status": "online",
                                                "timestamp": std::time::SystemTime::now()
                                                    .duration_since(std::time::UNIX_EPOCH)
                                                    .unwrap_or_default().as_secs()
                                            });
                                            let _ = hb_client
                                                .publish(format!("dbcli/heartbeat/{}", hb_id), QoS::AtMostOnce, false,
                                                    serde_json::to_vec(&payload).unwrap_or_default())
                                                .await;
                                        }
                                    });

                                    // Main event loop
                                    loop {
                                        if stop_rx.try_recv().is_ok() {
                                            println!("  {} Agent '{}' stopped.", ">".green(), a_id.cyan());
                                            let _ = client.disconnect().await;
                                            break;
                                        }
                                        match tokio::time::timeout(
                                            std::time::Duration::from_millis(100), event_loop.poll()
                                        ).await {
                                            Ok(Ok(Event::Incoming(rumqttc::Packet::Publish(p)))) => {
                                                if let Ok(cmd) = serde_json::from_slice::<MqttCmd>(&p.payload) {
                                                    let resp = {
                                                        let db_lock = local_db.lock().unwrap_or_else(|e| e.into_inner());
                                                        match db_lock.as_ref() {
                                                            Some(DbType::Sqlite(conn)) => {
                                                                match exec_sqlite(conn, &cmd.sql, &cmd.params) {
                                                                    Ok((text, is_select)) => {
                                                                        if is_select {
                                                                            let rows: Vec<Value> = serde_json::from_str(&text).unwrap_or_default();
                                                                            let columns = rows.first()
                                                                                .and_then(|r| r.as_object())
                                                                                .map(|m| m.keys().cloned().collect())
                                                                                .unwrap_or_default();
                                                                            MqttResp { request_id: cmd.request_id, ok: true, columns: Some(columns), rows: Some(rows), affected: None, error: None }
                                                                        } else {
                                                                            let affected: u64 = text.parse().unwrap_or(0);
                                                                            MqttResp { request_id: cmd.request_id, ok: true, columns: None, rows: None, affected: Some(affected), error: None }
                                                                        }
                                                                    }
                                                                    Err(e) => MqttResp { request_id: cmd.request_id, ok: false, columns: None, rows: None, affected: None, error: Some(e) },
                                                                }
                                                            }
                                                            Some(DbType::Mysql(pool)) => {
                                                                match pool.get_conn() {
                                                                    Ok(mut conn) => {
                                                                        match exec_mysql(&mut conn, &cmd.sql, &cmd.params) {
                                                                            Ok((text, is_select)) => {
                                                                                if is_select {
                                                                                    let rows: Vec<Value> = serde_json::from_str(&text).unwrap_or_default();
                                                                                    let columns = rows.first()
                                                                                        .and_then(|r| r.as_object())
                                                                                        .map(|m| m.keys().cloned().collect())
                                                                                        .unwrap_or_default();
                                                                                    MqttResp { request_id: cmd.request_id, ok: true, columns: Some(columns), rows: Some(rows), affected: None, error: None }
                                                                                } else {
                                                                                    let affected: u64 = text.parse().unwrap_or(0);
                                                                                    MqttResp { request_id: cmd.request_id, ok: true, columns: None, rows: None, affected: Some(affected), error: None }
                                                                                }
                                                                            }
                                                                            Err(e) => MqttResp { request_id: cmd.request_id, ok: false, columns: None, rows: None, affected: None, error: Some(e) },
                                                                        }
                                                                    }
                                                                    Err(e) => MqttResp { request_id: cmd.request_id, ok: false, columns: None, rows: None, affected: None, error: Some(e.to_string()) },
                                                                }
                                                            }
                                                            None => MqttResp { request_id: cmd.request_id, ok: false, columns: None, rows: None, affected: None, error: Some("No database connected".to_string()) },
                                                        }
                                                    };
                                                    let resp_topic = format!("dbcli/resp/{}", resp.request_id);
                                                    if let Ok(payload) = serde_json::to_vec(&resp) {
                                                        let _ = client.publish(resp_topic, QoS::AtLeastOnce, false, payload).await;
                                                    }
                                                }
                                            }
                                            Ok(Ok(_)) => {}
                                            Ok(Err(_)) => {}
                                            Err(_) => {}
                                        }
                                    }
                                });
                            });

                            agent_state.running = true;
                            agent_state.agent_id = agent_id.clone();
                            agent_state.stop_tx = Some(stop_tx);
                            println!("  {} Agent '{}' starting...", "->".dimmed(), agent_id.cyan().bold());
                        }
                        "stop" => {
                            if !agent_state.running {
                                println!("  {} No agent running.", "!".yellow());
                                return false;
                            }
                            if let Some(tx) = agent_state.stop_tx.take() {
                                let _ = tx.send(());
                            }
                            agent_state.running = false;
                            agent_state.agent_id.clear();
                        }
                        "status" => {
                            println!();
                            println!("  {}", "Agent Status".bold());
                            println!("  {}", "─────────────────────────────".dimmed());
                            if agent_state.running {
                                println!("  {:<12} {}", "Status:".dimmed(), "Running".green());
                                println!("  {:<12} {}", "Agent ID:".dimmed(), agent_state.agent_id.cyan().bold());
                            } else {
                                println!("  {:<12} {}", "Status:".dimmed(), "Not running".dimmed());
                                println!("  {}", "Use .mqtt agent start <id> to start.".dimmed());
                            }
                            println!("  {}", "─────────────────────────────".dimmed());
                            println!();
                        }
                        _ => {
                            println!("  {} Usage:", "!".yellow());
                            println!("    {} <id> [broker] [opts]  Start agent (one command)", ".mqtt agent start".cyan());
                            println!("    {}                        Stop running agent", ".mqtt agent stop".cyan());
                            println!("    {}                        Show agent status", ".mqtt agent status".cyan());
                            println!();
                            println!("  {}:", "Options".bold());
                            println!("    {} MQTT broker URL (e.g. mqtt://host:8883)", "broker".dimmed());
                            println!("    {} --user <u> --pass <p> --tls  MQTT auth", "  ".dimmed());
                            println!("    {} -d <path>                     Database path", "  ".dimmed());
                            println!();
                            println!("  {}:", "Examples".bold());
                            println!("    {}", ".mqtt agent start s1 mqtt://broker:8883 --user u --pass p --tls -d my.db".dimmed());
                            println!("    {}", ".mqtt agent start s1   (uses existing .mqtt connect)".dimmed());
                        }
                    }
                }
                "agents" | "a" => {
                    if mqtt_state.client.is_none() {
                        println!("  {} MQTT not connected. Use {} first.", "x".red().bold(), ".mqtt connect".cyan());
                    } else {
                        println!("  {} Known agents:", "Agents".bold());
                        if agent_state.running {
                            println!("    {} {} (local)", "*".green(), agent_state.agent_id.cyan());
                        }
                        if let Some(ref id) = *remote_agent {
                            println!("    {} {} (remote)", ">".blue(), id.cyan());
                        }
                        println!();
                        println!("  {} Use {} to discover online agents.", "Tip:".dimmed(), ".mqtt agent start".cyan());
                    }
                }
                _ => {
                    println!("  {} Unknown mqtt subcommand: {}", "!".yellow(), sub.cyan());
                    println!("  {}", ".mqtt connect | disconnect | status | use | chat | exec | agent".dimmed());
                }
            }
        }
        ".ai" => {
            let sub = parts.get(1).map(|s| s.trim()).unwrap_or("");
            match sub {
                "connect" | "c" => {
                    let key = parts.get(2).map(|s| s.trim()).unwrap_or("");
                    if key.is_empty() {
                        println!("  {} Usage: {} <API_KEY>", "!".yellow(), ".ai connect".cyan());
                        return false;
                    }
                    ai_state.api_key = Some(key.to_string());
                    println!("  {} AI connected to Agnes AI.", "+".green());
                    println!("  {} Model: {}", "->".dimmed(), ai_state.model.cyan());
                }
                "config" => {
                    println!();
                    println!("  {}", "AI Configuration".bold());
                    println!("  {}", "─────────────────────────────".dimmed());
                    println!("  {:<12} {}", "API Key:".dimmed(), if ai_state.api_key.is_some() { "Set".green() } else { "Not set".red() });
                    println!("  {:<12} {}", "Model:".dimmed(), ai_state.model.cyan());
                    println!("  {:<12} {}", "Base URL:".dimmed(), ai_state.base_url.dimmed());
                    println!("  {:<12} {} message(s)", "History:".dimmed(), ai_state.history.len().to_string().cyan());
                    println!("  {:<12} {}", "Search:".dimmed(), if ai_state.search_enabled { "Enabled".green() } else { "Disabled".yellow() });
                    println!("  {:<12} {}", "Tavily:".dimmed(), if ai_state.tavily_key.is_some() { "Set".green() } else { "Not set (using DuckDuckGo)".dimmed() });
                    println!("  {}", "─────────────────────────────".dimmed());
                    println!();
                }
                "search" => {
                    let search_rest = parts.get(2).map(|s| s.trim()).unwrap_or("");
                    let sub_parts: Vec<&str> = search_rest.splitn(2, ' ').collect();
                    match sub_parts[0] {
                        "on" => { ai_state.search_enabled = true; println!("  {} Web search enabled.", "+".green()); }
                        "off" => { ai_state.search_enabled = false; println!("  {} Web search disabled.", "+".green()); }
                        "tavily" => {
                            let key = sub_parts.get(1).map(|s| s.trim()).unwrap_or("");
                            if key.is_empty() {
                                println!("  {} Usage: {} <api_key>", "!".yellow(), ".ai search tavily".cyan());
                                println!("  {}", "Get key from https://tavily.com".dimmed());
                            } else {
                                ai_state.tavily_key = Some(key.to_string());
                                println!("  {} Tavily API key set.", "+".green());
                            }
                        }
                        _ => {
                            println!("  {} Usage:", "!".yellow());
                            println!("    {}            Enable web search", ".ai search on".cyan());
                            println!("    {}           Disable web search", ".ai search off".cyan());
                            println!("    {} <key>  Set Tavily API key", ".ai search tavily".cyan());
                        }
                    }
                }
                "sql" | "s" => {
                    if ai_state.api_key.is_none() {
                        println!("  {} AI not connected. Use {} first.", "x".red().bold(), ".ai connect".cyan());
                        return false;
                    }
                    let desc = parts.get(2).map(|s| s.trim()).unwrap_or("");
                    if desc.is_empty() {
                        println!("  {} Usage: {} <description>", "!".yellow(), ".ai sql".cyan());
                        return false;
                    }
                    let schema = ai_get_schema(db_type);
                    let db_kind = match db_type {
                        Some(DbType::Sqlite(_)) => "SQLite",
                        Some(DbType::Mysql(_)) => "MySQL",
                        None => "Unknown",
                    };
                    let system_prompt = format!(
                        "You are a SQL expert. Convert natural language to SQL.\n\
                         Database type: {}\n\
                         Schema: {}\n\
                         Return ONLY the SQL query, no explanation.",
                        db_kind, schema
                    );
                    let messages = vec![
                        AiMessage { role: "system".to_string(), content: Some(system_prompt), tool_calls: None, tool_call_id: None },
                        AiMessage { role: "user".to_string(), content: Some(desc.to_string()), tool_calls: None, tool_call_id: None },
                    ];
                    print!("  {} Thinking...", "->".dimmed());
                    match ai_sync_call(ai_state, &messages) {
                        Ok(sql) => {
                            let sql = sql.trim().trim_matches('`').trim();
                            let sql = if let Some(s) = sql.strip_prefix("sql") { s } else { sql };
                            let sql = sql.trim();
                            println!("\r  {} {}", "SQL:".green().bold(), sql.cyan());
                            println!();
                            println!("  {} Execute this SQL? [y/N]: ", "?".yellow());
                            let mut confirm = String::new();
                            let _ = std::io::stdin().read_line(&mut confirm);
                            if confirm.trim().eq_ignore_ascii_case("y") {
                                let start = Instant::now();
                                match db_type {
                                    Some(DbType::Sqlite(conn)) => {
                                        match exec_sqlite(conn, sql, &[]) {
                                            Ok((text, is_select)) => {
                                                if is_select {
                                                    println!();
                                                    write_output(output_file, &text);
                                                    println!("  {} in {:.2?}", "---".dimmed(), start.elapsed());
                                                } else {
                                                    println!("  {} in {:.2?}", "+".green(), start.elapsed());
                                                }
                                            }
                                            Err(e) => println!("  {} {}", "x".red().bold(), e.to_string().red()),
                                        }
                                    }
                                    Some(DbType::Mysql(pool)) => {
                                        match pool.get_conn() {
                                            Ok(mut conn) => {
                                                match exec_mysql(&mut conn, sql, &[]) {
                                                    Ok((text, is_select)) => {
                                                        if is_select {
                                                            println!();
                                                            write_output(output_file, &text);
                                                            println!("  {} in {:.2?}", "---".dimmed(), start.elapsed());
                                                        } else {
                                                            println!("  {} in {:.2?}", "+".green(), start.elapsed());
                                                        }
                                                    }
                                                    Err(e) => println!("  {} {}", "x".red().bold(), e.to_string().red()),
                                                }
                                            }
                                            Err(e) => println!("  {} {}", "x".red().bold(), e.to_string().red()),
                                        }
                                    }
                                    None => println!("  {} No database connected.", "x".red().bold()),
                                }
                            }
                        }
                        Err(e) => println!("\r  {} {}", "x".red().bold(), e.red()),
                    }
                }
                "analyze" | "a" => {
                    if ai_state.api_key.is_none() {
                        println!("  {} AI not connected. Use {} first.", "x".red().bold(), ".ai connect".cyan());
                        return false;
                    }
                    let last_result = ai_state.history.last()
                        .and_then(|m| m.content.as_ref())
                        .cloned()
                        .unwrap_or_default();
                    let schema = ai_get_schema(db_type);
                    let system_prompt = format!(
                        "You are a database analyst. Analyze the query result and provide insights.\n\
                         Schema:\n{}\n\
                         Provide: 1) Key findings 2) Data patterns 3) Recommendations",
                        schema
                    );
                    let user_content = if last_result.is_empty() {
                        "Analyze the database structure and provide recommendations.".to_string()
                    } else {
                        format!("Analyze this query result:\n{}", last_result)
                    };
                    let messages = vec![
                        AiMessage { role: "system".to_string(), content: Some(system_prompt), tool_calls: None, tool_call_id: None },
                        AiMessage { role: "user".to_string(), content: Some(user_content), tool_calls: None, tool_call_id: None },
                    ];
                    print!("  {} Analyzing...", "->".dimmed());
                    match ai_sync_call(ai_state, &messages) {
                        Ok(analysis) => {
                            println!("\r  {} {}", "Analysis:".green().bold(), analysis);
                        }
                        Err(e) => println!("\r  {} {}", "x".red().bold(), e.red()),
                    }
                }
                "diagnose" | "d" => {
                    if ai_state.api_key.is_none() {
                        println!("  {} AI not connected. Use {} first.", "x".red().bold(), ".ai connect".cyan());
                        return false;
                    }
                    let schema = ai_get_schema(db_type);
                    let db_kind = match db_type {
                        Some(DbType::Sqlite(_)) => "SQLite",
                        Some(DbType::Mysql(_)) => "MySQL",
                        None => "Unknown",
                    };
                    let system_prompt = format!(
                        "You are a database expert. Diagnose the database schema and identify issues.\n\
                         Database type: {}\n\
                         Schema:\n{}\n\
                         Provide: 1) Schema issues 2) Missing indexes 3) Optimization suggestions 4) Security concerns",
                        db_kind, schema
                    );
                    let messages = vec![
                        AiMessage { role: "system".to_string(), content: Some(system_prompt), tool_calls: None, tool_call_id: None },
                        AiMessage { role: "user".to_string(), content: Some("Diagnose this database schema.".to_string()), tool_calls: None, tool_call_id: None },
                    ];
                    print!("  {} Diagnosing...", "->".dimmed());
                    match ai_sync_call(ai_state, &messages) {
                        Ok(diagnosis) => {
                            println!("\r  {} {}", "Diagnosis:".green().bold(), diagnosis);
                        }
                        Err(e) => println!("\r  {} {}", "x".red().bold(), e.red()),
                    }
                }
                "optimize" | "o" => {
                    if ai_state.api_key.is_none() {
                        println!("  {} AI not connected. Use {} first.", "x".red().bold(), ".ai connect".cyan());
                        return false;
                    }
                    let sql = parts.get(2).map(|s| s.trim()).unwrap_or("");
                    if sql.is_empty() {
                        println!("  {} Usage: {} <SQL>", "!".yellow(), ".ai optimize".cyan());
                        return false;
                    }
                    let schema = ai_get_schema(db_type);
                    let system_prompt = format!(
                        "You are a SQL optimization expert. Optimize the given SQL query.\n\
                         Schema:\n{}\n\
                         Provide: 1) Optimized SQL 2) Explanation of changes 3) Performance tips",
                        schema
                    );
                    let messages = vec![
                        AiMessage { role: "system".to_string(), content: Some(system_prompt), tool_calls: None, tool_call_id: None },
                        AiMessage { role: "user".to_string(), content: Some(format!("Optimize this SQL:\n{}", sql)), tool_calls: None, tool_call_id: None },
                    ];
                    print!("  {} Optimizing...", "->".dimmed());
                    match ai_sync_call(ai_state, &messages) {
                        Ok(optimized) => {
                            println!("\r  {} {}", "Optimized:".green().bold(), optimized);
                        }
                        Err(e) => println!("\r  {} {}", "x".red().bold(), e.red()),
                    }
                }
                "explain" | "e" => {
                    if ai_state.api_key.is_none() {
                        println!("  {} AI not connected. Use {} first.", "x".red().bold(), ".ai connect".cyan());
                        return false;
                    }
                    let sql = parts.get(2).map(|s| s.trim()).unwrap_or("");
                    if sql.is_empty() {
                        println!("  {} Usage: {} <SQL>", "!".yellow(), ".ai explain".cyan());
                        return false;
                    }
                    let system_prompt = "You are a SQL expert. Explain the SQL query line by line in detail.\n\
                        Provide: 1) Purpose of each clause 2) Execution flow 3) Performance notes";
                    let messages = vec![
                        AiMessage { role: "system".to_string(), content: Some(system_prompt.to_string()), tool_calls: None, tool_call_id: None },
                        AiMessage { role: "user".to_string(), content: Some(format!("Explain this SQL:\n{}", sql)), tool_calls: None, tool_call_id: None },
                    ];
                    print!("  {} Explaining...", "->".dimmed());
                    match ai_sync_call(ai_state, &messages) {
                        Ok(explanation) => {
                            println!("\r  {} {}", "Explanation:".green().bold(), explanation);
                        }
                        Err(e) => println!("\r  {} {}", "x".red().bold(), e.red()),
                    }
                }
                "report" | "r" => {
                    if ai_state.api_key.is_none() {
                        println!("  {} AI not connected. Use {} first.", "x".red().bold(), ".ai connect".cyan());
                        return false;
                    }
                    let sql = parts.get(2).map(|s| s.trim()).unwrap_or("");
                    if sql.is_empty() {
                        println!("  {} Usage: {} <SQL>", "!".yellow(), ".ai report".cyan());
                        return false;
                    }
                    let schema = ai_get_schema(db_type);
                    let system_prompt = format!(
                        "You are a report generator. Generate a Markdown report from the SQL query.\n\
                         Schema:\n{}\n\
                         Format: Title, Summary, Data Table, Analysis, Recommendations",
                        schema
                    );
                    let messages = vec![
                        AiMessage { role: "system".to_string(), content: Some(system_prompt), tool_calls: None, tool_call_id: None },
                        AiMessage { role: "user".to_string(), content: Some(format!("Generate report for SQL:\n{}", sql)), tool_calls: None, tool_call_id: None },
                    ];
                    print!("  {} Generating...", "->".dimmed());
                    match ai_sync_call(ai_state, &messages) {
                        Ok(report) => {
                            println!("\r  {} {}", "Report:".green().bold(), report);
                        }
                        Err(e) => println!("\r  {} {}", "x".red().bold(), e.red()),
                    }
                }
                "schema" => {
                    if ai_state.api_key.is_none() {
                        println!("  {} AI not connected. Use {} first.", "x".red().bold(), ".ai connect".cyan());
                        return false;
                    }
                    let desc = parts.get(2).map(|s| s.trim()).unwrap_or("");
                    if desc.is_empty() {
                        println!("  {} Usage: {} <description>", "!".yellow(), ".ai schema".cyan());
                        return false;
                    }
                    let db_kind = match db_type {
                        Some(DbType::Sqlite(_)) => "SQLite",
                        Some(DbType::Mysql(_)) => "MySQL",
                        None => "SQLite",
                    };
                    let system_prompt = format!(
                        "You are a database schema designer. Generate CREATE TABLE statements.\n\
                         Database type: {}\n\
                         Return ONLY the SQL statements, no explanation.",
                        db_kind
                    );
                    let messages = vec![
                        AiMessage { role: "system".to_string(), content: Some(system_prompt), tool_calls: None, tool_call_id: None },
                        AiMessage { role: "user".to_string(), content: Some(desc.to_string()), tool_calls: None, tool_call_id: None },
                    ];
                    print!("  {} Generating...", "->".dimmed());
                    match ai_sync_call(ai_state, &messages) {
                        Ok(schema_sql) => {
                            println!("\r  {} {}", "Schema:".green().bold(), schema_sql.cyan());
                            println!();
                            println!("  {} Execute this SQL? [y/N]: ", "?".yellow());
                            let mut confirm = String::new();
                            let _ = std::io::stdin().read_line(&mut confirm);
                            if confirm.trim().eq_ignore_ascii_case("y") {
                                match db_type {
                                    Some(DbType::Sqlite(conn)) => {
                                        match conn.execute_batch(&schema_sql) {
                                            Ok(_) => println!("  {} Schema created successfully.", "+".green()),
                                            Err(e) => println!("  {} {}", "x".red().bold(), e.to_string().red()),
                                        }
                                    }
                                    Some(DbType::Mysql(pool)) => {
                                        match pool.get_conn() {
                                            Ok(mut conn) => {
                                                match conn.query_drop(&schema_sql) {
                                                    Ok(_) => println!("  {} Schema created successfully.", "+".green()),
                                                    Err(e) => println!("  {} {}", "x".red().bold(), e.to_string().red()),
                                                }
                                            }
                                            Err(e) => println!("  {} {}", "x".red().bold(), e.to_string().red()),
                                        }
                                    }
                                    None => println!("  {} No database connected.", "x".red().bold()),
                                }
                            }
                        }
                        Err(e) => println!("\r  {} {}", "x".red().bold(), e.red()),
                    }
                }
                "chat" => {
                    if ai_state.api_key.is_none() {
                        println!("  {} AI not connected. Use {} first.", "x".red().bold(), ".ai connect".cyan());
                        return false;
                    }
                    let target = parts.get(2).map(|s| s.trim()).unwrap_or("assistant");
                    ai_state.chat_active = true;
                    ai_state.chat_target = target.to_string();
                    let system_prompt = "You are a database assistant. Help users with SQL, analysis, and database operations. Be concise and helpful.".to_string();
                    ai_state.history.clear();
                    ai_state.history.push(AiMessage { role: "system".to_string(), content: Some(system_prompt), tool_calls: None, tool_call_id: None });
                    println!("  {} Entered AI chat mode with {}. Type {} to exit.", "+".green(), target.cyan().bold(), "!exit".cyan());
                }
                _ => {
                    println!("  {} Unknown AI subcommand: {}", "!".yellow(), sub.cyan());
                    println!("  {}", ".ai connect | sql | analyze | diagnose | optimize | explain | report | schema | chat | config".dimmed());
                }
            }
        }
        ".tables" | ".t" => {
            let r = match db_type {
                Some(DbType::Sqlite(conn)) => sqlite_tables(conn),
                Some(DbType::Mysql(pool)) => pool
                    .get_conn()
                    .map_err(|e| e.to_string())
                    .and_then(|mut c| mysql_tables(&mut c)),
                None => {
                    println!(
                        "  {} No connection. Use {} first.",
                        "x".red().bold(),
                        ".connect".cyan()
                    );
                    return false;
                }
            };
            match r {
                Ok(text) => {
                    println!();
                    write_output(output_file, &text);
                    println!();
                }
                Err(e) => println!("  {} {}", "x".red().bold(), e.to_string().red()),
            }
        }
        ".schema" | ".s" => {
            let table = parts.get(1).map(|s| s.trim()).unwrap_or("");
            if table.is_empty() {
                println!(
                    "  {} Usage: {} <table_name>",
                    "!".yellow(),
                    ".schema".cyan()
                );
                return false;
            }
            let r = match db_type {
                Some(DbType::Sqlite(conn)) => sqlite_schema(conn, table),
                Some(DbType::Mysql(pool)) => pool
                    .get_conn()
                    .map_err(|e| e.to_string())
                    .and_then(|mut c| mysql_schema(&mut c, table)),
                None => {
                    println!(
                        "  {} No connection. Use {} first.",
                        "x".red().bold(),
                        ".connect".cyan()
                    );
                    return false;
                }
            };
            match r {
                Ok(text) => {
                    println!();
                    write_output(output_file, &format!("{}\n", text));
                    println!();
                }
                Err(e) => println!("  {} {}", "x".red().bold(), e.to_string().red()),
            }
        }
        _ => {
            println!(
                "  {} Unknown command: {} (try {})",
                "!".yellow(),
                parts[0].cyan(),
                ".help".cyan()
            );
        }
    }
    false
}

// ─── Interactive Mode ──────────────────────────────────────────────

fn run_interactive(initial: Option<DbType>, initial_agent: Option<AgentState>) {
    let mut rl: rustyline::Editor<DbHelper, rustyline::history::DefaultHistory> =
        match rustyline::Editor::new() {
            Ok(editor) => editor,
            Err(e) => { eprintln!("Failed to initialize line editor: {}. Running in basic mode.", e); return; }
        };
    let hist_path = dirs_cache_file();
    let _ = rl.load_history(&hist_path);

    let mut db_type = initial;
    let mut output_file: Option<PathBuf> = None;
    let mut mqtt_state = MqttState {
        client: None,
        pending: Arc::new(Mutex::new(HashMap::new())),
        broker_url: String::new(),
        client_id: String::new(),
        mqtt_user: String::new(),
        mqtt_pass: String::new(),
        mqtt_tls: false,
        _runtime: None,
        chat_enabled: false,
        chat_queue: Arc::new(Mutex::new(Vec::new())),
    };
    let mut agent_state = initial_agent.unwrap_or(AgentState {
        running: false,
        agent_id: String::new(),
        stop_tx: None,
    });
    let mut remote_agent: Option<String> = None;
    let mut chat_target: Option<String> = None;
    let mut ai_state = AiState {
        api_key: None,
        base_url: "https://apihub.agnes-ai.com/v1".to_string(),
        model: "agnes-2.0-flash".to_string(),
        history: Vec::new(),
        chat_active: false,
        chat_target: String::new(),
        tavily_key: None,
        search_enabled: true,
    };

    // Create helper with initial plain prompt
    let helper = DbHelper {};
    rl.set_helper(Some(helper));

    print_banner();
    println!();

    match &db_type {
        Some(db) => {
            println!(
                "  {} {} ({})",
                "+".green().bold(),
                db.label().cyan().bold(),
                db.kind().dimmed()
            );
        }
        None => {
            println!("  {} No connection established.", "!".yellow());
            println!(
                "  {} Type {} to get started.",
                "->".dimmed(),
                ".help".cyan()
            );
        }
    }
    println!();

    loop {
        // Check for incoming chat messages
        if mqtt_state.chat_enabled {
            let msgs: Vec<ChatMsg> = mqtt_state.chat_queue.lock().unwrap_or_else(|e| e.into_inner()).drain(..).collect();
            for m in &msgs {
                println!("  {} [{}] {}", "<".green(), m.from.cyan(), m.msg);
            }
        }

        let ai_chat_prompt = if ai_state.chat_active { Some(ai_state.chat_target.clone()) } else { None };
        let prompt = make_plain_prompt(&output_file, &chat_target, &ai_chat_prompt);
        let line = match rl.readline(&prompt) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                println!("(Ctrl-C)");
                continue;
            }
            Err(ReadlineError::Eof) => {
                if agent_state.running {
                    if let Some(tx) = agent_state.stop_tx.take() {
                        let _ = tx.send(());
                    }
                    agent_state.running = false;
                    println!("  {} Stopped agent '{}'.", "-".dimmed(), agent_state.agent_id.cyan());
                }
                if mqtt_state.client.is_some() {
                    mqtt_disconnect(&mut mqtt_state);
                    println!("  {} Disconnected from MQTT broker.", "-".dimmed());
                }
                println!();
                println!("  {}", "Thank you for using dbcli. Goodbye!".dimmed());
                println!();
                break;
            }
            Err(e) => {
                println!("  {} {}", "x".red().bold(), e);
                break;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let _ = rl.add_history_entry(trimmed);

        // AI Chat mode handling
        if ai_state.chat_active {
            if let Some(rest) = trimmed.strip_prefix('!') {
                let cmd_parts: Vec<&str> = rest.splitn(2, ' ').collect();
                match cmd_parts[0] {
                    "exit" | "back" | "quit" => {
                        ai_state.chat_active = false;
                        ai_state.chat_target.clear();
                        ai_state.history.clear();
                        println!("  {} Exited AI chat mode.", "+".green());
                        continue;
                    }
                    "help" | "h" => {
                        println!("  {}:", "AI Chat Mode".bold());
                        println!("    {}   Exit AI chat mode", "!exit".cyan());
                        println!("    {}     Show this help", "!help".cyan());
                        println!("    {}   Send a message (just type)", "text".cyan());
                        continue;
                    }
                    _ => {
                        println!("  {} Unknown command: {} (try !help)", "!".yellow(), cmd_parts[0].cyan());
                        continue;
                    }
                }
            } else {
                if ai_state.history.len() == 1 {
                    let schema = ai_get_schema(&db_type);
                    let db_kind = match db_type {
                        Some(DbType::Sqlite(_)) => "SQLite",
                        Some(DbType::Mysql(_)) => "MySQL",
                        None => "Unknown",
                    };
                    let schema_msg = format!("Connected database type: {}. Schema:\n{}", db_kind, schema);
                    // Insert schema as a system message (not fake user/assistant messages)
                    ai_state.history.insert(1, AiMessage { role: "system".to_string(), content: Some(schema_msg), tool_calls: None, tool_call_id: None });
                }
                ai_state.history.push(AiMessage { role: "user".to_string(), content: Some(trimmed.to_string()), tool_calls: None, tool_call_id: None });
                println!();
                print!("  {} Thinking...", "...".yellow());
                let _ = std::io::stdout().flush();
                match ai_sync_call(&ai_state, &ai_state.history.clone()) {
                    Ok(reply) => {
                        println!("\r  {} {}", "AI:".green().bold(), reply);
                        ai_state.history.push(AiMessage { role: "assistant".to_string(), content: Some(reply.clone()), tool_calls: None, tool_call_id: None });
                        // Auto-execute SQL if found in AI response
                        if let Some(sql) = extract_sql_from_ai_response(&reply) {
                            println!();
                            println!("  {} {}", ">>".cyan(), sql.cyan());
                            println!();
                            let _ = exec_on_db(&db_type, &sql, &output_file);
                        }
                    }
                    Err(e) => println!("\r  {} {}", "x".red().bold(), e.to_string().red()),
                }
                continue;
            }
        }

        // Chat mode handling (MQTT)
        if let Some(ref target) = chat_target.clone() {
            if let Some(rest) = trimmed.strip_prefix('!') {
                let cmd_parts: Vec<&str> = rest.splitn(2, ' ').collect();
                match cmd_parts[0] {
                    "exit" | "back" | "quit" => {
                        chat_target = None;
                        println!("  {} Exited chat mode.", "+".green());
                        continue;
                    }
                    "sql" | "q" => {
                        if cmd_parts.len() < 2 || cmd_parts[1].trim().is_empty() {
                            println!("  {} Usage: !sql <query>", "!".yellow());
                            continue;
                        }
                        let sql = cmd_parts[1].trim();
                        if mqtt_state.client.is_some() {
                            println!("  {} [remote:{}] {}", "->".dimmed(), target.cyan(), sql.dimmed());
                            let start = Instant::now();
                            match mqtt_exec_remote(&mut mqtt_state, target, "sqlite", sql, &[]) {
                                Ok(resp) => {
                                    if resp.ok {
                                        if let Some(rows) = &resp.rows {
                                            let json_str = serde_json::to_string_pretty(rows).unwrap_or_default();
                                            println!();
                                            println!("{}", json_str);
                                            println!();
                                            println!("  {} {} in {:.2?}",
                                                "---".dimmed(),
                                                format!("{} row(s)", rows.len()).dimmed(),
                                                start.elapsed());
                                        } else if let Some(affected) = &resp.affected {
                                            println!("  {} {} in {:.2?}",
                                                "+".green(),
                                                format!("{} row(s) affected", affected).green(),
                                                start.elapsed());
                                        }
                                    } else {
                                        println!("  {} {}", "x".red().bold(),
                                            resp.error.unwrap_or_else(|| "Unknown error".to_string()).red());
                                    }
                                }
                                Err(e) => println!("  {} {}", "x".red().bold(), e.to_string().red()),
                            }
                        } else {
                            println!("  {} MQTT not connected.", "x".red().bold());
                        }
                        continue;
                    }
                    "help" | "h" => {
                        println!("  {}:", "Chat Mode".bold());
                        println!("    {}   Exit chat mode", "!exit".cyan());
                        println!("    {}     Execute SQL remotely", "!sql <query>".cyan());
                        println!("    {}     Show this help", "!help".cyan());
                        println!("    {}   Send a message (just type)", "text".cyan());
                        continue;
                    }
                    _ => {
                        println!("  {} Unknown command: {} (try !help)", "!".yellow(), cmd_parts[0].cyan());
                        continue;
                    }
                }
            } else {
                // Send as chat message
                if let Some(ref client) = mqtt_state.client {
                    let chat_msg = ChatMsg {
                        from: mqtt_state.client_id.clone(),
                        to: target.clone(),
                        msg: trimmed.to_string(),
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default().as_secs(),
                    };
                    let topic = format!("dbcli/chat/{}", target);
                    let rt = match mqtt_state._runtime.as_ref() {
                        Some(rt) => rt,
                        None => { println!("  {} No MQTT runtime.", "x".red().bold()); continue; }
                    };
                    let payload = match serde_json::to_vec(&chat_msg) {
                        Ok(p) => p,
                        Err(e) => { println!("  {} Serialization error: {}", "x".red().bold(), e); continue; }
                    };
                    rt.block_on(async {
                        let _ = client.publish(topic, QoS::AtMostOnce, false, payload).await;
                    });
                    println!("  {} {}", "->".dimmed(), trimmed.dimmed());
                } else {
                    println!("  {} MQTT not connected.", "x".red().bold());
                }
                continue;
            }
        }

        if trimmed.starts_with('.') {
            if handle_dot_command(trimmed, &mut db_type, &mut output_file, &mut mqtt_state, &mut agent_state, &mut remote_agent, &mut chat_target, &mut ai_state) {
                break;
            }
            continue;
        }

        let mut full_sql = trimmed.to_string();
        if !full_sql.ends_with(';') {
            loop {
                let prompt2 = "       ...>";
                match rl.readline(prompt2) {
                    Ok(next_line) => {
                        let next_trimmed = next_line.trim();
                        if next_trimmed.is_empty() {
                            break;
                        }
                        full_sql.push(' ');
                        full_sql.push_str(next_trimmed);
                        if next_trimmed.ends_with(';') {
                            break;
                        }
                    }
                    Err(ReadlineError::Interrupted) => {
                        full_sql.clear();
                        break;
                    }
                    Err(ReadlineError::Eof) => {
                        full_sql.clear();
                        break;
                    }
                    Err(_) => {
                        full_sql.clear();
                        break;
                    }
                }
            }
        }

        if full_sql.is_empty() {
            continue;
        }

        let query = full_sql.trim_end_matches(';').trim();

        // Check if remote mode is active
        if let Some(ref agent_id) = remote_agent {
            if mqtt_state.client.is_some() {
                println!("  {} [remote:{}] {}", "->".dimmed(), agent_id.cyan(), query.dimmed());
                let start = Instant::now();
                match mqtt_exec_remote(&mut mqtt_state, agent_id, "sqlite", query, &[]) {
                    Ok(resp) => {
                        if resp.ok {
                            if let Some(rows) = &resp.rows {
                                let json_str = serde_json::to_string_pretty(rows).unwrap_or_default();
                                println!();
                                write_output(&output_file, &json_str);
                                println!();
                                println!("  {} {} in {:.2?}",
                                    "---".dimmed(),
                                    format!("{} row(s)", rows.len()).dimmed(),
                                    start.elapsed());
                            } else if let Some(affected) = &resp.affected {
                                println!("  {} {} in {:.2?}",
                                    "+".green(),
                                    format!("{} row(s) affected", affected).green(),
                                    start.elapsed());
                            }
                        } else {
                            println!("  {} {}", "x".red().bold(),
                                resp.error.unwrap_or_else(|| "Unknown error".to_string()).red());
                        }
                    }
                    Err(e) => {
                        println!("  {} {}", "x".red().bold(), e.red());
                    }
                }
            } else {
                println!("  {} MQTT not connected. Use .mqtt connect first.", "x".red().bold());
            }
        } else {
            let _ = exec_on_db(&db_type, query, &output_file);
        }
    }

    let _ = rl.save_history(&hist_path);
}

// ─── Utils ─────────────────────────────────────────────────────────

fn dirs_cache_file() -> String {
    let dir = dirs2_cache_dir().unwrap_or_else(|| PathBuf::from("."));
    let _ = std::fs::create_dir_all(&dir);
    dir.join("dbcli_history")
        .to_string_lossy()
        .to_string()
}

fn dirs2_cache_dir() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        std::env::var("APPDATA")
            .ok()
            .map(|d| PathBuf::from(d).join("dbcli"))
    }
    #[cfg(not(target_os = "windows"))]
    {
        std::env::var("HOME")
            .ok()
            .map(|d| PathBuf::from(d).join(".cache").join("dbcli"))
    }
}

fn start_agent_background(
    broker_url: &str,
    agent_id: &str,
    user: &str,
    pass: &str,
    tls: bool,
    database: &str,
    db_type_str: &str,
) -> Option<AgentState> {
    // Connect to database if provided
    let db_conn = if !database.is_empty() {
        let result = match db_type_str {
            "sqlite" | "sql" => connect_sqlite(database),
            "mysql" | "my" => connect_mysql(database),
            _ => {
                eprintln!("  {} Unknown db type: {}", "x".red().bold(), db_type_str);
                return None;
            }
        };
        match result {
            Ok(db) => {
                println!("  {} {} ({})", "+".green().bold(), db.label().cyan().bold(), db.kind().dimmed());
                Some(db)
            }
            Err(e) => {
                eprintln!("  {} {}", "x".red().bold(), e);
                return None;
            }
        }
    } else {
        println!("  {} No database specified. Use --database <path>.", "!".yellow());
        None
    };

    let broker_host = broker_url
        .replace("mqtt://", "").replace("mqtts://", "")
        .trim_end_matches('/').to_string();
    let user = user.to_string();
    let pass = pass.to_string();
    let agent_id = agent_id.to_string();
    let agent_id_for_state = agent_id.clone();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => { eprintln!("  Agent runtime error: {}", e); return; }
        };
        rt.block_on(async move {
            let client_id = format!("agent-{}-{}", agent_id, &Uuid::new_v4().to_string()[..8]);
            let default_port = if tls { 8883 } else { 1883 };
            let (host, port) = parse_broker_host_port(&broker_host, default_port);

            let mut opts = MqttOptions::new(&client_id, &host, port);
            opts.set_credentials(&user, &pass);
            opts.set_keep_alive(std::time::Duration::from_secs(30));
            if tls {
                opts.set_transport(rumqttc::Transport::Tls(
                    rumqttc::TlsConfiguration::default(),
                ));
            }

            let (client, mut event_loop) = AsyncClient::new(opts, 100);
            let topic = format!("dbcli/cmd/{}", agent_id);

            // Wait for connack BEFORE subscribing (MQTT protocol)
            match tokio::time::timeout(std::time::Duration::from_secs(5), event_loop.poll()).await {
                Ok(Ok(Event::Incoming(rumqttc::Packet::ConnAck(_)))) => {}
                _ => {
                    eprintln!("  {} Agent failed to connect to broker", "x".red().bold());
                    return;
                }
            }

            // Subscribe after ConnAck
            if let Err(e) = client.subscribe(&topic, QoS::AtLeastOnce).await {
                eprintln!("  {} Agent subscribe error: {}", "x".red().bold(), e);
                return;
            }
            println!("  {} Agent '{}' connected and listening on {}",
                "+".green().bold(), agent_id.cyan(), topic.dimmed());

            let local_db = Arc::new(Mutex::new(db_conn));

            // Heartbeat
            let hb_client = client.clone();
            let hb_id = agent_id.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
                loop {
                    interval.tick().await;
                    let payload = json!({
                        "agent_id": hb_id, "status": "online",
                        "timestamp": std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default().as_secs()
                    });
                    let _ = hb_client
                        .publish(format!("dbcli/heartbeat/{}", hb_id), QoS::AtMostOnce, false,
                            serde_json::to_vec(&payload).unwrap_or_default())
                        .await;
                }
            });

            // Event loop
            loop {
                if stop_rx.try_recv().is_ok() {
                    let _ = client.disconnect().await;
                    break;
                }
                match tokio::time::timeout(std::time::Duration::from_millis(100), event_loop.poll()).await {
                    Ok(Ok(Event::Incoming(rumqttc::Packet::Publish(p)))) => {
                        if let Ok(cmd) = serde_json::from_slice::<MqttCmd>(&p.payload) {
                            let resp = {
                                let db_lock = local_db.lock().unwrap_or_else(|e| e.into_inner());
                                match db_lock.as_ref() {
                                    Some(DbType::Sqlite(conn)) => {
                                        match exec_sqlite(conn, &cmd.sql, &cmd.params) {
                                            Ok((text, is_select)) => {
                                                if is_select {
                                                    let rows: Vec<Value> = serde_json::from_str(&text).unwrap_or_default();
                                                    let columns = rows.first().and_then(|r| r.as_object()).map(|m| m.keys().cloned().collect()).unwrap_or_default();
                                                    MqttResp { request_id: cmd.request_id, ok: true, columns: Some(columns), rows: Some(rows), affected: None, error: None }
                                                } else {
                                                    let affected: u64 = text.parse().unwrap_or(0);
                                                    MqttResp { request_id: cmd.request_id, ok: true, columns: None, rows: None, affected: Some(affected), error: None }
                                                }
                                            }
                                            Err(e) => MqttResp { request_id: cmd.request_id, ok: false, columns: None, rows: None, affected: None, error: Some(e) },
                                        }
                                    }
                                    Some(DbType::Mysql(pool)) => {
                                        match pool.get_conn() {
                                            Ok(mut conn) => {
                                                match exec_mysql(&mut conn, &cmd.sql, &cmd.params) {
                                                    Ok((text, is_select)) => {
                                                        if is_select {
                                                            let rows: Vec<Value> = serde_json::from_str(&text).unwrap_or_default();
                                                            let columns = rows.first().and_then(|r| r.as_object()).map(|m| m.keys().cloned().collect()).unwrap_or_default();
                                                            MqttResp { request_id: cmd.request_id, ok: true, columns: Some(columns), rows: Some(rows), affected: None, error: None }
                                                        } else {
                                                            let affected: u64 = text.parse().unwrap_or(0);
                                                            MqttResp { request_id: cmd.request_id, ok: true, columns: None, rows: None, affected: Some(affected), error: None }
                                                        }
                                                    }
                                                    Err(e) => MqttResp { request_id: cmd.request_id, ok: false, columns: None, rows: None, affected: None, error: Some(e) },
                                                }
                                            }
                                            Err(e) => MqttResp { request_id: cmd.request_id, ok: false, columns: None, rows: None, affected: None, error: Some(e.to_string()) },
                                        }
                                    }
                                    None => MqttResp { request_id: cmd.request_id, ok: false, columns: None, rows: None, affected: None, error: Some("No database connected".to_string()) },
                                }
                            };
                            let resp_topic = format!("dbcli/resp/{}", resp.request_id);
                            if let Ok(payload) = serde_json::to_vec(&resp) {
                                let _ = client.publish(resp_topic, QoS::AtLeastOnce, false, payload).await;
                            }
                        }
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(_)) => {}
                    Err(_) => {}
                }
            }
        });
    });

    Some(AgentState {
        running: true,
        agent_id: agent_id_for_state,
        stop_tx: Some(stop_tx),
    })
}

// ─── Main ──────────────────────────────────────────────────────────

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Sqlite {
            database,
            query,
            params,
        }) => {
            let conn = match rusqlite::Connection::open(&database) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("  {} Failed to open database: {}", "x".red().bold(), e);
                    std::process::exit(1);
                }
            };
            match query {
                Some(q) => {
                    let start = Instant::now();
                    match exec_sqlite(&conn, &q, &params) {
                        Ok((text, is_select)) => {
                            if is_select {
                                println!("{}", text);
                            } else {
                                let affected: u64 = text.parse().unwrap_or(0);
                                println!(
                                    "  {} {} row(s) affected in {:.2?}",
                                    "+".green(),
                                    affected,
                                    start.elapsed()
                                );
                            }
                        }
                        Err(e) => {
                            eprintln!("  {} {}", "x".red().bold(), e);
                            std::process::exit(1);
                        }
                    }
                }
                None => run_interactive(Some(DbType::Sqlite(conn)), None),
            }
        }
        Some(Commands::Mysql { url, query, params }) => {
            let pool = match mysql::Pool::new(url.as_str()) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("  {} Failed to connect: {}", "x".red().bold(), e);
                    std::process::exit(1);
                }
            };
            match query {
                Some(q) => {
                    let mut conn = pool.get_conn().unwrap_or_else(|e| {
                        eprintln!("  {} {}", "x".red().bold(), e);
                        std::process::exit(1);
                    });
                    let start = Instant::now();
                    match exec_mysql(&mut conn, &q, &params) {
                        Ok((text, is_select)) => {
                            if is_select {
                                println!("{}", text);
                            } else {
                                let affected: u64 = text.parse().unwrap_or(0);
                                println!(
                                    "  {} {} row(s) affected in {:.2?}",
                                    "+".green(),
                                    affected,
                                    start.elapsed()
                                );
                            }
                        }
                        Err(e) => {
                            eprintln!("  {} {}", "x".red().bold(), e);
                            std::process::exit(1);
                        }
                    }
                }
                None => run_interactive(Some(DbType::Mysql(pool)), None),
            }
        }
        Some(Commands::Agent {
            broker,
            id,
            user,
            password,
            tls,
            database,
            db_type,
        }) => {
            let user_str = user.unwrap_or_default();
            let pass_str = password.unwrap_or_default();
            let db_path = database.unwrap_or_default();
            let agent = start_agent_background(&broker, &id, &user_str, &pass_str, tls, &db_path, &db_type);
            run_interactive(None, agent);
        }
        None => run_interactive(None, None),
    }
}
