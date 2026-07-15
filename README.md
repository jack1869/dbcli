# dbcli

A feature-rich CLI tool for reading/writing SQLite and MySQL databases, with AI-powered natural language queries, MQTT remote execution, and web search integration.

## Features

- **SQLite & MySQL** - Connect, query, and manage both databases from one tool
- **AI Assistant** - Natural language to SQL conversion, auto-execute, database analysis
- **MQTT Remote Execution** - Execute SQL on remote databases via MQTT agents
- **Web Search** - AI can search the web for real-time information
- **SQL Syntax Highlighting** - Real-time colorized SQL input
- **Chat Messaging** - Communicate between clients/agents via MQTT
- **Multi-line SQL** - Write complex queries across multiple lines
- **Persistent History** - Command history saved across sessions

## Quick Start

```bash
# Interactive mode
dbcli

# Connect to SQLite
dbcli sqlite -d mydb.db

# Connect to MySQL
dbcli mysql -u "mysql://root:pass@localhost:3306/mydb"

# Execute SQL and exit
dbcli sqlite -d mydb.db -q "SELECT * FROM users LIMIT 10"
```

## Commands

### Connection

| Command | Description |
|---------|-------------|
| `.connect sqlite <file>` | Connect to SQLite database |
| `.connect mysql <url>` | Connect to MySQL database |
| `.disconnect` | Disconnect current database |
| `.status` | Show connection status |
| `.tables` / `.t` | List all tables |
| `.schema <table>` / `.s` | Show CREATE TABLE statement |

### Output

| Command | Description |
|---------|-------------|
| `.output <file>` / `.o` | Redirect output to file |
| `.output off` | Stop file redirect |
| `.help` / `.h` | Show help |
| `.clear` / `.cls` | Clear screen |
| `.quit` / `.exit` | Exit dbcli |

### SQL Execution

- Type SQL directly at the prompt
- Multi-line: lines not ending with `;` enter continuation mode (`...>`)
- Press Enter on empty line to execute without `;`
- Results displayed as formatted JSON with row count and timing

```
dbcli>>SELECT * FROM users WHERE age > 25;

  [{"id": 1, "name": "Alice", "age": 30},
   {"id": 2, "name": "Bob", "age": 28}]

  --- 2 row(s) in 0.01s
```

### AI Assistant

#### Setup

```bash
dbcli
.ai connect <your-api-key>
```

Default: Agnes AI (`https://apihub.agnes-ai.com/v1`, model `agnes-2.0-flash`).

#### Commands

| Command | Description |
|---------|-------------|
| `.ai connect <key>` | Set AI API key |
| `.ai config` | Show AI configuration |
| `.ai sql <description>` | Natural language to SQL (auto-execute) |
| `.ai analyze` | Analyze database structure |
| `.ai diagnose` | Diagnose schema issues |
| `.ai optimize <sql>` | Get SQL optimization suggestions |
| `.ai explain <sql>` | Line-by-line SQL explanation |
| `.ai report <sql>` | Generate analysis report |
| `.ai schema <desc>` | Generate CREATE TABLE from description |
| `.ai chat` | Enter AI chat mode |

#### AI Chat Mode

```
[AI:assistant]查询今日订单总数

  AI: ```sql
  SELECT COUNT(*) AS total FROM orders WHERE DATE(created_at) = CURDATE();
  ```

  >> SELECT COUNT(*) AS total FROM orders WHERE DATE(created_at) = CURDATE();

  [{"total": 42}]

  --- 1 row(s) in 0.02s
```

- AI responses with SQL code blocks are auto-executed
- Full conversation history maintained
- Database schema automatically injected as context
- Type `!exit` to leave chat mode

#### Web Search

```bash
.ai search on              # Enable web search
.ai search tavily <key>    # Use Tavily for better results (optional)
```

AI can automatically search the web during conversations when enabled.

### MQTT Remote Execution

#### Setup

```bash
# Start an agent on remote machine
dbcli agent --broker mqtt://broker:1883 --id myagent -d /path/to/db.db

# Or start agent from interactive mode
.mqtt agent start myagent mqtt://broker:1883 -d /path/to/db.db
```

#### Client Commands

| Command | Description |
|---------|-------------|
| `.mqtt connect <broker>` | Connect to MQTT broker |
| `.mqtt disconnect` | Disconnect from broker |
| `.mqtt status` | Show MQTT status |
| `.mqtt use <agent_id>` | Enter remote execution mode |
| `.mqtt use local` | Switch back to local mode |
| `.mqtt exec <agent> "<sql>"` | Execute SQL on remote agent |
| `.mqtt agents` | Show known agents |

#### Chat

```bash
.mqtt chat on                     # Enable chat display
.mqtt chat with <agent_id>        # Enter chat mode
.mqtt chat send <id> <message>    # Send one-shot message
.mqtt chat broadcast <message>    # Broadcast to all
```

### Agent Mode (CLI)

```bash
dbcli agent \
  --broker mqtt://broker:1883 \
  --id myagent \
  --user admin \
  --password secret \
  --tls \
  --database /path/to/db.db \
  --db-type sqlite
```

## AI Features in Detail

### Natural Language to SQL

```bash
dbcli>>.ai sql 查询所有年龄大于25的用户

  >> SELECT * FROM users WHERE age > 25;
  [y/N]?
```

### Database Analysis

```bash
dbcli>>.ai analyze
# Analyzes schema, data patterns, and provides recommendations

dbcli>>.ai diagnose
# Identifies missing indexes, optimization opportunities

dbcli>>.ai optimize SELECT * FROM orders WHERE customer_id = 123
# Suggests indexes, query restructuring
```

### Schema Generation

```bash
dbcli>>.ai schema 创建一个用户表，包含id、name、email、created_at字段
```

## MQTT Protocol

### Topics

| Topic | Direction | Purpose |
|-------|-----------|---------|
| `dbcli/cmd/<agent_id>` | Client → Agent | SQL commands |
| `dbcli/resp/<request_id>` | Agent → Client | Query results |
| `dbcli/chat/<client_id>` | Bidirectional | Direct messages |
| `dbcli/chat/broadcast` | Bidirectional | Broadcast messages |
| `dbcli/heartbeat/<agent_id>` | Agent → | Status heartbeat |

### Agent Features

- Subscribes to command topic after MQTT ConnAck
- Publishes heartbeat every 30 seconds
- Supports both SQLite and MySQL
- 30-second response timeout
- Graceful shutdown via stop channel

## Build

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Run
cargo run
cargo run -- sqlite -d mydb.db
```

### Dependencies

| Crate | Purpose |
|-------|---------|
| `rusqlite` | SQLite driver |
| `mysql` | MySQL driver |
| `rumqttc` | MQTT client |
| `tokio` | Async runtime |
| `reqwest` | HTTP client (AI API, web search) |
| `rustyline` | Line editor with history |
| `clap` | CLI argument parsing |
| `colored` | Terminal colors |
| `serde_json` | JSON serialization |

## Registration & Resources

### AI Service

| Service | URL | Description |
|---------|-----|-------------|
| **Agnes AI** | https://platform.agnes-ai.com | AI API platform, register to get free API key |
| Agnes AI Docs | https://wiki.agnes-ai.com/en/docs/quickstart | Developer documentation |
| Agnes AI GitHub | https://github.com/AgnesAI-Labs/Agnes-AI | Official GitHub repository |

### Web Search

| Service | URL | Description |
|---------|-----|-------------|
| **Tavily** | https://tavily.com | AI-powered web search API |
| Tavily API Key | https://app.tavily.com/home | Register to get free API key (1000 calls/month) |

### MQTT Broker

| Service | URL | Description |
|---------|-----|-------------|
| **EMQX Cloud** | https://www.emqx.com/en/cloud | Free Serverless MQTT broker (1M session min/month) |
| EMQX Cloud Sign Up | https://www.emqx.com/en/try?tab=cloud | Start free trial, no credit card required |
| MQTT Official | https://mqtt.org | MQTT protocol specification and resources |

## License

MIT
