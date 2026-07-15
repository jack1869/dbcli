<p align="center">
  <a href="README_EN.md"><img src="https://img.shields.io/badge/English-Read%20Me-blue?style=for-the-badge" alt="English"></a>
</p>

# dbcli

一个功能丰富的命令行数据库工具，支持 SQLite 和 MySQL 读写操作，集成 AI 自然语言查询、MQTT 远程执行和 Web 搜索功能。

## 功能特性

- **SQLite & MySQL** — 一个工具同时管理两种数据库
- **AI 助手** — 自然语言转 SQL，自动执行，数据库分析诊断
- **MQTT 远程执行** — 通过 MQTT Agent 远程执行 SQL
- **Web 搜索** — AI 可自动搜索网络获取实时信息
- **SQL 语法高亮** — 实时彩色显示 SQL 输入
- **聊天通信** — 通过 MQTT 实现客户端/Agent 间消息通信
- **多行 SQL** — 支持复杂查询的多行输入
- **持久化历史** — 命令历史跨会话保存

## 快速开始

```bash
# 交互模式
dbcli

# 连接 SQLite
dbcli sqlite -d mydb.db

# 连接 MySQL
dbcli mysql -u "mysql://root:pass@localhost:3306/mydb"

# 执行 SQL 后退出
dbcli sqlite -d mydb.db -q "SELECT * FROM users LIMIT 10"
```

## 命令参考

### 连接管理

| 命令 | 说明 |
|------|------|
| `.connect sqlite <file>` | 连接 SQLite 数据库 |
| `.connect mysql <url>` | 连接 MySQL 数据库 |
| `.disconnect` | 断开当前数据库 |
| `.status` | 显示连接状态 |
| `.tables` / `.t` | 列出所有表 |
| `.schema <table>` / `.s` | 显示建表语句 |

### 输出控制

| 命令 | 说明 |
|------|------|
| `.output <file>` / `.o` | 输出重定向到文件 |
| `.output off` | 停止文件重定向 |
| `.help` / `.h` | 显示帮助 |
| `.clear` / `.cls` | 清屏 |
| `.quit` / `.exit` | 退出 dbcli |

### SQL 执行

- 直接在提示符输入 SQL
- 多行模式：不以 `;` 结尾的行进入续行模式（`...>`）
- 空行可直接执行，无需 `;`
- 结果以格式化 JSON 显示，附带行数和耗时

```
dbcli>>SELECT * FROM users WHERE age > 25;

  [{"id": 1, "name": "Alice", "age": 30},
   {"id": 2, "name": "Bob", "age": 28}]

  --- 2 row(s) in 0.01s
```

## AI 助手

### 配置

```bash
dbcli
.ai connect <your-api-key>
```

默认使用 Agnes AI（`https://apihub.agnes-ai.com/v1`，模型 `agnes-2.0-flash`）。

### 命令

| 命令 | 说明 |
|------|------|
| `.ai connect <key>` | 设置 API 密钥 |
| `.ai config` | 显示 AI 配置 |
| `.ai sql <description>` | 自然语言转 SQL（自动执行） |
| `.ai analyze` | 分析数据库结构 |
| `.ai diagnose` | 诊断数据库问题 |
| `.ai optimize <sql>` | SQL 优化建议 |
| `.ai explain <sql>` | SQL 逐行解释 |
| `.ai report <sql>` | 生成分析报告 |
| `.ai schema <desc>` | 根据描述生成建表语句 |
| `.ai chat` | 进入 AI 聊天模式 |

### AI 聊天示例

```
[AI:assistant]查询今日订单总数

  AI: ```sql
  SELECT COUNT(*) AS total FROM orders WHERE DATE(created_at) = CURDATE();
  ```

  >> SELECT COUNT(*) AS total FROM orders WHERE DATE(created_at) = CURDATE();

  [{"total": 42}]

  --- 1 row(s) in 0.02s
```

- AI 响应中的 SQL 代码块会自动执行
- 保持完整对话历史
- 数据库 Schema 自动注入上下文
- 输入 `!exit` 退出聊天

### Web 搜索

```bash
.ai search on              # 启用 Web 搜索
.ai search tavily <key>    # 使用 Tavily 获取更好结果（可选）
```

启用后，AI 在对话中可自动搜索网络。

## MQTT 远程执行

### 配置

```bash
# 在远程机器启动 Agent
dbcli agent --broker mqtt://broker:1883 --id myagent -d /path/to/db.db

# 或从交互模式启动
.mqtt agent start myagent mqtt://broker:1883 -d /path/to/db.db
```

### 客户端命令

| 命令 | 说明 |
|------|------|
| `.mqtt connect <broker>` | 连接 MQTT Broker |
| `.mqtt disconnect` | 断开连接 |
| `.mqtt status` | 显示 MQTT 状态 |
| `.mqtt use <agent_id>` | 进入远程执行模式 |
| `.mqtt use local` | 切换回本地模式 |
| `.mqtt exec <agent> "<sql>"` | 在远程 Agent 执行 SQL |
| `.mqtt agents` | 显示已知 Agent |

### 聊天

```bash
.mqtt chat on                     # 启用聊天显示
.mqtt chat with <agent_id>        # 进入聊天模式
.mqtt chat send <id> <message>    # 发送单条消息
.mqtt chat broadcast <message>    # 广播消息
```

### Agent 模式（CLI）

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

## MQTT 协议

### 主题

| 主题 | 方向 | 用途 |
|------|------|------|
| `dbcli/cmd/<agent_id>` | 客户端 → Agent | SQL 命令 |
| `dbcli/resp/<request_id>` | Agent → 客户端 | 查询结果 |
| `dbcli/chat/<client_id>` | 双向 | 私聊消息 |
| `dbcli/chat/broadcast` | 双向 | 广播消息 |
| `dbcli/heartbeat/<agent_id>` | Agent → | 心跳状态 |

### Agent 特性

- 收到 MQTT ConnAck 后订阅命令主题
- 每 30 秒发布心跳
- 同时支持 SQLite 和 MySQL
- 30 秒响应超时
- 通过 stop channel 优雅关闭

## 构建

```bash
# 调试构建
cargo build

# 发布构建
cargo build --release

# 运行
cargo run
cargo run -- sqlite -d mydb.db
```

### 依赖

| Crate | 用途 |
|-------|------|
| `rusqlite` | SQLite 驱动 |
| `mysql` | MySQL 驱动 |
| `rumqttc` | MQTT 客户端 |
| `tokio` | 异步运行时 |
| `reqwest` | HTTP 客户端（AI API、Web 搜索） |
| `rustyline` | 带历史的行编辑器 |
| `clap` | CLI 参数解析 |
| `colored` | 终端颜色 |
| `serde_json` | JSON 序列化 |

## 注册与资源

### AI 服务

| 服务 | 网址 | 说明 |
|------|------|------|
| **Agnes AI** | https://platform.agnes-ai.com | AI API 平台，注册获取免费 API 密钥 |
| Agnes AI 文档 | https://wiki.agnes-ai.com/en/docs/quickstart | 开发者文档 |
| Agnes AI GitHub | https://github.com/AgnesAI-Labs/Agnes-AI | 官方 GitHub 仓库 |

### Web 搜索

| 服务 | 网址 | 说明 |
|------|------|------|
| **Tavily** | https://tavily.com | AI 驱动的 Web 搜索 API |
| Tavily API Key | https://app.tavily.com/home | 注册获取免费 API 密钥（每月 1000 次） |

### MQTT Broker

| 服务 | 网址 | 说明 |
|------|------|------|
| **EMQX Cloud** | https://www.emqx.com/en/cloud | 免费 Serverless MQTT Broker（每月 100 万会话分钟） |
| EMQX Cloud 注册 | https://www.emqx.com/en/try?tab=cloud | 免费试用，无需信用卡 |
| MQTT 官网 | https://mqtt.org | MQTT 协议规范与资源 |

## 许可证

MIT
