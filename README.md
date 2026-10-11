# API Proxy

基于 Node.js 的反向代理服务，将所有请求转发到目标域名。

## 功能

- 所有请求转发到目标域名，保留原始路径和查询参数
- 支持 WebSocket 连接
- 请求和响应直接流式转发，支持 SSE，不缓存完整请求或响应
- 上游 HTTP/HTTPS 连接复用，优先使用最近空闲的连接，减少建连开销
- 处理请求、响应流和 WebSocket 断连；客户端取消后释放上游连接
- 配置 API Key 替换或补充上游认证头，支持浏览器跨域请求
- 支持连接超时、活跃连接空闲超时以及容器停止时的优雅退出
- 访问日志异步批量写入文件，按北京时间逐日分割，保留最近 90 个自然日
- 通过环境变量配置

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `TARGET_DOMAIN` | 目标域名（含协议） | `https://example.com` |
| `PORT` | 监听端口 | `3000` |
| `API_KEY` | 替换已有 `Authorization`、`x-api-key`、`api-key`；无认证头时补充 `Authorization: Bearer ...` | 空，不覆盖 |
| `CONNECT_TIMEOUT_MS` | DNS/TCP/TLS 建连等待上限，毫秒；`0` 禁用 | `10000` |
| `UPSTREAM_IDLE_TIMEOUT_MS` | 活跃上游连接无读写活动的等待上限，毫秒；`0` 禁用 | `300000` |
| `KEEP_ALIVE_TIMEOUT_MS` | 连接池中空闲连接的保留时间，毫秒；`0` 禁用定时回收 | `4000` |
| `SHUTDOWN_TIMEOUT_MS` | 收到 SIGTERM/SIGINT 后等待活跃请求结束的上限，毫秒 | `10000` |
| `LOG_DIR` | 直接用 Node 启动时的日志目录；Docker 固定为 `/app/logs`，挂载到主机 `./logs` | 代码目录下的 `logs` |

持续传输数据的 SSE 不受总时长限制。若上游可能超过 5 分钟完全不返回数据，可增大 `UPSTREAM_IDLE_TIMEOUT_MS` 或设为 `0`。建立后的 WebSocket 不使用此空闲超时。

Docker Compose 的停止宽限期为 15 秒。增大 `SHUTDOWN_TIMEOUT_MS` 时，也需要把 `stop_grace_period` 调得更长。

## 访问日志

Docker Compose 将主机 `./logs` 挂载到容器 `/app/logs`。每天一个 `access-YYYY-MM-DD.log` 文件，每行一个 JSON 对象；时间含毫秒和 `+08:00` 时区。文件以追加模式打开，重启不会覆盖当天日志。启动、错误和日志故障的控制台输出也包含时间。

每个 HTTP 请求记录 `start` 和 `complete`（或 `aborted`），包括请求 ID、客户端 IP、方法、路径、状态码、耗时、重试次数及错误码。WebSocket 另记录握手 `upgrade` 和连接 `close`。`start` 会在请求仍运行时写入，因此 SSE/WebSocket 长连接也能及时观察到使用情况。

```json
{"time":"2026-10-11T18:30:00.123+08:00","request_id":"123-abc-1","protocol":"http","client_ip":"10.0.1.20","method":"POST","path":"/v1/chat/completions","event":"complete","status":200,"duration_ms":1250.34,"retries":0}
```

查看当天日志：

```bash
tail -f "./logs/access-$(TZ=Asia/Shanghai date +%F).log"
```

日志省略查询参数、认证头和请求体，路径最长记录 1024 个字符。格式化、写盘和旧文件清理在独立工作线程执行；主进程只入队，约每 100 毫秒分批发送，不等待写盘。启动、跨日和每小时检查过期文件，只清理本服务的 `access-YYYY-MM-DD.log`，保留当天及前 89 天。

为保证转发优先，未完成写入的队列最多容纳 8192 条记录；磁盘故障或积压超过上限时允许丢弃日志，并限频输出告警，代理继续转发。文件写入失败后自动重试，工作线程异常退出后自动重启。正常停止会尝试刷完日志，最多额外等待 1 秒；强制终止或存储故障时可能丢失队列中的记录。

## 使用

```bash
# 设置环境变量并启动
TARGET_DOMAIN=https://your-new-domain.com docker-compose up -d

# 或创建 .env 文件
echo "TARGET_DOMAIN=https://your-new-domain.com" > .env
docker-compose up -d
```

已有部署更新代码后，重新构建并启动：

```bash
docker compose up -d --build
```

## 故障处理

- 上游拒绝连接或在响应开始前断开，返回带 CORS 头的 `502`；建连或空闲超时返回 `504`。
- 响应开始后上游断开，关闭当前流，避免往 JSON/SSE 中追加错误内容；其他请求继续正常处理。
- 仅当复用的上游连接发生 `ECONNRESET`，且响应尚未开始时，对无请求体的 GET/HEAD 使用新连接重试一次。POST、带请求体的请求和已经开始的响应不重试。
- WebSocket 握手被上游拒绝时，转发实际状态码和响应内容；断连时清理两端连接。
- 错误日志记录请求方法、路径和错误码，省略查询参数、请求体及认证密钥。

这些处理可以避免连接错误拖垮代理进程；上游服务未监听端口导致的 `ECONNREFUSED` 仍需在目标服务或网络侧修复。

## 测试

无需安装依赖，使用 Node.js 内置测试运行器：

```bash
npm test
```

测试使用本地真实 TCP/HTTP 连接，覆盖上游连接拒绝、重置、流式传输、超时、客户端取消、连接复用、安全重试、WebSocket 双向转发以及进程退出。

## 性能对比

将修改前的 `index.js` 保存为独立 `.cjs` 文件后运行：

```bash
npm run benchmark -- /path/to/original-index.cjs
```

脚本在本地启动原版和当前版两个代理进程，使用相同 Node 运行时、32 并发和 1 KB 响应。分别测量客户端长连接与短连接；预热后交替执行 5 轮，输出吞吐、P50/P95 延迟和新增上游连接数的中位数。需要用目标环境的 Node 版本运行，以评估实际性能。
