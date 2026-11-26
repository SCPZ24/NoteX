# NoteX — Go 高性能微服务微博后端系统

NoteX 是一个基于 **Go + Gin + gRPC + MySQL + Redis + RabbitMQ + etcd** 构建的纯后端微服务微博系统。项目围绕社交内容平台的典型高并发工程难题展开深耕，重点解决以下四类核心问题：

1. **热点点赞写入冲击与异步合并**：冷热自适应分流，热帖通过分段 Bitmap + Lua 承载高频读写，经 Redis Stream 与 RabbitMQ 单活队列批量折叠落库。
2. **大 V 写放大与推拉混合 Feed 分发**：普通作者写扩散 + 大 V 读扩散，结合分页规划、分块 Fanout、有界 Redis Pipeline、多路归并游标分页与强一致性兜底。
3. **分布式异步一致性与可靠事件投递**：基于 MySQL 事务 Outbox + Relay 发布确认 + 消费端事务去重/租约保护 + 人工可审计重放，保障至少一次投递与消费幂等。
4. **多角色微服务拆分与分片容灾治理**：按职责拆分为 6 类独立进程角色，通过 gRPC 收敛同步边界，结合 etcd 服务发现/动态配置与 RabbitMQ Quorum 单活分片队列实现并发隔离与故障接管。

> 接口契约文档见 [`docs/interfaces.md`](docs/interfaces.md)，运维与实验细则见 [`docs/operations/`](docs/operations/)。

---

## 系统架构与进程角色

NoteX 支持单进程组合模式（`NOTEX_ROLE=all`，便于本地快速验证）和 **6 类独立角色分进程部署模式**（配合 etcd 治理）。

```mermaid
flowchart TB
    Client["HTTP 客户端 / 压测脚本"]

    subgraph HTTP["HTTP 入口层 (Gin)"]
        Core["core (:8080)<br/>账号/关系/发帖/删帖/详情/点赞<br/>+ gRPC CoreReadService (:9090)"]
        FeedQuery["feed-query (:8081)<br/>Feed 首页 / 个人帖子列表<br/>多路归并 + SQL 兜底"]
    end

    subgraph Async["异步与重建层"]
        Relay["relay (:8084)<br/>Outbox 中继 + Like Stream 中继<br/>+ 热帖冷却调度"]
        FeedWorker["feed-worker (:8082+)<br/>Feed 根消费 / Planner / 分片 Chunk 扇出"]
        FeedRebuilder["feed-rebuilder (:8083)<br/>读者 & 作者索引冷重建<br/>+ gRPC FeedRebuildService (:9091)"]
        LikeWorker["like-worker (:8086)<br/>点赞批量折叠落库"]
    end

    subgraph Infra["基础设施"]
        MySQL[("MySQL (InnoDB)<br/>权威业务数据 / Outbox / 消费任务 / 检查点")]
        Redis[("Redis 7.4 (noeviction + AOF)<br/>Feed ZSet / 正文缓存 / 分段 Bitmap / Stream")]
        MQ[("RabbitMQ 4.2<br/>Classic 队列 + Quorum SAC 分片/点赞队列")]
        Etcd[("etcd 3.6<br/>服务发现 Lease + Runtime CAS 配置")]
    end

    Client -->|写操作 / 详情 / 点赞| Core
    Client -->|Feed / 作者列表| FeedQuery

    Core --> MySQL
    Core --> Redis
    Core -.->|热帖晋级预热作者索引 gRPC| FeedRebuilder

    FeedQuery -->|鉴权 / 关系 / 候选回查 gRPC| Core
    FeedQuery -->|冷索引重建 gRPC| FeedRebuilder
    FeedQuery --> Redis

    Relay -->|扫描 outbox_events| MySQL
    Relay -->|读取 like:stream / 冷却| Redis
    Relay -->|Publisher Confirm| MQ

    MQ -->|feed / feed.plan / feed.chunk.s000..| FeedWorker
    FeedWorker -->|权威读 gRPC| Core
    FeedWorker -->|关系重建 gRPC| FeedRebuilder
    FeedWorker -->|任务状态 / 槽位锁| MySQL
    FeedWorker -->|Pipeline 写入读者索引| Redis

    FeedRebuilder -->|Snapshot 读取 gRPC| Core
    FeedRebuilder --> Redis

    MQ -->|likes Quorum SAC| LikeWorker
    LikeWorker -->|批量折叠落库 + checkpoint| MySQL
    LikeWorker -->|清理 pending 水位| Redis

    Core -.-> Etcd
    FeedQuery -.-> Etcd
    FeedWorker -.-> Etcd
    FeedRebuilder -.-> Etcd
    Relay -.-> Etcd
    LikeWorker -.-> Etcd
```

### 6 类进程角色职责

| `NOTEX_ROLE` | 默认 HTTP 端口 | gRPC 边界 | 默认 MySQL 连接上限 | 核心职责 |
|---|---:|---|---:|---|
| `core` | `8080` | 提供 `CoreReadService` (`:9090`) | 普通池 `32` + 专读池 `4` | 账号会话、关注关系、发帖/删帖、正文缓存、冷热点赞入口，以及全部权威读 RPC |
| `feed-query` | `8081` | 调用 `CoreReadService` & `FeedRebuildService` | 无直连 | 承接 `/api/v1/feed` 与 `/api/v1/users/:id/posts`，负责多路归并、候选校验与 SQL 回退 |
| `feed-worker` | `8082` | 调用 `CoreReadService` & `FeedRebuildService` | `4`（受 etcd 管控） | 消费 `feed` / `feed.plan` / `feed.chunk.sXXX`，执行分块扇出、关系重算与分片重试 |
| `feed-rebuilder` | `8083` | 提供 `FeedRebuildService` (`:9091`) | 无直连 | 隔离冷缓存重建压力，通过有界 `singleflight` 与短租约从 Core 拉快照并安装至 Redis |
| `relay` | `8084` | 无 | `2` | 扫描 `outbox_events` 投递 MQ；自适应点赞模式下负责 Stream -> MQ 中继与热帖退火 |
| `like-worker` | `8086` | 无 | `10` | 单活消费 `.likes` Quorum 队列，按 Stream 位置去重折叠并批量事务写入 `post_likes` |

---

## 核心技术设计与实现原理

### 1. 热点点赞与异步合并落库

对应代码：[`src/internal/post/like_heat.go`](src/internal/post/like_heat.go)、[`src/internal/post/likes.go`](src/internal/post/likes.go)、[`src/internal/outbox/likes.go`](src/internal/outbox/likes.go)

为解决单篇热门文章遭遇高频点赞/取消带来的 MySQL 行锁争用，同时避免为大量冷门文章浪费 Redis 内存，系统实现了**冷热自适应点赞架构**（`LIKE_MODE=adaptive`）：

```mermaid
flowchart LR
    Req["PUT/DELETE 点赞<br/>或 GET 详情"] --> Heat["Redis Lua 滑动窗口观测<br/>observeHeat"]
    Heat -->|"冷态<br/>(8s 内写 <=256 且读 <=2048)"| ColdSQL["冷路径: MySQL 行锁<br/>SELECT ... FOR UPDATE<br/>同步写入 post_likes (200)"]
    Heat -->|"触发晋级<br/>(写 >256 或 读 >2048)"| Promote["MySQL 帖子行锁 + promoteLike<br/>写入 :hot 标记 + 入队冷却 ZSet<br/>异步预热作者索引"]
    Promote --> HotLua
    Heat -->|"已热态"| HotLua["Redis Lua 原子读写<br/>accessLike"]
    HotLua -->|"段未就绪 (-1)"| BuildSeg["singleflight + 短租约 Claim<br/>按 4096B 段回源 MySQL 装填 Bitmap<br/>installSegment (SETRANGE)"]
    BuildSeg --> HotLua
    HotLua -->|"同态请求 / 仅读取"| RespFast["直接返回位图状态<br/>无写扩散"]
    HotLua -->|"状态变化 (0<->1)"| Stream["同脚本原子执行:<br/>XADD :like:stream<br/>SET :pending stream_id<br/>SETBIT :bits (返回 202)"]
    Stream --> LikeRelay["Relay 顺序拉取 <=64 条<br/>Confirm 发布至 .likes Quorum 队列<br/>确认后 XDEL"]
    LikeRelay --> LikeWorker["LikeWorker 单活消费<br/>攒批 (<=8 批或 20ms)<br/>按 (post_id, user_id) 折叠终态"]
    LikeWorker --> ApplyTX["单事务: 锁 like_checkpoints<br/>跳过 <= (stream_ms, stream_seq)<br/>批量 INSERT/DELETE + 推进位点"]
    ApplyTX --> ClearPending["Pipeline Lua 清理匹配 :pending<br/>手动批量 Ack(multiple=true)"]
```

**关键工程细节**：
- **滑动窗口冷热切换与滞回防抖**：使用 Redis `TIME` 统一时钟与定长 List 维护最近时间戳，8 秒内点赞写请求 `> 256` 次或详情读 `> 2048` 次晋级热帖；冷却（`CoolLikes`）需满足 192 秒内写 `< 16` 且读 `< 128` **且无 `:pending` 未落库积压**才退火清理位图，避免冷热抖动及数据丢失。
- **冷热并发切换保护**：冷路径写入 `coldLike` 与热点晋级 `heat` 共同竞争 `posts` 表同一行的 `FOR UPDATE` 锁，并在锁内二次检查 `:hot` 标记，防止晋级瞬间冷写穿透导致缓存与库状态倒挂。
- **按需分段 Bitmap（防大 Key 与 OOM）**：每段固定 `4096 字节`（覆盖 `32,768` 个连续用户 ID），单帖总位图上限 `1 MiB`（支持约 838 万用户 ID，超出范围自动降级 SQL）。访问某用户时仅按需懒加载其所属段，配合本地 `singleflight` 与 Redis `SETNX` 短租约回源，并用 `gen` + `token` 校验防旧构建覆盖。
- **有序合并与防乱序**：无需客户端版本号，直接利用 **Redis Stream 单调递增 Entry ID（`ms-seq`）作为全局版本号**。RabbitMQ `.likes` 队列采用 `Quorum + x-single-active-consumer`，确保单消费者串行消费；消费端攒批后在 MySQL 事务中锁定 `like_checkpoints`，自动跳过小于等于当前检查点的重复/迟到消息，按 `(post_id, user_id)` 折叠为最终状态后执行多行批量 `INSERT ... ON DUPLICATE KEY UPDATE` / `DELETE ... IN (...)`。

---

### 2. 推拉混合 Feed 架构与读侧强一致性

对应代码：[`src/internal/feed/fanout.go`](src/internal/feed/fanout.go)、[`src/internal/feed/distribution.go`](src/internal/feed/distribution.go)、[`src/internal/feed/read.go`](src/internal/feed/read.go)、[`src/internal/feed/cache.go`](src/internal/feed/cache.go)、[`src/internal/feed/relationship.go`](src/internal/feed/relationship.go)、[`src/internal/post/cache.go`](src/internal/post/cache.go)

系统支持 `sql`（基线）、`push`（纯推）和 `hybrid`（推拉混合）三种运行模式，所有节点通过 SHA-256 `PolicyID` 校验推拉策略一致。

```mermaid
flowchart TB
    subgraph WritePath["写侧: 三阶段分块扇出 (普通作者)"]
        PostCreate["POST /api/v1/posts<br/>事务写 posts + post_requests + outbox"] --> Root["feed 根消费<br/>更新作者 ZSet 索引"]
        Root -->|"大 V (FEED_PULL_AUTHORS)"| StopFanout["停止写扩散<br/>仅保留作者索引"]
        Root -->|"普通作者"| Plan["feed.plan (Planner)<br/>每页游标拉取 500 粉丝<br/>按 reader_id % P 分片 + 100人/Chunk<br/>单事务冻结 feed_tasks.children"]
        Plan -->|投递分片 Chunk + 下一页 Plan| Chunk["feed.chunk.sXXX (Chunk Worker)<br/>批量查读者 generation + 权威关系校验<br/>32 命令有界 Redis Pipeline 执行 cacheUpdate Lua"]
    end

    subgraph ReadPath["读侧: 推拉混合多路归并 (GET /api/v1/feed)"]
        FeedReq["GET /api/v1/feed"] --> Rel["Core gRPC GetFeedRelations<br/>计算关系 SHA-256 signature<br/>识别关注的大 V (<=64 个)"]
        Rel -->|"关系超限(>4096) / 大 V >64 / source=sql"| SQLFallback["Authoritative SQL 分页<br/>零关注走 selfFeedPage 索引分支<br/>游标标记 source=sql"]
        Rel --> Touch["Pipeline touchSources<br/>续期 7 天活跃窗口 / 检查 ready & signature"]
        Touch -->|"冷缓存 / 签名未追平"| SQLFallback
        SQLFallback -.->|"冷态异步/限时触发"| Rebuilder["FeedRebuildService<br/>有界重建最近 500 条快照"]
        Touch -->|"全部就绪"| Merge["多路 ZREVRANGEBYLEX 归并<br/>计算全局安全前沿 frontier<br/>最多 8 轮 / 候选上限 8192"]
        Merge --> Resolve["Core gRPC ResolveFeedCandidates<br/>批量回查 MySQL 校验最新关系/删除与正文"]
        Resolve --> Validate["validateSources 二次校验 generation & revision<br/>防读取期间并发变更"]
        Validate --> PageOut["输出 limit 条 + Base64 游标"]
    end
```

**核心亮点与防坑机制**：
- **ZSet 字典序无损排序**：成员编码为 `fmt.Sprintf("%020d:%020d", created_on, post_id)`，统一 `score=0`，借助 `ZREVRANGEBYLEX` 精确实现 `(created_on DESC, post_id DESC)` 稳定全序，规避 64 位 ID 放入 IEEE 754 float64 score 时的精度丢失。
- **有界化与防写放大**：
  - 写侧：`Planner` 每页 500 粉丝冻结一次子任务（不把十万粉丝全量塞进内存），按 `reader_id % shards` 归组后再每 100 人切为一个 `feed.chunk`，每个 Redis Pipeline 严格限制 `<= 32` 条 Lua 脚本（遇 `NOSCRIPT` 自动回退 `EVAL`）。
  - 内存控制：每个读者/作者索引仅保留最近 `500` 条记录，超限时裁剪并记录 `floor`（覆盖下界）；读者收件箱自带 `7 天`滑动 TTL，仅由真实读请求续期，非活跃粉丝自然过期、后台扇出自动跳过空 `generation` 的僵尸粉丝。
- **安全前沿（`frontier`）多路归并**：混合模式下同时拉取 1 个读者收件箱 + 最多 64 个大 V 作者索引。每轮取各源 `limit+1` 条，以所有未耗尽来源末尾成员的最大值（叠加 `coverageFloor`）作为 `frontier`，只消费 `>= frontier` 的全局确定前缀，保证绝不遗漏任何跨源新帖；触达 `floor` 后自动切换到 SQL 历史分页并在游标中打标 `source="sql"`。
- **关系变更与迟到写入防护**：读者缓存维护 `generation`（每次冷建刷新）、`revision`（每次写入递增）与关系列表 SHA-256 `signature`。取关时执行 `closeRelationship` Lua，记录 `blocked:<relation_id>` 屏蔽迟到扇出并清理旧关系帖子；读取侧对候选集批量回查 MySQL 权威状态并在返回前二次验证 `generation/revision`。
- **零关注读者 SQL 优化**：针对无关注用户回退 SQL 时沿全局时间索引扫描全表的问题，[`selfFeedPage`](src/internal/post/service.go#L328-L367) 在单条 CTE 语句内判断是否存在关注并直接走作者索引 `(author_id, deleted_on, created_on DESC, id DESC)`，将扫描行从 27,144 行降至 0 行。
- **高可靠正文缓存与并发保护**（[`src/internal/cache/cache.go`](src/internal/cache/cache.go)、[`src/internal/post/cache.go`](src/internal/post/cache.go)）：
  - 自研有界 `singleflight`（最大并发等待者 64、最大飞行 Key 64）+ SQL 并发准入信号量（4 并发 + 16 队列 / 100ms 超时快速降级 `503 cache_busy`），彻底隔离缓存击穿对 MySQL 的雪崩冲击。
  - 删除帖子时写入 11 分钟墓碑（`:deleted`）并撤销构建 token，且正文 Key 嵌入 Redis 服务端 `run_id`，防止 Redis 重启加载旧 RDB/AOF 导致已删除帖子“复活”。

---

### 3. 事务 Outbox 与可靠事件投递 / 消费

对应代码：[`src/internal/outbox/`](src/internal/outbox/)、[`src/cmd/events/main.go`](src/cmd/events/main.go)

```mermaid
sequenceDiagram
    participant API as Core Service
    participant DB as MySQL
    participant Relay as Outbox Relay
    participant MQ as RabbitMQ
    participant Worker as Consumer / Worker

    API->>DB: 单事务: 业务变更 (post_requests/posts/following) + INSERT outbox_events
    DB-->>API: COMMIT
    API--)Relay: 非阻塞唤醒 (chan struct{}, cap=1) + 5s 兜底轮询

    Relay->>DB: 查询未发布且到期事件 (LIMIT 32, 查完即释放连接)
    Relay->>MQ: Publish (Persistent + Mandatory)
    MQ-->>Relay: basic.return (如不可路由) / Publisher Confirm (Ack/Nack)
    alt 全部目标队列确认成功
        Relay->>DB: UPDATE outbox_events SET published_on = NOW()
    else 失败/超时/不可路由
        Relay->>DB: 指数退避 next_attempt_on (1s..32s), 记录 last_error
    end

    MQ->>Worker: 投递消息 (Qos=1)
    Worker->>DB: INSERT INTO consumption_jobs (ON DUPLICATE KEY) / 坏消息 SHA-256 隔离
    Worker->>DB: 事务领取任务 (SQL 模式用 SAVEPOINT; 外部模式推进 attempts + 30s 租约)
    Worker->>Worker: 执行幂等副作用 (Redis / 下游任务)
    Worker->>DB: 按单调 attempts 条件更新状态 (completed / pending 退避 / failed)
    Worker->>MQ: 手动 ACK (确认前宕机可由 MQ 或 MySQL 重试恢复)
```

**可靠性保障机制**：
- **发帖接口幂等**：强制携带 `Idempotency-Key`，通过 `post_requests(user_id, request_key)` 联合主键在同事务仲裁并发请求；主键冲突（MySQL `1062`）时回滚并脱离 `REPEATABLE READ` 快照重读已提交结果，正文一致返回原 `201`，正文不同返回 `409 idempotency_conflict`。
- **不跨网络持锁**：Relay 先查库释放连接再发 MQ；若 `amqp091-go` 写阻塞超 5 秒，通过 `context.AfterFunc` 主动关闭底层 socket 中断阻塞并进入可重试状态。
- **两套消费协议**：
  - **SQL 副作用消费（`RunConsumer`）**：在事务内 `SELECT ... FOR UPDATE` + `SAVEPOINT effect`，业务失败回滚到保存点并记录重试计数，单事务原子完成。
  - **外部副作用消费（`RunExternalConsumer` / `RunShard`）**：分离“领取”与“完成”两段短事务。领取时递增单调 `attempts` 并设置 30 秒租约；完成更新附带 `WHERE attempts = ?` 乐观锁，彻底防止超时僵尸 Worker 覆盖新 Worker 的执行结果。
- **死信隔离与人工重放审计**：格式非法或同 ID 载荷冲突的毒丸消息通过 SHA-256 指纹写入隔离记录；普通失败指数退避最多 5 次转 `failed`；通过 `cmd/events list|replay` 可检索原始 Base64 字节并带原因人工重放，审计轨迹写入 `event_replays`。

---

### 4. 微服务治理（etcd）与分片单活消费

对应代码：[`src/internal/control/`](src/internal/control/)、[`src/internal/feed/managed.go`](src/internal/feed/managed.go)、[`src/internal/outbox/shards.go`](src/internal/outbox/shards.go)、[`src/cmd/control/main.go`](src/cmd/control/main.go)

```mermaid
flowchart LR
    CLI["cmd/control<br/>init / assign / apply / status"] -->|"etcd Txn CAS (ModRevision)"| Etcd["etcd<br/>/notex/{prefix}/runtime<br/>/notex/{prefix}/services/{role}/{id}"]

    Etcd -->|"Watch (断线保留快照 / 压缩自动重拉)"| Workers["feed-worker 实例组<br/>worker-a / worker-b / worker-c"]
    Etcd -->|"gRPC Custom Resolver + round_robin"| Clients["feed-query / feed-worker<br/>发现 core & feed-rebuilder"]

    subgraph Safety["双保险防脑裂与有序接管"]
        SlotLock["MySQL GET_LOCK(sha256(prefix/id))<br/>2s 心跳校验防同名双活"]
        Drain["配置变更 -> 旧代消费组 5s 优雅排空<br/>-> 应用新并发/SQL 预算 -> 重新订阅"]
        SAC["RabbitMQ Quorum 分片队列<br/>feed.chunk.s000..s003<br/>x-single-active-consumer=true<br/>首选 Priority=100 / 备选 Priority=10"]
    end

    Workers --> SlotLock
    Workers --> Drain
    Drain --> SAC
```

- **分片单活消费与无损故障接管**：
  - 按 `reader_id % shards` 将扇出 Chunk 路由至固定 Quorum 队列（`feed.chunk.s000..s00N`），启用 `x-single-active-consumer` 且每分片 `prefetch=1`，保证同一读者的分片内串行执行。
  - 所有预留槽位的 Worker 同时订阅全部分片，通过 etcd 动态配置下发 `preferred` 分配表：首选实例以 `x-priority=100` 订阅，备选实例以 `x-priority=10` 待命。
  - 扩缩容或首选切换时，RabbitMQ 4.2 SAC 会等待原活跃消费者确认在途消息后自动转交；当活跃 Worker 异常 `SIGKILL` 宕机时，Broker 在连接断开后秒级将消费权切换至最高优先级备选 Worker，配合 `RunShardRetries` 的随机 `retry_publish_token` 协调扫描 MySQL 到期重试任务并回投原分片。
- **etcd 容灾与安全边界**：
  - gRPC 服务端拦截器采用 `subtle.ConstantTimeCompare` 校验 32 字节以上共享 `RPC_TOKEN`，并透传 `x-request-id`。
  - etcd 注册采用 15 秒租约 `LeaseKeepAlive` + `Version/LeaseValue` CAS 防同 ID 冲突；etcd 失联或 Watch 遭遇 Revision 压缩时，业务节点保留最后一份合法配置/地址快照继续服务，恢复后自动重新拉取全量快照续接 Watch。
  - 每个 `feed-worker` 额外持有 MySQL 连接级 `GET_LOCK` 独占槽位锁，即使在 etcd 网络分区期间也能绝对阻止同一槽位启动两个活跃实例。

---

## 数据库结构与目录导航

### MySQL 表结构（[`migrations/`](migrations/)）

| 迁移文件 | 核心表 | 作用 |
|---|---|---|
| `0001_accounts_follow.sql` | `users`, `sessions`, `following` | 用户（bcrypt）、Bearer 会话（SHA-256）、关注关系与双向游标索引 |
| `0002_posts.sql` | `posts` | 帖子正文、软删除标记及作者/全局时间复合索引 |
| `0003_post_requests.sql` | `post_requests` | 发帖 `Idempotency-Key` 去重表与首次响应快照 |
| `0004_outbox_events.sql` | `outbox_events` | 事务事件发件箱及重试/隔离状态 |
| `0005_event_consumption.sql` | `consumption_jobs`, `event_replays`, `dummy_effects` | 消费幂等去重表、人工重放审计记录 |
| `0006_feed_tasks.sql` | `feed_tasks` | Feed Planner 冻结的不可变分页/分块子任务表 |
| `0007_feed_topology.sql` | `feed_topologies`, `consumption_jobs` 扩展列 | 固化分片拓扑校验与分片重试投递 token |
| `0008_post_likes.sql` | `post_likes`, `like_checkpoints` | 点赞联合主键表与 Redis Stream 消费检查点 |

### 代码目录结构

```text
NoteX/
├── compose.yaml                  # 本机 Redis 7.4 / RabbitMQ 4.2 / etcd 3.6 资源受限编排
├── migrations/                   # 0001 ~ 0008 增量 SQL DDL
├── src/
│   ├── main.go                   # 主服务入口 (通过 NOTEX_ROLE 切换单进程或 6 类角色)
│   ├── cmd/
│   │   ├── control/              # etcd 运行配置与分片分配 CLI (init/status/apply/assign)
│   │   └── events/               # Outbox & 消费任务查询/重放 CLI (list/replay/dummy)
│   └── internal/
│       ├── account/              # 注册、登录、SHA-256 会话认证
│       ├── follow/               # 关注/取关 (1213 死锁重试)、双向游标分页
│       ├── post/                 # 发帖幂等、正文缓存(run_id/墓碑)、冷热自适应点赞、SQL Feed
│       ├── feed/                 # 推拉混合读归并、Planner/Chunk 扇出、索引重建 RPC、托管运行
│       ├── outbox/               # 事务发件箱、MQ 确认发布、消费协议、分片 SAC、点赞中继与落库
│       ├── core/                 # CoreReadService gRPC 定义 (core.proto) 与权威读边界
│       ├── control/              # etcd 配置 Watch、租约注册、gRPC 服务发现 Resolver
│       ├── cache/                # 有界 singleflight、SQL 并发准入信号量、分布式短租约
│       ├── httpapi/              # Gin 路由、认证中间件、统一错误码映射
│       ├── app/                  # 6 类角色装配、MySQL 槽位锁、pprof 诊断端点
│       └── platform/             # MySQL 连接池封装、gRPC 认证/超时拦截器
├── scripts/                      # API 回归、Feed 验收、综合阶梯压测、热点点赞压测脚本
├── docs/                         # 接口文档、运维手册、压测方案
└── notes/                        # 各阶段压测报告与原始实验归档
```

---

## 压测与容灾验证实录

所有测试均在单机 MacBook（资源受限预算：`GOMAXPROCS=2`，Redis 1 GiB `noeviction`，RabbitMQ 768 MiB，etcd 256 MiB）下执行并完整核对数据一致性。

### 1. 百万关注规模 Feed 读取对照（[`notes/fanout/07-acceptance.md`](notes/fanout/07-acceptance.md)）
- **数据集 Q**：12 万用户、**100 万关注关系**、20 万帖子；混合模式固定 64 位拉取大 V；每模式 3 轮 300s 采样（共 176,761 次请求），逐条校验与 SQL 参考结果完全一致。

| 运行模式 | 100 关注作者 P95 | 500 关注作者 P95 | 1000 关注作者 P95 | 平均每请求 SQL 扫描行数 |
|---|---:|---:|---:|---:|
| **SQL 基线** | **70.00 ms** (`69.999`) | 19.27 ms | 13.02 ms | 26,114 行 |
| **Push 纯推** | **11.96 ms** (`11.955`) | 12.56 ms | 13.28 ms | 1,272 行 |
| **Hybrid 混合** | **18.91 ms** (`18.913`) | 18.39 ms | 18.91 ms | **1,334 行（降幅 94.9%）** |

- **大 V 扇出容量（数据集 F）**：1,000 / 10,000 / 100,000 活跃粉丝完整扇出分别耗时约 **0.22s / 1.72s / 11.77s**；10 万粉丝扇出期间并发插入的普通作者帖子在 ~110ms 内完成投递，无队列饥饿。
- **零关注回退优化（[`notes/performance-tests/batch06/README.md`](notes/performance-tests/batch06/README.md)）**：消除零关注读者回退 SQL 时的全局时间索引扫描（27,144 行 -> 0 行，单查询从 184ms 降至 0.10ms），1024 QPS 综合混合压测下 Feed 成功率由 96.67% 升至 **99.95%**，P99 从 89.9ms 降至 **39.8ms**。

### 2. 热帖点赞异步合并写库对照（[`notes/performance-tests/likes/README.md`](notes/performance-tests/likes/README.md)）
- **负载模型**：80% 点赞/取消写（其中 80% 集中于单一热帖）+ 20% 详情读，相同 30s 轨迹回放（15,360 请求 / 12,289 次写操作）：
  - MySQL `post_likes` 已提交 **DML 执行次数从 12,289 次降至 4,462 次（降低 63.69%）**；
  - 排空队列后，最终点赞关系集合（2,596 条）与纯 SQL 组及参考状态 SHA-256 摘要 **100% 一致**。

### 3. 故障注入与多实例治理回归（[`notes/fanout/07-acceptance.md`](notes/fanout/07-acceptance.md)、[`docs/operations/distributed.md`](docs/operations/distributed.md)）
- **MQ 停机 >= 60s**：期间提交 10,000 笔发帖事务不丢单，恢复后双队列按事件身份 100% 追平。
- **Redis 停机 >= 60s**：发帖与 Feed 查询自动回退 MySQL 正常服务，恢复后投影追平，且借助 `run_id` 隔离避免旧缓存复苏。
- **Worker `SIGKILL` 强杀与 etcd 分区**：Quorum SAC 秒级接管未完成分片，迟到写入被 `attempts` 令牌与 Redis `generation` 拦截，MySQL `GET_LOCK` 阻断同槽位双活，etcd 断线保留快照并在 Revision 压缩后自动重拉。

---

## 快速启动与运行

### 1. 启动基础设施与初始化表结构

```bash
# 启动 Redis 与 RabbitMQ（如需多进程治理，附加 --profile governance 启动 etcd）
docker compose --profile governance up -d

# 按顺序导入 migrations/0001 ~ 0008 至目标 MySQL 数据库
for f in migrations/*.sql; do
  mysql -u root -p notex < "$f"
done
```

### 2. 单进程组合模式运行（本地调试）

```bash
export MYSQL_DSN='notex:password@tcp(127.0.0.1:3306)/notex'
export REDIS_URL='redis://127.0.0.1:6380/0'
export RABBITMQ_URL='amqp://notex:notex-local@127.0.0.1:5673/'
export RABBITMQ_QUEUE_PREFIX='notex'
export FEED_MODE='hybrid'
export FEED_PULL_AUTHORS='42,108'
export LIKE_MODE='adaptive'

go run ./src
```

### 3. 分进程运行

分进程模式支持静态 RPC 地址或启用 `ETCD_ENDPOINTS` 服务治理（需提供 32 字符以上的 `RPC_TOKEN`）。

```bash
# 1. 构建主程序与控制 CLI
go build -o /tmp/notex ./src
go build -o /tmp/notex-control ./src/cmd/control

# 2. 导出公共配置并初始化 etcd 运行时配置（4 分片，3 槽位，2 活跃）
export ETCD_ENDPOINTS='http://127.0.0.1:23790'
export MYSQL_DSN='notex:password@tcp(127.0.0.1:3306)/notex'
export REDIS_URL='redis://127.0.0.1:6380/0'
export RABBITMQ_URL='amqp://notex:notex-local@127.0.0.1:5673/'
export RABBITMQ_QUEUE_PREFIX='notex'
export RPC_TOKEN='replace-with-a-secret-token-at-least-32-chars'
export FEED_MODE='hybrid'
export FEED_PULL_AUTHORS='42,108'
export LIKE_MODE='adaptive'

/tmp/notex-control init -workers worker-a,worker-b,worker-c -active worker-a,worker-b -shards 4 -sql 4 -concurrency 2

# 3. 按依赖顺序在不同终端拉起 6 类角色
NOTEX_ROLE=core           INSTANCE_ID=core-a      /tmp/notex
NOTEX_ROLE=feed-rebuilder INSTANCE_ID=rebuilder-a /tmp/notex
NOTEX_ROLE=feed-query     INSTANCE_ID=query-a     /tmp/notex
NOTEX_ROLE=feed-worker    INSTANCE_ID=worker-a    HTTP_ADDR=127.0.0.1:8082 /tmp/notex
NOTEX_ROLE=feed-worker    INSTANCE_ID=worker-b    HTTP_ADDR=127.0.0.1:8085 /tmp/notex
NOTEX_ROLE=relay          INSTANCE_ID=relay-a     /tmp/notex
NOTEX_ROLE=like-worker    INSTANCE_ID=like-a      /tmp/notex
```

### 4. 运维 CLI 与测试入口

```bash
# 查看 etcd 运行配置与服务注册状态
/tmp/notex-control status

# 在线扩容/调整分片首选归属（传入 status 查到的当前 revision 做 CAS）
/tmp/notex-control assign -active worker-a,worker-b,worker-c -expect <REVISION>

# 查询失败消费任务并带原因人工重放
go run ./src/cmd/events list --scope consumer --status failed --limit 20
go run ./src/cmd/events replay --scope consumer --id <JOB_ID> --reason '下游恢复后重放'

# 运行单元测试
go test ./src/...
```
