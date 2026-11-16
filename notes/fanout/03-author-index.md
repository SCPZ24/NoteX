# 阶段三 Redis 作者索引

## 实现范围

Feed 专属实现集中在 `src/internal/feed/service.go`。现有 Outbox 经 RabbitMQ 交付根事件，外部消费者回读 MySQL 当前帖子状态，再执行 Redis 条件更新。根事件的原始消息、尝试次数、失败与重放记录保存在现有 consumption_jobs 和 event_replays 表。

帖子事件 payload 为 `{"post_id":9001}`。关系事件 payload 为 `{"actor_id":7,"target_user_id":42,"relation_id":501}`，relation_id 来自实际建立或删除的 following 行。每轮清空实验数据并使用同一轮程序，事件结构由本轮代码定义。

关系任务暂存为 waiting，原消息和原因可靠落库后 ACK，后续分发阶段通过显式重放继续处理。当前完成标记对应作者索引效果。SQL Feed 的接口读取路径仍作为权威对照，混合读取在后续阶段接入。

## 启动与处置

依赖采用 go-redis v9.7.3，Redis 镜像固定到本轮拉取的 7.4.12 摘要。Compose Redis 使用宿主机 6380 端口，1 CPU／256 MiB，maxmemory 128 MiB，noeviction，AOF everysec，独立数据卷。删除标记与作者索引共同保留，容量不足会进入失败恢复路径。

在本轮新建且已运行五份迁移的数据库上设置 MYSQL_DSN。以下命令从仓库根目录执行。

```bash
docker compose up -d redis rabbitmq
export RABBITMQ_URL='amqp://notex:notex-local@127.0.0.1:5673/'
export RABBITMQ_QUEUE_PREFIX='notex'
export REDIS_URL='redis://127.0.0.1:6380/0'
GOMAXPROCS=2 GOMEMLIMIT=512MiB go run ./src
```

REDIS_URL 非空时，API 启动流程组装作者索引消费者。Redis 连接在执行任务时建立，Redis 暂时不可用时 HTTP 业务仍可提交 MySQL 事务。配置加载要求同时提供 RabbitMQ 地址。

```bash
go run ./src/cmd/events list --scope consumer --status failed
go run ./src/cmd/events list --scope consumer --status waiting
go run ./src/cmd/events replay --scope consumer --id "$RECORD_ID" --reason '依赖恢复或处理阶段已就绪'
```

Inspect 支持 waiting 查询。Replay 允许 failed／waiting 任务重新进入 pending，重置本轮 cycle_attempts，保留累计 attempts 和重放前错误。Dummy 命令沿用自己的隔离测试队列前缀，真实 Feed 使用实际应用前缀。

## 作者索引及原子状态更新

每个作者有两个 Redis 键，共享作者 hash tag。

```text
<prefix>:author:{42}:posts     ZSET，所有 score 均为 0
<prefix>:author:{42}:deleted   SET，成员为已删除的帖子 ID
```

排序成员采用 20 位零填充 Unix 秒、冒号和 20 位零填充帖子 ID，总长 41 字节。固定宽度字符串的字典序对应 `(created_on, post_id)` 的数值序。Lua 将 ID 作为字符串使用，覆盖超过 2^53 的 int64 ID。

### New 与 Close

```mermaid
flowchart LR
    A[New 解析 Redis URL] --> B[配置4连接池和2秒网络超时]
    B --> C[禁用客户端自动重试并启用context超时]
    C --> D[返回 Service]
    E[消费者结束] --> F[Close 关闭Redis客户端]
```

### Handle 与 apply

```mermaid
flowchart TD
    A[Handle 接收事件] --> B{事件类型}
    B -->|关系事件| C[返回 ErrWaiting]
    B -->|帖子事件| D[校验 post_id]
    B -->|未知类型| E[返回 ErrPermanent]
    D --> F[MySQL读取作者 时间和删除状态]
    F -->|查无帖子| E
    F -->|依赖错误| G[返回可重试错误]
    F --> H[apply 验证身份并编码排序成员]
    H --> I{当前删除状态}
    I -->|已删除| J[Lua先记录删除标记 再ZREM]
    I -->|有效| K[Lua检查删除标记]
    K -->|已标记| L[返回已处理]
    K -->|未标记| M[ZADD固定成员]
    J --> L
    M --> L
```

`keys` 为两个键生成同一作者 hash tag，`member` 生成精确的固定宽度排序成员。Lua 每次只处理一条帖子，操作次数固定，ZSET 更新成本随索引大小变化。Redis 脚本具有原子执行特性，执行中发生错误时已完成的命令可能保留，因此删除路径先保存终态标记，再移除成员，防止部分失败重新放行旧添加。

### ReadAuthor

```mermaid
flowchart TD
    A[ReadAuthor] --> B[校验作者 limit及可选before]
    B --> C[编码排他上界]
    C --> D[ZREVRANGEBYLEX有界读取]
    D --> E[校验41字节成员并解析int64]
    E --> F[返回降序Entry列表]
```

ReadAuthor 返回索引候选，limit 为 1～100，before 为排他边界。读取错误返回 error，空结果返回非 nil 空切片。上层 Feed 读取仍需按当前关系和删除状态执行批量权威检查。

## 外部任务恢复协议

SQLHandler 的 SQL Dummy 效果继续在事务内提交。ExternalHandler 接收 context 与 Event，执行结果通过返回值表达。

| 返回值 | 持久状态 | 后续动作 |
|---|---|---|
| nil | completed | ACK |
| ErrWaiting | waiting | 保存原始任务，等待显式重放 |
| ErrPermanent | failed | 保留审计与重放入口 |
| 其他错误 | pending 或 failed | 有界退避，最多5次尝试 |

### RunConsumer 与 RunExternalConsumer

两个公开入口选择各自的任务执行函数，共用 runConsumer、consumeSession 和 retryConsumption 的 MQ 交付与恢复循环。

```mermaid
flowchart TD
    A[RunConsumer] --> B[选择事务内SQL执行函数]
    C[RunExternalConsumer] --> D[选择外部执行函数]
    B --> E[runConsumer启动及恢复循环]
    D --> E
    E --> F[retryConsumption扫描已到期pending任务]
    F --> G[consumeSession连接MQ并设prefetch为1]
    G --> H[receive保留原消息及唯一身份]
    H --> I[执行选定处理函数]
    I -->|恢复进度已保存| J[ACK]
    I -->|持久化失败| K[关闭连接等待恢复]
    G -->|定时恢复| F
    K --> E
```

### claimExternal 与 processExternal

复用 attempts 作为递增领取身份，next_attempt_on 在执行期间表示领取到期时间。每次领取使用 MySQL 短事务，提交后才调用外部处理器。领取期限为30秒，单次处理 context 上限为5秒，结果回写另设5秒超时。

```mermaid
flowchart TD
    A[processExternal] --> B[claimExternal 开启短事务并锁任务行]
    B --> C{pending且已到期}
    C -->|否| D[结束本次尝试]
    C -->|是| E{本轮次数已达5次}
    E -->|是| F[保存最终领取过期失败原因]
    E -->|否| G[校验事件 增加attempts与cycle_attempts]
    G --> H[设置30秒到期时间并提交]
    H --> I[释放SQL连接后调用外部处理器]
    I --> J{父context已取消}
    J -->|是| K[保留领取记录等待到期恢复]
    J -->|否| L[计算完成 等待 重试或失败状态]
    L --> M[按任务ID consumer pending与attempts条件回写]
    M --> N[交回消费循环决定ACK]
```

旧执行者恢复时，如果任务已经被新执行者领取，累计 attempts 已增加，旧执行者的条件 UPDATE 匹配零行。目标效果仍需幂等，领取身份只保护任务进度。重放保留累计 attempts，因此旧执行者也无法覆盖重放后的领取。

普通依赖错误的重试间隔为1、2、4、8秒，第5次失败进入 failed。扫描周期为5秒，实际重试时刻受扫描和任务排队影响。连续退出导致最后一次领取到期时，保存“最终尝试结果需要核对”的失败原因，随后可显式重放。

### Inspect 与 Replay

```mermaid
flowchart TD
    A[Inspect] --> B[校验scope 状态和分页参数]
    B --> C[查询有界记录并返回原消息]
    D[Replay] --> E[事务锁定任务]
    E --> F[检查failed或waiting及原事件合法性]
    F --> G[重置pending和本轮次数]
    G --> H[保存重放原因及原错误]
    H --> I[同事务提交]
```

## 事件生产及启动流程

### RecordPost 与 RecordFollow

```mermaid
flowchart TD
    A[Post.Create或Post.Delete] --> B[RecordPost编码post_id]
    B --> C[在调用方事务插入Outbox]
    D[Follow或Unfollow] --> E[writeRelationship获取关系ID]
    E --> F[RecordFollow编码双方ID与relation_id]
    F --> C
    C --> G[调用方提交业务变更和事件]
```

取关流程使用 `SELECT id ... FOR UPDATE` 取得将删除的关系身份，再按 ID 删除并写入事件。无关系时提交空变更，重复关注沿用唯一键的幂等结果。既有1213死锁重试仍覆盖整个事务。

### Config.Load 与 App.Run

```mermaid
flowchart TD
    A[Config.Load] --> B[读取Redis URL并检查MQ配置]
    B --> C[App.Run连接MySQL并组装HTTP]
    C --> D[启动Outbox发布器]
    D --> E{配置Redis URL}
    E -->|是| F[New创建Feed服务并启动外部消费者]
    E -->|否| G[提供SQL基线接口]
    F --> H[服务HTTP请求]
    G --> H
    H --> I[退出信号触发context取消]
    I --> J[HTTP关闭 等待消费者结束 关闭Redis及MySQL]
```

## 验证范围与资源预算

用户已确认新增Redis 1 CPU／256 MiB，maxmemory=128 MiB、noeviction。Go GOMAXPROCS=2，测试进程合计 RSS≤2 GiB，MySQL RSS≤4 GiB，RabbitMQ 2 CPU／768 MiB，HTTP 并发≤4，新增数据、镜像、证据≤2 GiB，总磁盘≤100 GiB，单轮≤30分钟。

验证使用随机数据库、随机队列前缀和 Redis 命名空间，结束时逐项清理。Redis停机测试操作本项目Compose Redis，保留数据卷，停机至少60秒。两组外部故障窗口各20轮，测试通过提前设置持久领取到期时间触发接管，避免每轮等待30秒。生产到期时长固定为30秒。

```bash
GOMAXPROCS=2 GOMEMLIMIT=512MiB go test -p 2 -parallel 1 ./src/...
NOTEX_CONSUMER_INTEGRATION=1 GOMAXPROCS=2 GOMEMLIMIT=512MiB \
  go test -race -p 1 -count=1 -timeout=3m -run TestConsumerIntegration -v ./src/internal/outbox
NOTEX_FEED_INTEGRATION=1 NOTEX_REDIS_OUTAGE=1 GOMAXPROCS=2 GOMEMLIMIT=512MiB \
  go test -race -p 1 -count=1 -timeout=6m -run TestAuthorIndexIntegration -v ./src/internal/feed
GOMAXPROCS=2 GOMEMLIMIT=512MiB go build -p 2 -o /tmp/notex-stage3-api ./src
python3 scripts/verify_api.py --binary /tmp/notex-stage3-api --concurrency 4
```

阶段三覆盖Worker退出、连接故障、Redis保留数据的停机恢复。Redis数据丢失后的整体重建与缓存代次属于Sprint 4。AOF everysec存在最近写入的持久化窗口，测试报告按实际故障类型解释恢复结果。

## 实测记录

最终通过运行位于 [2026-11-16-stage3-04](runs/2026-11-16-stage3-04/report.json)，整轮82.08秒。报告保存基线commit、完整diff、源码SHA256、未跟踪源码快照、依赖版本和运行命令。运行完成后当前源码与记录的SHA256全部相同。

| 验证项 | 实测结果 |
|---|---|
| `go test ./src/...` | 通过，独立启用集成测试见下两行 |
| Outbox消费者集成与race检测 | 14个子测试通过，包含旧执行者回写隔离、5次重试、waiting重放和最终领取过期 |
| Feed集成与race检测 | 6个子测试通过 |
| 删除后迟到添加 | 20轮，无帖子复活 |
| Redis成功后取消Worker，尚未完成SQL写回及ACK | 20轮，重新领取后完成且成员唯一 |
| Redis成功后SQL完成写回失败 | 20轮，恢复后效果完整 |
| 作者分页 | 同秒、重复、大于2^53和最大int64 ID通过，作者1的41条有效帖完整分页与独立SQL查询完全一致 |
| 正常HTTP入口 | 发帖进入作者索引、删除移除成员、关系任务进入waiting |
| Redis停机恢复 | 停机至少60秒，HTTP发帖201、readyz 200，任务5次失败后可查，启动Redis并通过CLI重放后第6次完成且有审计记录 |
| 既有HTTP功能回归 | 27组通过，并发4，包含关系生命周期ID和事件基数核对 |
| 清理核对 | 隔离数据库、队列、Redis键及测试进程全部清理，Compose Redis和RabbitMQ运行中 |

停机测试日志中的64.42秒覆盖停机开始至重放完成，包含Redis启动和重放耗时。单次HTTP请求开始到观测作者索引可见为129.504毫秒，观测间隔20毫秒。这是本轮功能样本，正式延迟分位数由后续压测批次采集。

作者1的41条有效帖，ZSET的 `MEMORY USAGE` 为2144字节，一条删除标记对应SET为112字节。这是当前小集合编码下的样本，容量扩展时需要重新测量结构开销。

| 资源 | 本轮观测 |
|---|---:|
| 测试及其子进程合计RSS峰值 | 140.50 MiB |
| MySQL RSS峰值 | 193.39 MiB，取总采样和HTTP脚本两者最大值 |
| RabbitMQ容器内存采样峰值 | 128.90 MiB |
| Redis容器内存采样峰值 | 10.25 MiB |
| MySQL目录运行期间增量峰值 | 1264 KiB |
| MySQL目录结束增量 | 0 KiB |
| 新增Redis镜像大小 | 16927338字节 |

RSS每2秒采样，容器内存约每10秒采样，表中为采样峰值。Redis的1 CPU／256 MiB与RabbitMQ的2 CPU／768 MiB由Compose限制。整个fanout运行证据目录约16.12 MB，仓库占用约86.25 MiB，MySQL目录约521.86 MiB。Docker报告镜像7.182 GB、容器0.660 GB、数据卷3.447 GB，连同上述目录的账面空间约12 GB，处于本轮预算内。

第一轮通过后增加了全量分页和成本样本。第二、三轮暴露故障注入测试的判定问题，触发器会误拦已完成消息重复入库，瞬时消费者数量也无法稳定证明写回失败。最终将触发器限定为 `pending → completed`，直接等待消费者报告注入错误，同时补齐测试失败时的Worker清理。失败证据保留在stage3-02和stage3-03目录。

原始结果见 [Feed日志](runs/2026-11-16-stage3-04/feed.log)、[消费者日志](runs/2026-11-16-stage3-04/consumer.log)、[HTTP报告](runs/2026-11-16-stage3-04/api-report.json)、[资源采样](runs/2026-11-16-stage3-04/resources.jsonl) 和 [清理核对](runs/2026-11-16-stage3-04/cleanup.json)。

本批启用了消费者与Feed集成测试。发布器的独立完整故障套件保留为显式启用项，本批通过真实API发布路径验证了发布器与Feed消费者的正常交接。三策略性能对照、读者索引与Planner按后续阶段继续推进。

## 正常路径成本口径

以下按当前代码的数据库调用点计数，实际 SQL 协议还包含驱动 PREPARE／EXECUTE／DEALLOCATE，以及事务控制命令。

| 根事件处理步骤 | SELECT | 修改语句 |
|---|---:|---:|
| receive 保存任务并读取原消息 | 1 | 1 INSERT |
| claimExternal 读取并领取任务 | 1 | 1 UPDATE |
| Handle 回读权威帖子 | 1 | 0 |
| processExternal 条件保存结果 | 0 | 1 UPDATE |
| 合计 | 3 | 3 |

正常首次执行涉及任务接收的自动提交、领取事务提交及结果自动提交，共3次写事务提交。每批 pending 扫描额外分摊一次查询。重复消息及领取未到期的任务按各自分支产生不同的语句数。

Redis 在脚本已加载时执行一次 EVALSHA。有效帖添加路径在脚本内执行 SISMEMBER 和 ZADD，删除路径执行 SADD 和 ZREM。删除标记拒绝迟到添加时只执行 SISMEMBER。脚本缓存未命中时客户端追加 EVAL，首次连接还存在协议初始化开销。

根事件的 SQL 成功记录在本阶段保留，分发任务粒度及 SQL 记账成本在 Planner 阶段结合分块数衡量。作者索引中每条有效帖保存一份41字节排序成员，Redis 实际结构开销通过 MEMORY USAGE 记录。
