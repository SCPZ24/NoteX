# Sprint 3 第一步基线

## 范围与版本

本轮基线提交为 `a99a35e1b2428a56d12307f776f64e037da0a1d5`。业务实现、数据库迁移和 API 契约沿用该提交。本批增加独立测量脚本与本目录记录。

本机为 M4 Pro／24 GiB，Go 1.26.1、Python 3.13.7、Homebrew MySQL 9.6.0，事务隔离为 REPEATABLE-READ，InnoDB buffer pool 为 128 MiB。现有 Compose RabbitMQ 为 4.2.9，配置上限 2 CPU／768 MiB。SQL Feed 基线直接读取 MySQL。

2026-11-16 用户确认本批预算如下。

| 项目 | 上限与执行方式 |
|---|---|
| Go | 每进程 GOMAXPROCS=2，GOMEMLIMIT=512MiB |
| API、Dummy、脚本及子进程 | 合计采样 RSS 2 GiB |
| MySQL | 采样 RSS 4 GiB，沿用现有服务配置 |
| RabbitMQ | 现有 2 CPU／768 MiB 容器 |
| 数据库业务连接 | 测试账号 MAX_USER_CONNECTIONS=9，另为顺序管理 CLI 预留 1 个连接 |
| HTTP | 本轮顺序核对，并发上限约定为 4 |
| 新增数据及证据 | 2 GiB，整个项目磁盘总预算 100 GiB |
| 时间 | C/Q 分开运行，每轮 30 分钟 |

RSS 和测试库文件大小每 2 秒采样，超额时停止测试进程并清理隔离数据。GOMEMLIMIT 是 Go 软目标。MySQL 沿用共享服务，RSS 包含该实例其他业务。CPU 以 Go 调度并行数和容器配置限制，主机 MySQL CPU 在本轮采用观测口径。

运行前 Docker 全部镜像约 7.122 GB、容器可写层 660.2 MB、卷 3.447 GB，MySQL 数据目录约 407.9 MiB，仓库约 66.7 MiB，两个临时二进制约 21.4 MiB。以上含其他项目的 Docker 对象，合计低于 100 GiB。测试库与队列采用随机名称，结束后清理。RabbitMQ 使用量约每 10 秒采样，其样本时间单独记录；主机 ps CPU 百分比与 Docker CPU 百分比保留原始口径。

## 可重现命令

在仓库根目录执行。管理连接复用 `scripts/verify_api.py` 的 TEST_MYSQL_HOST、TEST_MYSQL_PORT、TEST_MYSQL_USER、TEST_MYSQL_PASSWORD 环境变量，默认本机 3306、root、空密码。业务账号由脚本随机创建。报告目录必须尚不存在。

```bash
GOMAXPROCS=2 GOMEMLIMIT=512MiB go build -p 2 -o /tmp/notex-fanout-api ./src
GOMAXPROCS=2 GOMEMLIMIT=512MiB go build -p 2 -o /tmp/notex-fanout-events ./src/cmd/events
GOMAXPROCS=2 GOMEMLIMIT=512MiB go test -p 2 -parallel 1 ./src/...

python3 scripts/fanout_baseline.py --dataset C \
  --binary /tmp/notex-fanout-api --events-binary /tmp/notex-fanout-events \
  --output notes/fanout/runs/2026-11-16-C3
python3 scripts/fanout_baseline.py --dataset Q \
  --binary /tmp/notex-fanout-api --events-binary /tmp/notex-fanout-events \
  --output notes/fanout/runs/2026-11-16-Q2
```

C 集会使用现有 RabbitMQ，默认地址为 `amqp://notex:notex-local@127.0.0.1:5673/`，可通过 TEST_RABBITMQ_URL 设置。自动清理队列使用本项目 Compose 的 rabbitmq 服务，使用远程 MQ 时需要相应调整清理入口。

现有 API、发布器、Dummy 和失败处置入口如下。MYSQL_DSN、RABBITMQ_URL、RABBITMQ_QUEUE_PREFIX 需要指向同一份已完成五次迁移的实验环境。

```bash
# API；RABBITMQ_URL 非空时，同时启动 Outbox 发布器。
go run ./src
# 独立启动 Feed 和 Search 的 SQL Dummy；前缀必须以 notex_dummy_ 开头。
go run ./src/cmd/events dummy
# 查询失败和积压。
go run ./src/cmd/events list --scope outbox --status pending
go run ./src/cmd/events list --scope consumer --status failed
# RECORD_ID 使用上一步查到的消费记录主键。
go run ./src/cmd/events replay --scope consumer --id "$RECORD_ID" --reason '依赖恢复后重放'
```

现有 Outbox 发布故障集成测试仍使用 `NOTEX_INTEGRATION=1`，它包含共享 Broker 停机 60 秒场景，单独安排。消费集成测试通过 `NOTEX_CONSUMER_INTEGRATION=1` 启用。普通 `go test` 默认跳过这两组外部集成测试。

## 数据与独立参考

固定生成种子参数为 3101，时间基点为 1700000000。关系时间每 7 行重复，帖子时间每 11 行重复，覆盖同秒排序。每 17 条帖子中有一条软删除。

| 数据集 | 用户 | 有效关系 | 帖子总数 | HTTP 全分页范围 |
|---|---:|---:|---:|---|
| C | 200 | 5000 | 10000 | 所有 200 用户 Feed，以及每个用户的关注／粉丝列表 |
| Q | 120000 | 1000000 | 200000 | 100／500／1000 关注作者的三个读者 Feed，两个方向的 10 万级关系以及三个读者的关注列表 |

Q 的用户 1 关注 100100 人，用户 2 拥有 100101 个粉丝。用户 3、4、5 分别关注 100、500、1000 人。其余关系按固定规则补足，全部关系唯一、双方不同。

装载使用 1000 行一批的 SQL，避免对 12 万用户运行密码哈希。脚本创建测试会话，后续请求经过真实 Bearer 认证、中间件、HTTP handler 和 service。装载数据本身不生成历史 Outbox 事件。后续投影初始化需要对这类既有数据定义构建边界。

参考生成过程先逐行导出原始表并与固定夹具全量比较，然后在 Python 中做集合筛选与排序。它独立于服务端 SQL 的 EXISTS、JOIN 和游标条件。C 覆盖所有用户，Q 的 HTTP Feed 核对范围为表中三个典型读者，Q 原始关系／帖子逐行核对覆盖全部数据。

```mermaid
flowchart TD
    A[fixture 固定数据生成] --> B[setup 有界批量装载隔离库]
    B --> C[导出全部原始关系与帖子]
    C --> D[逐行校验装载结果]
    D --> E[reference_feed 集合筛选和 Python 排序]
    E --> F[verify_pages 经 HTTP 完整翻页]
    F --> G[逐页核对身份 正文 顺序 游标与 has_more]
```

## 发布路径的成本口径

首次正常发布的核心事务执行如下。

| 顺序 | 语句／操作 | SQL 执行数 | 逻辑修改 |
|---|---|---:|---|
| 事务外认证 | 查询 sessions JOIN users | 1 | 0 |
| 开始事务 | BEGIN | 1 | 0 |
| 占用幂等键 | INSERT post_requests | 1 | 插入 1 行 |
| 查询作者 | SELECT users | 1 | 0 |
| 保存帖子 | INSERT posts | 1 | 插入 1 行 |
| 保存响应 | UPDATE post_requests | 1 | 更新 1 行 |
| 保存异步意图 | INSERT outbox_events | 1 | 插入 1 行 |
| 提交 | COMMIT | 1 | 1 次业务事务提交 |

因此核心事务是 4 次 DML、1 次作者查询，HTTP 正常路径另有 1 次认证查询，以及 BEGIN／COMMIT。逻辑修改计 4 次，新增持久业务记录计 3 行。该口径来自当前代码调用点，驱动 prepare／execute 协议往返、索引页写入、redo 和 binlog 字节单独计算。

首次成功发布后，Outbox 发布器还会执行如下工作。

- 批量扫描 pending 事件，查询成本由同批事件共同承担。
- 对该事件更新 attempts，属于一次自动提交写事务。
- 向 feed、search 两个队列发布，各等待发布确认并检查不可路由返回。
- 更新 published_on，属于另一次自动提交写事务。

正常无重试路径共增加 2 次 Outbox 逻辑更新与 2 次自动提交。它们在业务事务提交后发生。失败路径会额外保存错误和 next_attempt_on，部分队列已确认时重试可能重复投递。

SQL Dummy 每个消费路径首次接收包含一条任务 INSERT 和一次原消息查询。处理事务锁定任务，创建 SAVEPOINT，插入 dummy_effects，更新任务状态并提交。Feed 和 Search 各有一份消费效果和任务记录。重试、空轮询和失败重放增加相应语句，报告中保留尝试次数。

当前 SQL Feed 每页正常请求包括认证、检查用户存在和 Feed 查询，共 3 次业务 SELECT。当前读者 Redis 索引逻辑写入数为 0。后续纯推／混合对照需同时报告尝试写入、唯一目标效果与 SQL 记账量。

## 索引、执行计划与空间

保留现有关系联合唯一索引 `uk_user_follow`、两个时间排序索引 `idx_user_created`／`idx_follow_created`，以及帖子两个有效状态／时间索引。基线按当前自然执行计划运行。

Q 两个方向在 0／1000／10000／100000 位置比较 OFFSET 与复合游标，页大小均为 50，并核对两者返回相同记录。执行计划包含实际耗时与处理行数，保存在 plans.json。游标边界取自已完整核对的前一行；取得这个边界本身有遍历成本。

`information_schema.tables` 提供表数据与全部二级索引估算，`mysql.innodb_index_stats` 提供每个索引分配页的空间估算。小批写入可能使用已分配页，因此页空间差值与单条逻辑负载长度含义不同。

写入探针使用两张隔离副本，装载至多 1 万行，C 集实际 5000 行、Q 集 10000 行，交换执行顺序共 3 轮。一张维持全部索引，另一张保留主键、关系唯一索引及反向外键所需的列索引。CREATE TABLE LIKE 不复制外键约束，两张探针表均以相同约束条件比较。真实关系表始终保留原索引与外键。时间包含一次 mysql CLI 建连与批量插入，不代表单条 HTTP 写延迟。

## 可见性、积压和推进

C 集先单独发布 1 条用于 SQL 计数，再依次从真实发布 API 发出 20 条帖子，并轮询其已有粉丝（用户 2）的 Feed，记录请求开始至第一次观察到帖子、HTTP 成功响应至第一次观察到帖子的时间。当前 SQL Feed 在事务提交后具备可见条件，观察时间包含轮询 HTTP 成本。

Outbox oldest age 按未发布事件的最早 created_on 计算，消费 oldest age 按 pending 任务计算，单位为秒。报告同时保留累计消费完成数和 Dummy 效果数。SQL Dummy 的完成表示 SQL 测试效果已提交。SQL pending 年龄覆盖 Outbox 未交接和消费落库等待两段。已经进入 MQ、尚未落入消费任务表的等待时间，需要在真实分发监控中补充端到端任务年龄。

本批通过隔离库中的故障触发器使一个帖子对应的两个 Dummy 消费都耗尽重试，然后移除触发器，用 events replay 命令恢复并检查重放审计和最终效果数量。

```mermaid
flowchart TD
    A[pipeline 启动发布器与 Dummy] --> B[HTTP 发布并轮询真实 Feed]
    B --> C[采样 Outbox 积压和 Dummy 完成数量]
    C --> D[故障触发器阻止 Dummy 写入]
    D --> E[等待重试耗尽并保留失败记录]
    E --> F[移除触发器并执行 CLI replay]
    F --> G[核对完成记录 效果数及重放审计]
    G --> H[cleanup 停进程并清理隔离库 账号 队列]
```

普通任务推进在基线记录为普通帖子对应的 Dummy 完成数量随时间变化。后续接入真实分发后，增加按普通／大作者／补历史任务分类的完成曲线，以及每帖目标读者索引可见性。混合负载下的公平性在真实 Planner 接入后验证。

## 实测结果

首轮 C/Q 已通过。首轮 C 索引探针的 rows 标签误记为 10000，实际插入 5000 行，修订后结果记录在 C2。首轮证据保留用于追溯，C2 补充了实测 SQL 计数及资源采样，C3 将可见性观察者固定为作者的已有粉丝。最终结果以 C3/Q2 为准。正式三策略性能实验仍需冻结到达率、成功率、尾延迟、可见性及恢复目标，执行至少三轮。当前脚本中的请求时延属于顺序正确性遍历与短摸底样本。

## 脚本函数阅读入口

脚本复用现有 verify_api.py 的隔离库、随机账号、mysql CLI、HTTP 请求和 finally 清理方式，独立入口放在 scripts/fanout_baseline.py。所有新增逻辑均属于基线工具。

```mermaid
flowchart TD
    A[main 解析 C或Q 二进制和输出目录] --> B[Baseline.__init__ 建立运行上下文]
    B --> C[command_metadata 保存版本 哈希与源码状态]
    C --> D[run 管理全流程与失败报告]
    D --> E[monitor 后台采样资源和预算]
    D --> F[setup 建库 迁移 账号 装载与原始核对]
    F --> G[launch 启动 API 并等待就绪]
    G --> H[verify 全分页比较]
    H --> I[plans 双向深分页与 Feed 执行计划]
    I --> J[write_cost 索引写入探针]
    J --> K[storage 表与索引空间]
    K --> L{C 数据集}
    L -->|是| M[publication_cost 隔离发布 SQL 计数]
    M --> N[pipeline 发布 可见性 消费 失败 重放]
    L -->|否| O[cleanup]
    N --> O
    O --> P[percentiles 汇总 HTTP 样本并保存 report]
    E -->|超预算| Q[停止测试进程并标记失败]
    Q --> O
```

```mermaid
flowchart TD
    A[fixture 固定关系和帖子] --> B[insert 每1000行提交一批]
    B --> C[sql 通过 mysql CLI 执行]
    C --> D[command 设置超时 检查退出码]
    E[digest 按行生成内容哈希] --> F[报告夹具与参考摘要]
    G[reference_feed 按关注集合筛选有效帖并排序] --> H[verify_pages 校验当前页与下一页语义]
    H --> I[request HTTP 调用并记录耗时和状态]
    I --> J[token 首次为读者建立测试会话]
    J --> C
    I --> K[check_budget 检查时间及监控错误]
    L[statement_counts 读取本库 SQL 摘要计数] --> C
    M[progress 读取积压年龄与累计效果] --> C
    N[stop_processes SIGTERM并等待退出] --> O[cleanup 删除本轮队列 数据库与账号]
```

`request` 走真实接口入口，失败同样进入原始 HTTP 样本。`verify_pages` 每页校验返回值与独立期望的切片，保证 next_cursor 推进，且 has_more 与剩余数量吻合。`plans` 先比较两条分页查询的结果，再记录实际执行计划。

`publication_cost` 在 MQ 发布器关闭期间测量一次发布，读取本库 performance_schema SQL 摘要计数差值。可变长度负载单独报告帖子正文、请求内容与键及 JSON 响应、Outbox JSON 的存储字节。表与索引页空间另由 storage 报告。

`cleanup` 分别尝试清理进程、队列、数据库和账号，任何失败都会令该轮失败并留下原因。`run` 在异常和 Ctrl+C 路径仍会执行清理和报告落盘。

### 发布成本的实测口径

C2 的 performance_schema 差值确认，单次成功发布有 6 次 EXECUTE，其中 2 次 SELECT、3 次 INSERT、1 次 UPDATE，另有 1 次 START TRANSACTION 和 1 次 COMMIT。驱动为这 6 条带参数语句分别执行 PREPARE 与 DEALLOCATE，所以该窗口记录了 20 个服务器命令事件。网络往返次数需要另行采样。

探针正文 `publication_cost_probe` 的 UTF-8 大小为 22 字节。请求记录中的正文、请求键与二进制 JSON 响应合计 174 字节，Outbox payload 的二进制 JSON 大小为 34 字节。这些是选定可变长度字段的大小，行头、固定字段、索引、事务日志及页空闲空间在其他口径中记录。

### 执行计划中的基线限制

Q 首轮测量发现以下执行路径，原始计划保存在该轮 plans.json。

- 100 关注作者的 Feed 首屏，为获得 51 个候选，扫描了 60618 条有效帖子。500 和 1000 关注作者分别扫描 11413 和 5986 条。这与当前全局时间索引扫描加逐条关系判断有关，也取决于生成数据中关注作者帖子的密度。
- 粉丝列表在 1000 和 10000 游标位置选用了用户表优先的连接计划，扫描 120000 用户，再筛选关系并排序。这两个位置出现约 160 ms 和 156 ms 的实际计划耗时。
- 两个方向的 100000 位置均可使用关系范围索引，只输出 50 行给用户表连接。相同位置的 OFFSET 需要让连接处理 100050 行。

正式策略对照前，需要确认关系查询的计划稳定性，并记录 SQL 读取是否进行过查询优化。后续收益报告同时注明这份原始基线与任何优化后的 SQL 基线，保证改进来源可追溯。

### Q2 读取摸底

每个读者顺序请求 30 次首屏，页大小 50，无并发发压。分位数采用 nearest-rank，30 个样本的 P99 等于本组最大值。

| 关注作者数 | 请求数 | P50 ms | P95 ms | P99 ms |
|---|---:|---:|---:|---:|
| 100 | 30 | 69.253 | 70.188 | 85.289 |
| 500 | 30 | 18.440 | 19.315 | 19.319 |
| 1000 | 30 | 11.825 | 12.850 | 12.875 |

当前 SQL 从全局有效帖子时间索引依次筛选关系，关注作者密度较低时需要扫描更多候选。上表受当前固定分布影响。正式推拉对照还需报告发帖分布、目标用户和每页候选处理量。

Q2 的百万关系表主键约 52.59 MiB，唯一关系索引约 42.62 MiB，粉丝时间索引约 53.69 MiB，关注时间索引约 49.66 MiB。两个时间索引合计约 103.34 MiB。

1 万行索引写入探针共三轮，全索引副本耗时中位数约 80.05 ms，简化索引副本约 68.42 ms。该差值包含 CLI 建连与批量事务，样本仅支持本轮摸底，生产表保有完整索引及外键。

[Q2 原始报告](runs/2026-11-16-Q2/report.json) · [执行计划](runs/2026-11-16-Q2/plans.json) · [逐请求样本](runs/2026-11-16-Q2/http.jsonl)

## 第一步完成记录

C3 与 Q2 均通过并完成隔离资源清理。C3 完整比较了 200 位读者的 244712 条 Feed 返回记录，以及全部用户双向关系分页。Q2 原始表逐行比较覆盖百万关系与 20 万帖，三个目标读者完整 Feed 共返回并核对 2516 条帖子，两个方向的十万级列表完成 HTTP 全分页。

| 轮次 | 总时长 | 测试进程峰值 RSS | MySQL 峰值 RSS | 合计采样峰值 | 测试库及证据采样峰值 |
|---|---:|---:|---:|---:|---:|
| C3 | 82.66 s | 97.08 MiB | 310.72 MiB | 517.66 MiB | 24.41 MiB |
| Q2 | 184.72 s | 639.66 MiB | 409.95 MiB | 1141.40 MiB | 289.80 MiB |

合计采样值为测试进程 RSS、MySQL RSS 和最近一次 RabbitMQ 容器使用量之和。三者存在采样时间差，共享内存与容器统计口径也有差异。整机 Docker VM 开销和其他项目进程属于共享环境，未分摊到 NoteX。RabbitMQ 使用量峰值约 128.7 MiB。运行前后 MySQL 整个数据目录由约 407.9 MiB 增至 521.9 MiB，包含共享实例日志及空闲文件。全部实验记录约 20.4 MiB，总磁盘预算内完成。

C3 的 20 次发布从 HTTP 请求开始到粉丝 Feed 首次观察到帖子的 P50／P95／P99 为 12.881／16.461／17.768 ms。成功响应到首次观察到帖子的对应时间为 6.156／7.155／9.310 ms，包含轮询请求耗时。此样本为顺序可见性摸底。

正常发布阶段的 21 个事件包括 1 个 SQL 计数探针和 20 个可见性探针，形成 42 个 SQL Dummy 效果。故障探针另产生 1 个事件，其两个消费任务各失败 5 次，显式重放后第 6 次成功，最终共 44 个效果。原始失败消息、失败原因和重放后尝试次数均保留，最终逐事件核对类型与 payload 一致。

### 普通 SQL Dummy 推进曲线

横轴为相对第一次推进采样的秒数，纵轴为已完成的消费任务数。每帖对应 Feed 和 Search 两个任务。采样穿插在顺序发布之间，能够验证该测量入口持续记录完成进度。

```mermaid
xychart-beta
    x-axis ["0.00", "0.16", "0.31", "0.46", "0.62", "0.76"]
    y-axis "完成任务数" 0 --> 42
    line [2, 10, 18, 26, 34, 42]
```

该批采样的 Outbox pending 与消费 pending 年龄均为 0 秒，当前短样本快速追平。真实混合负载的积压上限与普通任务公平性仍由后续 Planner 验收覆盖。

### 证据入口

- [C3 报告](runs/2026-11-16-C3/report.json)、[独立参考](runs/2026-11-16-C3/reference.jsonl)、[失败消息](runs/2026-11-16-C3/failed-jobs.json)、[资源采样](runs/2026-11-16-C3/resources.jsonl)
- [Q2 报告](runs/2026-11-16-Q2/report.json)、[执行计划](runs/2026-11-16-Q2/plans.json)、[资源采样](runs/2026-11-16-Q2/resources.jsonl)
- [现有 Go 测试输出](runs/2026-11-16-unit-tests.log)

最终检查确认本轮随机数据库、账号、队列及 API／Dummy 测试进程均已清理。Q2 的 runner.py 快照与最终脚本差异仅在 C 集可见性观察者，Q 分支执行逻辑一致。每轮保存脚本和二进制哈希、业务源码 diff、运行命令及工作区状态。

### 后续验收边界

本轮完成 3.1 的基线冻结、C/Q 约定范围核对、双向执行计划与深分页、索引成本、独立参考及测量入口。正式三策略性能对照、F 集写放大、真实 Redis 目标效果、混合任务公平性和规定轮数的外部效果故障实验随后续批次实施。MySQL 8.0 兼容性仍按总体规划验证。
