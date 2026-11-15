# NoteX
用 Go 实现的小型推特系统。

## 当前实现

Sprint 1 已于 2026-11-14 经用户确认完成，交付记录与后续验收待办见 [Sprint 1](docs/sprints/1-基础闭环和权威数据查询.md)。

已编写 HTTP 启动骨架、注册登录、会话认证、准确用户名查找、关注／取关、关系判断和双向游标列表。
项目采用纯后端交付方式，已接入发帖、详情、软删除、个人帖子列表和 SQL 关注 Feed。
账号、关系、帖子和 Feed 接口已通过本机 MySQL 的脚本功能验证。

## 本地启动

需要 Go 1.26.1 或更高版本，以及已有数据库和账号的 MySQL 实例。
设计目标版本为 MySQL 8.0，本轮实际验证环境为 Homebrew 托管的 MySQL 9.6.0，隔离级别为 REPEATABLE-READ。
MySQL 8.0 的运行兼容性仍待实际验证。
在已有的 NoteX 数据库中执行一次初始化迁移，命令中的账号需要具备建表权限。

```bash
mysql --host=127.0.0.1 --port=3306 --user=notex -p notex < migrations/0001_accounts_follow.sql
mysql --host=127.0.0.1 --port=3306 --user=notex -p notex < migrations/0002_posts.sql
mysql --host=127.0.0.1 --port=3306 --user=notex -p notex < migrations/0003_post_requests.sql
mysql --host=127.0.0.1 --port=3306 --user=notex -p notex < migrations/0004_outbox_events.sql
```

第一份迁移创建 `users`、`sessions`、`following` 三张表，第二份迁移创建 `posts` 表。
第三份迁移创建 `post_requests` 表，保存发布请求标识和首次响应。发布接口要求携带 `Idempotency-Key`。
第四份迁移创建 `outbox_events` 表，升级后的发布、删除和关系变更会在业务事务内记录事件。
帖子建表语句同时创建个人列表索引和有效帖全局时间索引。
新数据库按编号依次执行，已有数据库从尚未执行的迁移继续。
已有 Paopao 的 `p_*` 表和账号需要另行迁移。本 SQL 由维护者手工执行，成功后应记录执行版本。
MySQL DDL 会隐式提交，执行中断后应检查已创建的表，再从失败语句继续，避免整份重复执行。

在仓库根目录设置连接信息，替换示例中的用户名、密码、端口和数据库名。

```bash
export MYSQL_DSN='notex:replace-with-your-password@tcp(127.0.0.1:3306)/notex'
export HTTP_ADDR='127.0.0.1:8080'
go run ./src
```

`MYSQL_DSN` 必填，`HTTP_ADDR` 默认是 `127.0.0.1:8080`。当前直接读取环境变量，不自动加载 `.env` 文件。
启动时连接数据库失败会退出；数据库连接成功后才监听 HTTP。不要把真实连接凭据提交到仓库。

目前可以手工检查这两个接口：

```bash
curl -i http://127.0.0.1:8080/healthz
curl -i http://127.0.0.1:8080/readyz
```

| 接口 | 含义 | 响应 |
|---|---|---|
| `/healthz` | HTTP 进程存活，不查询数据库 | `200 {"status":"ok"}` |
| `/readyz` | 数据库连通性检查，最多等待 1 秒 | 可连接时返回 `200 {"status":"ready"}`，否则返回 `503 {"status":"unavailable"}` |

就绪检查只验证数据库可连接，不代表表结构或业务功能已验收。
按 Ctrl+C 或收到 SIGTERM 后，应用停止接收新请求，最多等待 10 秒让已有 HTTP 请求结束，再关闭数据库连接池。

当前连接池上限为 10，最大空闲连接数为 5；连接最长使用 3 分钟，最长空闲 1 分钟。
数据库连接、读、写超时分别为 5 秒。这些是启动默认值，不是经过性能实验确定的参数。

## 代码阅读顺序

```text
src/main.go                         读取配置，监听退出信号
src/internal/app/app.go             组装连接池与路由，管理启动和关闭
src/internal/config/config.go       环境配置及必要校验
src/internal/platform/mysql/mysql.go 建立共享数据库连接池
src/internal/httpapi/router.go      注册 HTTP 路由
```

当前 SQL Feed 与个人帖子列表在 `post` 中共用分页实现，后续分发策略随需求逐步接入。
HTTP 使用 Gin，当前账号和关系操作使用共享的 `database/sql` 连接池执行参数化 SQL。
密码保护沿用参考项目使用的 bcrypt 算法，调用已有 `golang.org/x/crypto/bcrypt`。
当前模块按 NoteX 需求独立编写，Paopao 作为源码参照。

## 账号与关系接口

完整接口契约、请求与响应示例、错误码和验证命令见 [接口文档](docs/interfaces.md)。

所有业务路径以 `/api/v1` 开头。注册与登录公开，其余接口要求 `Authorization: Bearer <token>`。

| 方法 | 路径 | 请求与结果 |
|---|---|---|
| POST | `/register` | JSON `username`、`password`，成功返回 201 和用户信息 |
| POST | `/login` | 同上，成功返回用户、`token`、`expires_on` |
| POST | `/logout` | 删除当前令牌对应的会话，返回 200 |
| GET | `/me` | 当前登录用户 |
| GET | `/users?username=alice` | 按大小写敏感的准确用户名查询 |
| PUT | `/users/:id/follow` | 当前用户关注目标用户，重复请求返回 200 |
| DELETE | `/users/:id/follow` | 当前用户取关目标用户，关系已不存在时返回 200 |
| GET | `/users/:id/follow` | 当前用户是否关注目标用户，返回 `is_following` |
| GET | `/users/:id/following` | 目标用户关注了谁 |
| GET | `/users/:id/followers` | 谁关注了目标用户 |

用户名允许 3～32 个 ASCII 字母、数字或下划线，大小写敏感。
密码按 UTF-8 字节数限制为 8～72 字节，避免超出 bcrypt 的输入长度上限。
注册成功后调用登录获取令牌。令牌由 32 字节随机数生成，数据库仅保存 SHA-256 摘要。
会话有效期固定为 24 小时，可跨服务重启使用。退出立即删除当前会话，过期令牌返回 401。
过期会话行可由维护者定期执行 `DELETE FROM sessions WHERE expires_on <= UNIX_TIMESTAMP()` 清理。

列表默认每页 50 条，`limit` 允许 1～100，使用 `cursor` 继续读取。
响应包含 `items`、`next_cursor`、`has_more`。每个 item 的 `id` 是关系 ID，
`user_id` 是列表中另一位用户的 ID，`created_on` 是关系建立时间，采用 Unix 秒。
游标使用 URL 安全的 Base64 编码，携带列表用户、方向和排序边界，客户端原样回传。
服务端校验游标归属与数值，身份验证仍由会话完成。

```bash
curl -i -X POST http://127.0.0.1:8080/api/v1/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"local-demo-password"}'

curl -i -X POST http://127.0.0.1:8080/api/v1/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"local-demo-password"}'

# 创建另一个用户后，将登录结果中的 token 和目标用户 ID 填入变量。
TOKEN='replace-with-login-token'
TARGET_ID='replace-with-target-user-id'

curl -i -X PUT "http://127.0.0.1:8080/api/v1/users/$TARGET_ID/follow" \
  -H "Authorization: Bearer $TOKEN"

curl -i "http://127.0.0.1:8080/api/v1/users/$TARGET_ID/followers?limit=50" \
  -H "Authorization: Bearer $TOKEN"
```

HTTP 错误响应统一为 `{"error":"错误代码"}`。参数或游标错误返回 400，认证失败返回 401，
用户不存在返回 404，用户名已占用返回 409，未知内部错误返回 500，请求超时返回 503。
关系写入遇到 MySQL 1213 死锁错误时，最多尝试 5 次，重试耗尽返回 `503 relationship_busy`。

## 实现取舍

关系表以联合唯一索引保证幂等关注，取关物理删除，重新关注产生新关系 ID。
外键保证关系两端的用户存在。两个方向分别使用时间与 ID 倒序索引，查询多取一条判断下一页。
本轮直接写 SQL，使唯一性错误处理和游标范围条件便于审核。
数据库会话支持撤销登录态，每次受保护请求需要执行一次认证查询。
账号、关注与会话操作均使用主库，异步事件与缓存将在后续 Sprint 接入。
`GET /api/v1/feed` 从主库读取当前关注作者和自己的有效帖子，按发布时间与 ID 倒序分页。
关注与删除过滤在 SQL 中完成，默认每页 20 条，上限 100 条。

已提供健康检查的单元测试，数据库故障通过函数参数模拟，不需要 MySQL 实例。

## 脚本验证

2026-11-14 已通过 Go 编译、现有 Go 测试及本机 MySQL 接口验证。
帖子与 Feed 接入后，完整脚本连续两轮通过全部 19 组检查，执行记录见接口文档。
脚本覆盖注册登录、会话撤销与过期、双向游标分页、并发幂等、混合关注／取关、
帖子发布与删除、SQL Feed 的筛选与分页、应用重启与数据库查询故障。
验证范围为小规模功能正确性，性能指标留待后续实验。

```bash
GOMAXPROCS=2 GOMEMLIMIT=768MiB go test -p 2 -parallel 1 ./src/...
GOMAXPROCS=2 GOMEMLIMIT=768MiB go build -p 2 -o /tmp/notex-api-verify ./src
python3 scripts/verify_api.py --binary /tmp/notex-api-verify
```

脚本创建随机命名的独立数据库与受限业务账号，结束后自动清理，JSON 报告记录结果及资源采样。
管理连接默认使用本机 `127.0.0.1:3306` 的 `root` 空密码，连接配置方式见接口文档。
