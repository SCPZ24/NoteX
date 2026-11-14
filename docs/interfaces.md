# NoteX HTTP 接口

本文描述当前已实现的账号、会话与关注关系接口。请求示例使用本机默认地址 `http://127.0.0.1:8080`。
项目采用纯后端交付方式，接口通过脚本验证。

## 通用约定

- 业务接口前缀为 `/api/v1`，健康检查位于根路径。
- 注册、登录使用 JSON 请求体，并设置 `Content-Type: application/json`。
- 业务请求体上限为 4096 字节。超过限制或 JSON 解析失败时，当前实现返回 `400 invalid_input`。
- 业务请求设置 5 秒 Context 超时，数据库操作继承此超时。
- 除注册、登录及健康检查外，所有接口要求 `Authorization: Bearer <token>`。
- ID 是正整数。时间字段均为 Unix 秒。
- 成功响应直接返回对象，错误响应统一为 `{"error":"错误代码"}`。
- 登录响应及通过认证的业务响应设置 `Cache-Control: no-store`。

### 对象定义

用户对象如下。

```json
{"id":42,"username":"alice","created_on":1794618000}
```

关系列表中的对象如下。

```json
{"id":105,"user_id":8,"username":"bob","created_on":1794618100}
```

这里的 `id` 是关系 ID，`user_id` 是列表中另一位用户的 ID，`created_on` 是关系建立时间。
关注列表展示被关注者，粉丝列表展示关注者。

## 接口清单

| 方法 | 路径 | 认证 | 成功状态 |
|---|---|---|---|
| GET | `/healthz` | 公开 | 200 |
| GET | `/readyz` | 公开 | 200 |
| POST | `/api/v1/register` | 公开 | 201 |
| POST | `/api/v1/login` | 公开 | 200 |
| POST | `/api/v1/logout` | Bearer | 200 |
| GET | `/api/v1/me` | Bearer | 200 |
| GET | `/api/v1/users?username=alice` | Bearer | 200 |
| PUT | `/api/v1/users/:id/follow` | Bearer | 200 |
| DELETE | `/api/v1/users/:id/follow` | Bearer | 200 |
| GET | `/api/v1/users/:id/follow` | Bearer | 200 |
| GET | `/api/v1/users/:id/following` | Bearer | 200 |
| GET | `/api/v1/users/:id/followers` | Bearer | 200 |

## 健康检查

`GET /healthz` 检查 HTTP 进程存活，返回 `200 {"status":"ok"}`。

`GET /readyz` 在 1 秒期限内检查 MySQL 连接，可连接时返回 `200 {"status":"ready"}`，
失败时返回 `503 {"status":"unavailable"}`。该接口检查连接状态，业务表完整性由迁移与接口验证确认。

## 注册与登录

### POST /api/v1/register

```json
{"username":"alice","password":"local-demo-password"}
```

用户名允许 3～32 个 ASCII 字母、数字或下划线，大小写敏感。密码长度为 8～72 个 UTF-8 字节。
服务端保留输入原值。成功返回 201 和用户对象，随后可调用登录接口。

参数不合法返回 `400 invalid_input`，用户名重复返回 `409 username_taken`。
数据库保存 bcrypt 密码哈希。

### POST /api/v1/login

请求体与注册相同。成功响应如下。

```json
{
  "user":{"id":42,"username":"alice","created_on":1794618000},
  "token":"64个十六进制字符组成的随机令牌",
  "expires_on":1794704400
}
```

令牌有效期为 24 小时，数据库保存其 SHA-256 摘要及用户关联。会话持久化后可跨应用重启使用。
每次登录产生独立令牌。用户不存在、密码错误或凭据格式不符合登录要求时返回 `401 unauthorized`。
缺失必填字段或 JSON 解析失败返回 `400 invalid_input`。

### POST /api/v1/logout

携带当前 Bearer 令牌，请求体可为空。服务端删除当前会话，返回 `200 {"status":"ok"}`。
其他登录令牌继续有效。再次使用已撤销令牌请求任何受保护接口，包括退出接口，会返回 `401 unauthorized`。

### GET /api/v1/me

返回当前令牌对应的用户对象。

## 查找用户

### GET /api/v1/users?username=alice

`username` 必填，按大小写敏感的准确用户名匹配。成功返回用户对象。
输入格式不合法返回 `400 invalid_input`，符合格式但不存在的用户名返回 `404 user_not_found`。

## 关注、取关及关系判断

路径中的 `:id` 指目标用户，操作发起者由服务端认证结果确定。

### PUT /api/v1/users/:id/follow

请求体可为空。成功返回如下结果。

```json
{"is_following":true}
```

首次关注插入关系。重复关注返回 200，保留原关系 ID 和建立时间。
不合法 ID 或关注自己返回 `400 invalid_input`，目标用户不存在返回 `404 user_not_found`。
唯一约束保证同一用户对最多一条有效关系。

### DELETE /api/v1/users/:id/follow

请求体可为空。成功返回 `200 {"is_following":false}`。
取关物理删除关系，删除零行仍返回成功，因此重复取关及对不存在的正整数目标 ID 取关都返回 200。
不合法 ID 或对自己取关返回 `400 invalid_input`。重新关注产生新的关系 ID，并记录本次建立时间。

### GET /api/v1/users/:id/follow

返回 `200 {"is_following":true}` 或 `200 {"is_following":false}`。
目标用户不存在返回 `404 user_not_found`，不合法 ID 返回 `400 invalid_input`。
查询自己通常返回 `false`，写接口禁止自关注。

并发关注与取关按数据库实际执行情况收敛。旧关注请求在一次取关之后重新执行，会再次建立关系。
当前接口使用状态设置语义，操作版本与请求标识去重属于后续需求决策。
关注和取关的单语句写入遇到 MySQL 1213 死锁错误时，最多尝试 5 次，等待间隔为 5、10、20、40 毫秒。
等待响应请求取消，重试耗尽返回 `503 relationship_busy`。

## 关注列表与粉丝列表

- `GET /api/v1/users/:id/following` 查询该用户关注了谁。
- `GET /api/v1/users/:id/followers` 查询谁关注了该用户。

所有已登录用户均可查询其他用户的关系列表。

| 查询参数 | 类型 | 规则 |
|---|---|---|
| `limit` | 整数 | 默认 50，允许 1～100，省略或空字符串采用默认值 |
| `cursor` | 字符串 | 首次省略，后续原样传回上一页的 `next_cursor`，最长 512 字符 |

响应示例如下。

```json
{
  "items":[
    {"id":108,"user_id":8,"username":"bob","created_on":1005},
    {"id":105,"user_id":15,"username":"carol","created_on":1003}
  ],
  "next_cursor":"eyJvd25lciI6NDIsImRpcmVjdGlvbiI6ImZvbGxvd2luZyIsImNyZWF0ZWRfb24iOjEwMDMsImlkIjoxMDV9",
  "has_more":true
}
```

该示例属于用户 42 的关注列表，页大小为 2。结果按关系建立时间倒序，同时间按关系 ID 倒序。
查询最多读取 `limit + 1` 条，用额外一条判断 `has_more`，游标基于实际返回的末尾记录。
结束时返回空 `next_cursor` 和 `has_more=false`，空列表的 `items` 为 `[]`。

游标是 JSON 的 Base64 URL 编码，包含 `owner`、`direction`、`created_on`、`id`。
服务端检查列表归属和正数边界。游标边界记录被删除后仍可继续读取。
固定数据下分页应无重复、无遗漏。连续翻页期间新增到边界之前的关系通过刷新获取。
`has_more` 反映当前查询时的状态，多次请求之间的关系变化可能使后续页面为空。

游标格式或归属错误返回 `400 invalid_cursor`，非法页大小返回 `400 invalid_input`，
列表所属用户不存在返回 `404 user_not_found`。

## 错误响应

| HTTP 状态 | `error` | 含义 |
|---|---|---|
| 400 | `invalid_input` | 参数、JSON 请求体或业务输入不合法 |
| 400 | `invalid_cursor` | 游标无法解析，归属不匹配或边界值不合法 |
| 401 | `unauthorized` | 凭据错误，Bearer 缺失，令牌无效、过期或已撤销 |
| 404 | `user_not_found` | 查找或操作要求存在的用户不存在 |
| 409 | `username_taken` | 用户名唯一约束冲突 |
| 500 | `internal_error` | 未知数据库或内部错误，详细错误记入服务端日志 |
| 503 | `request_unavailable` | 业务请求的 Context 已超时或取消 |
| 503 | `relationship_busy` | 关系写入死锁重试已耗尽，可稍后重试 |

这些响应适用于上述业务处理函数。未注册的路径使用 Gin 默认 404 响应。

## 脚本验证

先构建 API，然后运行验证脚本。脚本使用现有 MySQL 服务，创建独占的随机数据库和受限账号，
执行迁移、启动 API、验证接口，结束后关闭进程并删除该测试数据库与账号。

```bash
GOMAXPROCS=2 GOMEMLIMIT=768MiB go build -p 2 -o /tmp/notex-api-verify ./src
python3 scripts/verify_api.py --binary /tmp/notex-api-verify
```

管理连接通过 `TEST_MYSQL_HOST`、`TEST_MYSQL_PORT`、`TEST_MYSQL_USER`、`TEST_MYSQL_PASSWORD` 配置，
默认分别为 `127.0.0.1`、`3306`、`root` 和空密码。管理账号需要创建及删除测试数据库和账号的权限。
API 本身使用测试账号，其权限限定为测试库中的 SELECT、INSERT、UPDATE、DELETE。

报告默认输出到 `/tmp/notex-api-verification.json`，可通过 `--report` 指定。
报告记录 MySQL 版本、各验证组结果、采样 RSS、表空间估计和资源清理结果。
密码和登录令牌保留在进程内存中，报告记录验证结果。

本轮约定 Go 编译并行度最多 2，客户端请求并发最多 8，新增进程内存预算 2 GiB，新增磁盘预算 1 GiB。
脚本采样新增进程 RSS，超过内存预算会停止测试 API。`GOMEMLIMIT` 控制 Go 运行时的软内存目标。
已运行的 MySQL 单独记录 RSS，保留其现有全局设置。
该脚本覆盖小规模功能及并发正确性，C/Q 集和正式性能场景仍按 Sprint 文档执行。

### 本轮执行记录

2026-11-14 在本机 MySQL 9.6.0、REPEATABLE-READ 隔离级别下执行。
Go 编译及现有 Go 测试通过，修复后连续三轮接口验证均通过全部 11 组检查。
每轮使用 7 个测试用户，请求最大并发为 8，覆盖以下行为。

- 注册校验、用户名唯一性、bcrypt 存储、登录认证及准确用户名查询。
- 重复关注保留关系 ID 和建立时间，重复取关成功，重新关注产生新 ID。
- 非对称关注图的双向列表、同秒记录的分页排序、空列表及游标归属校验。
- 游标边界记录删除后的续页、翻页期间新增关系后的刷新查询。
- 混合关注／取关并发后的唯一性，以及数据库、关系判断和双向列表的一致性。
- 应用正常关闭、重启后的会话恢复、单会话退出及过期认证。
- 测试库关系表临时不可用时返回 `500 internal_error`。

初次验证复现了混合关注／取关引起的 MySQL 1213 死锁，并导致 HTTP 500。
关系写入加入上述有界重试后，三轮均通过。日志中的 1146 表不存在错误来自最后一组主动故障注入。

三轮脚本及子进程的采样 RSS 峰值最高约 60.3 MiB，既有 MySQL 进程采样峰值最高约 139.2 MiB。
每轮测试表数据与索引空间估计为 144 KiB，验证二进制占用约 12.3 MiB。
运行后 Go 构建缓存总量约 757.2 MiB，包含此前已有缓存。RSS 采样范围为接口脚本及其子进程，编译期采用前述并行度与 Go 软内存目标。
三轮临时数据库、账号及 API 进程均已清理。
MySQL 8.0 的实际运行兼容性、C/Q 集和性能指标仍待验证。
