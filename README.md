# NoteX
用 Go 实现的小型推特系统。

## 当前实现

目前已编写 HTTP 启动骨架：环境配置、共享 MySQL 连接池、健康检查和优雅关闭。
账号、帖子、关注关系及前端尚未接入。此阶段没有自动建表，也不自动启动依赖服务。

## 本地启动

需要 Go 1.26.1 或更高版本，以及已有数据库和账号的 MySQL 8.0 实例。
在仓库根目录设置连接信息（替换示例中的用户名、密码、端口和数据库名）：

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

后续业务模块沿用已讨论的 `account`、`post`、`follow`、`feed` 划分，随实现逐步添加。
HTTP 使用 Gin；本轮数据库仅建立 `database/sql` 连接池，关系查询实现时再接入 GORM。

已提供健康检查的单元测试，数据库故障通过函数参数模拟，不需要 MySQL 实例。
本轮按约定未运行编译、测试、服务或压力实验，实际联调结果待记录。
