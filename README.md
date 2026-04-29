# ScheduleJob

`ScheduleJob` 是一个面向高并发预约/延时执行场景的分布式任务调度示例工程。

当前工程包含 6 个核心模块：

- `model`：共享 DTO、命令模型和 Redis key 常量
- `mq`：写请求入口，负责把创建/暂停/恢复/回调请求写入 Redis Stream
- `scheduler`：调度核心，消费 Stream 命令，维护任务状态，claim 到期任务并分发给 worker
- `worker`：执行节点，定时上报负载并异步执行任务，执行完成后回调 `mq`
- `preheater`：把近未来 `WAITING` 任务批量预热到 Redis
- `archiver`：把冷数据从 `sj_job` 迁移到 `sj_job_archive`，并导出归档文件

当前写链路：

`Client / Worker -> MQ -> Redis Stream -> Scheduler -> MySQL / Redis / Worker`

当前读链路：

`Client -> Scheduler`

更多说明见：

- [docs/任务调度器版.md](docs/任务调度器版.md)
- [docs/scheduler-core-only.md](docs/scheduler-core-only.md)
- [docs/scheduler-performance-high-traffic-reservation.md](docs/scheduler-performance-high-traffic-reservation.md)
- [docs/redis-cluster/README.md](docs/redis-cluster/README.md)
- [docs/local-startup.md](docs/local-startup.md)

## 环境要求

- JDK 17
- Maven 3.8+
- MySQL 8.x
- Redis 6.x+

说明：

- Windows 本地开发推荐使用 `Memurai` 或其他支持 `Redis Stream + Redis Cluster` 的 Redis 发行版
- 旧版 Windows `Redis 3.2` 不支持 `XADD / XREADGROUP`，不能运行当前工程

## 编译

```bash
mvn -DskipTests compile
```

## 启动顺序

1. 初始化 MySQL：执行 `docs/schedule_job.sql`
2. 启动 Redis Cluster
3. 启动 `scheduler`
4. 启动 `mq`
5. 启动 `worker`
6. 启动 `preheater`
7. 视需要启动 `archiver`

说明：

- 生产环境推荐把写请求发到 `mq`
- `scheduler` 仍保留原写接口，主要用于直连调试和兼容验证
- Windows + Memurai 的本地启动步骤见 [docs/local-startup.md](docs/local-startup.md)
