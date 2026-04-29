# Windows 本地启动流程（Memurai）

本文档记录当前工程在 Windows 本地使用 `Memurai + MySQL + IDEA` 的实际启动流程，适合本地联调和压测前准备。

## 1. 初始化 MySQL

执行：

```sql
source docs/schedule_job.sql
```

或直接在 MySQL 客户端中导入 `docs/schedule_job.sql`。

## 2. 编译工程

进入项目根目录：

```powershell
Set-Location <repo>
```

执行：

```powershell
mvn -DskipTests compile
```

## 3. 启动 Redis Cluster

按 [docs/redis-cluster/README.md](redis-cluster/README.md) 中的步骤启动 `7001~7006` 六个 Redis 节点，并完成 cluster 创建。

## 4. 在 IDEA 中配置多实例

所有 `--xxx=yyy` 参数都填写在 IDEA 运行配置的 `Program arguments` 中，不要填到 `VM options`。

### `mq`

主类：

```text
mq.MqMainApplication
```

Program arguments：

```text
```

### `scheduler`

主类：

```text
scheduler.SchedulerMainApplication
```

建议至少复制出 3 个配置：

`scheduler-1`

```text
--server.port=8301 --schedulejob.scheduler.instance-id=scheduler-1 --schedulejob.scheduler.dispatch-interval-ms=50
```

`scheduler-2`

```text
--server.port=8302 --schedulejob.scheduler.instance-id=scheduler-2 --schedulejob.scheduler.dispatch-interval-ms=50
```

`scheduler-3`

```text
--server.port=8303 --schedulejob.scheduler.instance-id=scheduler-3 --schedulejob.scheduler.dispatch-interval-ms=50
```

说明：

- `instance-id` 必须稳定且互不相同
- 压测场景建议把 `dispatch-interval-ms` 调低到 `50ms`

### `worker`

主类：

```text
worker.WorkerMainApplication
```

建议至少复制出 3 个配置：

`worker-1`

```text
--server.port=8401 --schedulejob.worker.host=127.0.0.1:8401 --schedulejob.worker.scheduler-callback-base-url=http://127.0.0.1:8200 --schedulejob.worker.execute-delay-min-ms=5 --schedulejob.worker.execute-delay-max-ms=10
```

`worker-2`

```text
--server.port=8402 --schedulejob.worker.host=127.0.0.1:8402 --schedulejob.worker.scheduler-callback-base-url=http://127.0.0.1:8200 --schedulejob.worker.execute-delay-min-ms=5 --schedulejob.worker.execute-delay-max-ms=10
```

`worker-3`

```text
--server.port=8403 --schedulejob.worker.host=127.0.0.1:8403 --schedulejob.worker.scheduler-callback-base-url=http://127.0.0.1:8200 --schedulejob.worker.execute-delay-min-ms=5 --schedulejob.worker.execute-delay-max-ms=10
```

说明：

- `host` 必须与 `server.port` 对应
- `scheduler-callback-base-url` 当前应指向 `mq`，即 `http://127.0.0.1:8200`
- 压测时建议降低模拟执行延迟，否则默认 `1000~3000ms` 会显著拉高端到端耗时

### `preheater`

主类：

```text
preheater.PreheaterMainApplication
```

Program arguments：

```text
```

### `archiver`

主类：

```text
archiver.ArchiverMainApplication
```

Program arguments：

```text
```

`archiver` 对联调和压测不是必需，可按需启动。

## 5. 推荐启动顺序

1. `scheduler-1`
2. `scheduler-2`
3. `scheduler-3`
4. `mq`
5. `worker-1`
6. `worker-2`
7. `worker-3`
8. `preheater`
9. `archiver`（可选）

## 6. 启动后验证

### 查看调度看板

```text
http://127.0.0.1:8301/jobs/dashboard
```

### 查看 scheduler 成员

```powershell
D:\Memurai\memurai-cli.exe -c -p 7001 ZRANGE schedulejob:scheduler:members 0 -1 WITHSCORES
```

### 查看 worker 心跳

```powershell
D:\Memurai\memurai-cli.exe -c -p 7001 ZRANGE schedulejob:{executor}:alive 0 -1 WITHSCORES
```

### 查看 Stream 消费组

```powershell
D:\Memurai\memurai-cli.exe -c -p 7001 XINFO GROUPS schedulejob:stream:commands
```

## 7. 常见问题

### 1. `ERR unknown command 'XADD'`

原因：使用了旧版 Windows `Redis 3.2.x`。  
处理：更换为 `Redis 6.x+` 或 `Memurai`。

### 2. `CROSSSLOT Keys in request don't hash to the same slot`

原因：在 Redis Cluster 中执行 Lua 脚本时，同时访问了不在同一 slot 的 key。  
当前代码已将执行器相关 key 统一为：

```text
schedulejob:{executor}:alive
schedulejob:{executor}:load
schedulejob:{executor}:loadstep
```

### 3. `BUSYGROUP Consumer Group name already exists`

原因：命令流消费组已存在。  
当前代码已将该场景作为幂等启动处理，重启 `scheduler` 时无需手动删组。

### 4. `ScheduleJobMapper` Bean 找不到

原因：启动类的 `@MapperScan` 未扫到 `scheduler.mapper`。  
当前代码已修正为：

```java
@MapperScan("scheduler.mapper")
```
