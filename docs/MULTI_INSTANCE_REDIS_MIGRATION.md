# 多实例部署 Redis 改造方案

## 目录

- [1. 分析过程](#1-分析过程)
- [2. 最终结论](#2-最终结论)
- [3. 修改方案](#3-修改方案)
- [4. 最终部署架构](#4-最终部署架构)
- [5. 边缘场景验证](#5-边缘场景验证)

---

## 1. 分析过程

### Step 1：识别所有实例本地状态数据

扫描整个项目，找出所有存储在 JVM 内存中的状态：

| # | 位置 | 存储结构 | 存储内容 |
|---|------|---------|---------|
| ① | `MemorySaver._checkpointsByThread` | `HashMap<String, LinkedList<Checkpoint>>` | 图执行的 Checkpoint（state、nodeId、nextNodeId） |
| ② | `GraphServiceImpl.streamContextMap` | `ConcurrentHashMap<String, StreamContext>` | SSE sink、Disposable、Langfuse Span、文本类型 |
| ③ | `MultiTurnContextManager.history` | `ConcurrentHashMap<String, Deque<ConversationTurn>>` | 多轮对话历史（用户问题 + planner 摘要） |
| ④ | `MultiTurnContextManager.pendingTurns` | `ConcurrentHashMap<String, PendingTurn>` | 当前轮次的 userQuestion + planBuilder |
| ⑤ | `LangfuseService.TOKEN_ACCUMULATOR` | `ConcurrentHashMap<String, long[]>` | token 累计计数 |

### Step 2：逐项分析多实例下的影响

对每个状态，追问：如果请求被路由到不同实例，会读到什么？

---

#### ① `MemorySaver` — 需要改为 `RedisSaver`

```
首次请求 (实例A):
  compiledGraph.stream(input, config)
    → 遇到 HUMAN_FEEDBACK_NODE
    → CheckpointSaver.put(config, checkpoint)  ← 写入实例 A 内存

human feedback 请求 (实例B):
  compiledGraph.updateState(config, stateUpdate)
    → CheckpointSaver.get(config)               ← 实例 B 没有此 checkpoint!
    → 读不到 → 抛异常
```

**结论：❌ 必须改为 RedisSaver。** 框架已内置 `RedisSaver`，使用 Redisson 分布式锁 + Redis Bucket，支持跨实例读写。

---

#### ② `streamContextMap` — 不需要修改

访问路径追踪：

```
graphStreamProcess(sink, request)    ← 所有 SSE 请求的唯一入口
  ├── computeIfAbsent(Z, new())      ← 必定创建新 context
  ├── context.setSink(sink)
  │
  ├── handleNewProcess(request)      ← 首次查询
  │     ├── streamContextMap.get(Z)  ← 不为 null（刚 computeIfAbsent）
  │     └── subscribeToFlux(...)
  │           ├── handleNodeOutput → streamContextMap.get(Z)  ← 流输出，同一实例
  │           ├── handleStreamComplete → streamContextMap.remove(Z)  ← 完成时移除
  │           └── handleStreamError → streamContextMap.remove(Z)    ← 异常时移除
  │
  └── handleHumanFeedback(request)   ← human feedback
        ├── streamContextMap.get(Z)  ← 不为 null（刚 computeIfAbsent）
        └── subscribeToFlux(...)
              ├── ...
              ├── handleStreamComplete → streamContextMap.remove(Z)
              └── handleStreamError → streamContextMap.remove(Z)
```

关键观察：`computeIfAbsent` 和 `get` 之间是**顺序执行**的，没有并发窗口。所有读写都在同一个 HTTP 请求的处理线程内，不会跨实例。

```
human feedback 的场景：

请求①（首次查询，路由到 A）:
  A: graphStreamProcess → computeIfAbsent(Z) → sink=A_conn
  A: handleNewProcess → compiledGraph.stream()
  A: → 遇到 HUMAN_FEEDBACK_NODE → Checkpoint 写入 Redis
  A: → Normal End → handleStreamComplete → remove(Z) + cleanup(sink)

此时 A 上 threadId=Z 已被 remove。

请求②（human feedback，路由到 B）:
  B: graphStreamProcess → computeIfAbsent(Z) → sink=B_conn  ← 全新 sink
  B: handleHumanFeedback → get(Z) → 不为 null
  B: → compiledGraph.updateState → 从 Redis 读 Checkpoint
  B: → compiledGraph.stream(null, resume) → 从 Checkpoint 恢复
  B: → handleStreamComplete → remove(Z) + cleanup(sink)
```

此外，`StreamContext` 存储的内容（`Sinks.Many`、`Disposable`、`Span`）都是 **JVM 进程内对象引用**，无法且不应序列化到其他实例。

**结论：✅ 不需要修改。**

---

#### ③ `history` — 需要改为 Redis

```
首次请求（实例A）:
  beginTurn(Z, Q)                                       ← 写入 pendingTurns
  ...
  handleStreamComplete(Z):
    → pendingTurns.remove(Z) → 构建 ConversationTurn
    → history.computeIfAbsent(Z, ...).addLast(turn)     ← 写入实例 A 内存

human feedback 请求（实例B）:
  buildContext(Z):
    → history.get(Z) → null                             ← 实例 B 没有！
    → 返回 "(无)"

  restartLastTurn(Z):
    → history.get(Z) → null
    → 跳过，什么都不做                                   ← 上下文丢失！
```

**结论：❌ 必须改为 Redis。** history 的写入和读取可能在不同实例上。

---

#### ④ `pendingTurns` — 不需要修改

```
实例A:
  beginTurn(Z, Q)              ← pendingTurns.put(Z, PendingTurn(Q))
  appendPlannerChunk(Z, chunk) ← pending.planBuilder.append(chunk)
  ...
  stream 正常完成/异常          ← 仍在实例 A
  finishTurn(Z):               ← pendingTurns.remove(Z)
  discardPending(Z):           ← pendingTurns.remove(Z)
```

`beginTurn`、`appendPlannerChunk`、`finishTurn`/`discardPending` 三者全部在 **同一个 `graph.stream()` 调用内部的同一个实例**上执行。

**结论：✅ 不需要修改。**

---

#### ⑤ `LangfuseService.TOKEN_ACCUMULATOR` — 不需要修改

用于统计 token 用量上报 Langfuse。多实例下每个实例各自累计，上报时每个实例报自己的部分，Langfuse 端会汇总。

**结论：✅ 不需要修改。** 统计误差可接受。

---

### Step 3：收敛到最终结论

经过逐一分析：

| 组件 | 是否需要修改 | 依据 |
|------|------------|------|
| `MemorySaver` → `RedisSaver` | ✅ 必须改 | Checkpoint put 在 A，get 在 B |
| `history` → Redis `RList` | ✅ 必须改 | 跨实例读写 |
| `pendingTurns` | ✅ 不改 | 绑定在一次 graph.stream() 生命周期内 |
| `streamContextMap` | ✅ 不改 | sink 生命周期 = stream 周期，读写不跨实例 |
| `LangfuseService` | ✅ 不改 | 统计误差可接受 |

### Step 4：验证隐含假设

**假设：一次 `graph.stream()` 调用会分裂到多个实例上执行吗？**

不会。`compiledGraph.stream()` 返回 `Flux<NodeOutput>`，由 `subscribeToFlux` 通过 `CompletableFuture.runAsync` 在本地 ExecutorService 上执行。所有 subscribe 回调（`handleNodeOutput`、`handleStreamComplete`、`handleStreamError`）都在同一个 JVM 进程中执行。

```java
CompletableFuture.runAsync(() -> {
    Disposable disposable = nodeOutputFlux.subscribe(
        output -> handleNodeOutput(request, output),       // 本实例
        error -> handleStreamError(agentId, threadId, error),  // 本实例
        () -> handleStreamComplete(agentId, threadId)       // 本实例
    );
    context.setDisposable(disposable);
}, executor);
```

**结论：一次 stream 调用不会跨实例。**

---

## 2. 最终结论

需要修改的只有 **2 个点**：

```
需要修改：
  ├── Checkpoint:   MemorySaver → RedisSaver    [框架内置，配置即可]
  └── 多轮 history:  ConcurrentHashMap → Redis   [业务代码改造]

不需要修改（确认）：
  ├── pendingTurns                                [本地缓冲区，不跨实例]
  ├── streamContextMap                            [sink = stream 周期，不跨实例]
  ├── LangfuseService                             [统计误差可接受]
  ├── GraphController                             [无状态]
  ├── StreamContext                               [进程内对象，无法序列化]
  └── 所有 Node/Dispatcher 实现类                  [操作 OverAllState，对存储透明]
```

---

## 3. 修改方案

### 修改清单

```
修改的文件 (4)
  pom.xml                                    +1 行依赖
  application.yml                            +6 行配置
  DataAgentConfiguration.java                ~30 行（新增 CompiledGraph Bean）
  MultiTurnContextManager.java               ~60 行（history 改为 Redis RList）

不修改但构造参数类型变化的文件 (1)
  GraphServiceImpl.java                      StateGraph → CompiledGraph
```

---

### 3.1 `pom.xml` — 添加 Redisson 依赖

```xml
<dependency>
    <groupId>org.redisson</groupId>
    <artifactId>redisson-spring-boot-starter</artifactId>
    <version>3.34.1</version>
</dependency>
```

Spring Boot 自动配置 `RedissonClient`，无需手动创建。

---

### 3.2 `application.yml` — Redis 配置

```yaml
spring:
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
      password: ${REDIS_PASSWORD:}
      timeout: 3000ms
      lettuce:
        pool:
          max-active: 16
          max-idle: 8
          min-idle: 4
```

---

### 3.3 `DataAgentConfiguration.java` — 注入 RedisSaver

**新增 `CompiledGraph` Bean**，将 `GraphServiceImpl` 构造中的 `compile()` 上提到配置层：

```java
@Bean
public CompiledGraph compiledGraph(
        StateGraph stateGraph,
        RedissonClient redissonClient) throws GraphStateException {

    RedisSaver redisSaver = RedisSaver.builder()
            .redisson(redissonClient)
            .build();

    return stateGraph.compile(
            CompileConfig.builder()
                    .saverConfig(SaverConfig.builder()
                            .register(redisSaver)
                            .build())
                    .interruptBefore(HUMAN_FEEDBACK_NODE)
                    .build());
}
```

**`GraphServiceImpl` 相应调整**：构造参数从 `StateGraph` 改为 `CompiledGraph`：

```java
// 改前
public GraphServiceImpl(StateGraph stateGraph, ExecutorService executorService,
        MultiTurnContextManager multiTurnContextManager, LangfuseService langfuseReporter)
        throws GraphStateException {
    this.compiledGraph = stateGraph.compile(
            CompileConfig.builder().interruptBefore(HUMAN_FEEDBACK_NODE).build());
    this.executor = executorService;
    this.multiTurnContextManager = multiTurnContextManager;
    this.langfuseReporter = langfuseReporter;
}

// 改后
public GraphServiceImpl(CompiledGraph compiledGraph, ExecutorService executorService,
        MultiTurnContextManager multiTurnContextManager, LangfuseService langfuseReporter) {
    this.compiledGraph = compiledGraph;
    this.executor = executorService;
    this.multiTurnContextManager = multiTurnContextManager;
    this.langfuseReporter = langfuseReporter;
}
```

---

### 3.4 `MultiTurnContextManager.java` — history 改为 Redis RList

**只改 `history`，不动 `pendingTurns`**：

```java
@Slf4j
@Component
public class MultiTurnContextManager {

    private final RedissonClient redisson;
    private final DataAgentProperties properties;

    private static final String TURNS_KEY = "agent:multi_turn:%s:turns";

    // pendingTurns 保持本地 — 不跨实例
    private final Map<String, PendingTurn> pendingTurns = new ConcurrentHashMap<>();

    public MultiTurnContextManager(RedissonClient redisson, DataAgentProperties properties) {
        this.redisson = redisson;
        this.properties = properties;
    }

    // beginTurn — 完全不变（操作本地 pendingTurns）
    public void beginTurn(String threadId, String userQuestion) {
        if (StringUtils.isAnyBlank(threadId, userQuestion)) return;
        pendingTurns.put(threadId, new PendingTurn(userQuestion.trim()));
    }

    // appendPlannerChunk — 完全不变（本地 StringBuilder 累积）
    public void appendPlannerChunk(String threadId, String chunk) {
        if (StringUtils.isAnyBlank(threadId, chunk)) return;
        PendingTurn pending = pendingTurns.get(threadId);
        if (pending != null) {
            pending.planBuilder.append(chunk);
        }
    }

    // discardPending — 完全不变
    public void discardPending(String threadId) {
        pendingTurns.remove(threadId);
    }

    // finishTurn — 只改此处：写入 Redis 而非本地 history
    public void finishTurn(String threadId) {
        PendingTurn pending = pendingTurns.remove(threadId);
        if (pending == null) return;
        String plan = StringUtils.trimToEmpty(pending.planBuilder.toString());
        if (StringUtils.isBlank(plan)) return;

        String trimmed = StringUtils.abbreviate(plan, properties.getMaxplanlength());
        // 写入 Redis，保留最近 N 轮
        RList<String> turns = redisson.getList(TURNS_KEY.formatted(threadId));
        turns.add(pending.userQuestion + "|||" + trimmed);
        while (turns.size() > properties.getMaxturnhistory()) {
            turns.remove(0);
        }
        turns.expire(Duration.ofHours(24));  // TTL 防止僵尸 key
    }

    // restartLastTurn — 改为从 Redis 读取
    public void restartLastTurn(String threadId) {
        RList<String> turns = redisson.getList(TURNS_KEY.formatted(threadId));
        if (turns.isEmpty()) return;
        String last = turns.remove(turns.size() - 1);
        String[] parts = last.split("\\|\\|\\|", 2);
        pendingTurns.put(threadId, new PendingTurn(parts[0]));
    }

    // buildContext — 改为从 Redis 读取
    public String buildContext(String threadId) {
        List<String> all = redisson.<String>getList(TURNS_KEY.formatted(threadId)).readAll();
        if (all.isEmpty()) return "(无)";
        return all.stream().map(entry -> {
            String[] parts = entry.split("\\|\\|\\|", 2);
            return "用户: " + parts[0] + "\nAI计划: " + (parts.length > 1 ? parts[1] : "");
        }).collect(Collectors.joining("\n"));
    }

    // PendingTurn 内部类保持不变
    private static class PendingTurn {
        private final String userQuestion;
        private final StringBuilder planBuilder = new StringBuilder();
        private PendingTurn(String userQuestion) {
            this.userQuestion = userQuestion;
        }
    }
}
```

**为什么不把 `appendPlannerChunk` 写入 Redis？**

一次 graph stream 中 `appendPlannerChunk` 可能被调用几百次（流式输出），每次写 Redis 的 RT 约 1ms，就是几百 ms 的开销。而且 `appendPlannerChunk` 和 `finishTurn` 在**同一个实例**上执行（Step 4 已验证），所以保持本地 `StringBuilder` 累积，`finishTurn` 时一次性写入 Redis。

---

## 4. 最终部署架构

```
                        ┌──────────────┐
                        │   Client     │
                        └──────┬───────┘
                               │ HTTP / SSE（每次请求独立）
                        ┌──────┴───────┐
                        │  Nginx / LB  │  （可选 hash $arg_threadId）
                        └──────┬───────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
      ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
      │  Instance A  │ │  Instance B  │ │  Instance C  │
      │              │ │              │ │              │
      │  streamCtxMap│ │  streamCtxMap│ │  streamCtxMap│ ← 本地，不跨实例
      │  pendingTurns│ │  pendingTurns│ │  pendingTurns│ ← 本地，不跨实例
      └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
             │                │                │
             └────────────────┼────────────────┘
                              │
                    ┌─────────▼──────────┐
                    │       Redis         │
                    │  checkpoint:*       │ ← RedisSaver（框架内置）
                    │  agent:multi_turn:* │ ← 多轮对话 history
                    └────────────────────┘
```

**Nginx 可选配置**（Session Affinity，加了更好，不加也能工作）：

```nginx
upstream dataagent {
    hash $arg_threadId consistent;
    server 192.168.1.1:8065;
    server 192.168.1.2:8065;
    server 192.168.1.3:8065;
}
```

---

## 5. 边缘场景验证

### 场景 1：请求① 的 stream 还没 complete，请求② 就到了

`computeIfAbsent` 会返回已存在的 context，请求② 的 `setSink(sink2)` 会覆盖 ① 的 sink。但实际中：
- human feedback 请求的前提是第一次 stream 遇到了 `HUMAN_FEEDBACK_NODE` 并完成
- 如果第一次 stream 还没完成，前端不会有 human feedback 的 UI 交互入口
- 这是**前端时序保证**，不是后端需要处理的问题

### 场景 2：客户端断连后，通知路由到无 context 的实例

```
请求①（首次查询，路由到 A）→ StreamContext 在 A 上

客户端断连 → doOnCancel → stopStreamProcessing(Z)
  → 如果断连通知到 A：正常，remove(Z) 成功
  → 如果断连通知到 B：remove(Z) → null → 跳过（已有 null 判断）
```

`stopStreamProcessing` 已有 `if (context != null)` 兜底。

### 场景 3：同一 threadId 并发发起两个新查询

两个请求都会调用 `computeIfAbsent(Z)`，但第二个请求会覆盖第一个的 sink。不过实践中，前端不会同时对同一个 threadId 发起两个 SSE 请求。如果确实有这种场景，需要在应用层做并发控制，但这是另一层的问题，与 Redis 改造无关。

### 场景 4：Redisson 连接中断

Redisson 有内置重连机制。连接恢复后会自动重连并继续工作。期间正在执行的 stream 不会受影响（它们在本地 JVM 中执行）。当 stream 完成时，`finishTurn` 写入 Redis 如果因连接断开失败，会抛异常，由 `handleStreamError` 处理。

如果对 Redis 可用性要求高，可以部署 Redis Sentinel 或 Redis Cluster 实现高可用。
