# Chord DHT 实现方案

## 概述

Chord 协议使用 SHA-1 哈希将节点和键映射到一个环形标识符空间（0 到 2^160 - 1）。每个键存储在其标识符的第一个后继节点上。指针表（Finger Table）实现 O(log N) 的查找复杂度，周期性稳定协议维护节点加入/离开时的正确性。通过后继列表实现数据冗余，保证部分节点强制退出时数据不丢失。

---

## 阶段一：数据结构与哈希工具（新文件 `node/chord.go`）

### 1.1 常量

```go
const (
    M           = 160     // SHA-1 输出位数
    SUCC_LIST_L = 10      // 后继列表大小（容错用）
    REPLICA_R   = 3       // 每条数据的副本数
)
var ringSize = new(big.Int).Exp(big.NewInt(2), big.NewInt(M), nil) // 2^160
```

### 1.2 `ChordEntry` 结构体

表示环上的一个节点引用：

```go
type ChordEntry struct {
    Addr string    // "ip:port"
    Id   *big.Int  // 哈希后的节点 ID
}
```

### 1.3 `ChordNode` 结构体

替换原有的 `Node`，实现 `DhtNode` 接口：

```go
type ChordNode struct {
    Addr   string       // "ip:port"
    online bool
    id     *big.Int     // hash(Addr)

    listener net.Listener
    server   *rpc.Server

    // 环状态
    predecessor   *ChordEntry
    successor     *ChordEntry
    successorList []*ChordEntry     // 大小 = SUCC_LIST_L
    fingerTable   []*ChordEntry     // 大小 = M
    nextFinger    int               // fixFingers 的轮转指针

    // 数据存储
    data     map[string]string
    dataLock sync.RWMutex

    // 锁
    ringLock sync.RWMutex

    // 稳定化控制
    stabilizeStop chan struct{}
}
```

### 1.4 哈希与环形运算函数

| 函数 | 作用 |
|---|---|
| `hashString(s string) *big.Int` | SHA-1 哈希 → 160 位大整数 |
| `between(id, a, b *big.Int) bool` | 判断 `id ∈ (a, b]`（环上，处理回绕） |
| `betweenLeftIncl(id, a, b *big.Int) bool` | 判断 `id ∈ [a, b)` |
| `addPowerOf2(id *big.Int, exp int) *big.Int` | 计算 `(id + 2^exp) mod 2^160` |

**`between` 的实现要点**：环形空间比较需要分三种情况：

1. `a < b`（正常区间）：`a < id && id <= b`
2. `a > b`（跨 0 回绕）：`id > a || id <= b`
3. `a == b`（整个环）：返回 `true`

### 1.5 `Init(addr string)` 方法

初始化所有字段，对地址做哈希得到 `id`，分配各切片/map/chan。

---

## 阶段二：RPC 方法（可远程导出的方法）

所有 RPC 方法遵循 Go `net/rpc` 签名规范：`func (t *T) MethodName(argType T1, replyType *T2) error`

### 2.1 环管理 RPC

| 方法 | 参数 → 返回值 | 功能 |
|---|---|---|
| `FindSuccessor(id *big.Int, reply *ChordEntry)` | id → 后继节点 | 核心 Chord 查找：若 id ∈ (self, successor]，返回 successor；否则转发到 closestPrecedingNode |
| `GetPredecessor(_ struct{}, reply *ChordEntry)` | — → 前驱节点 | |
| `SetPredecessor(entry *ChordEntry, _ *struct{})` | entry → — | 直接设置前驱（Quit 时使用） |
| `Notify(entry *ChordEntry, _ *struct{})` | entry → — | 若前驱为空或 entry 位于前驱和自身之间，则更新前驱；随后将属于新前驱区间的键迁移给它 |
| `GetSuccessors(_ struct{}, reply *[]*ChordEntry)` | — → 后继列表 | |
| `Ping(_ struct{}, _ *struct{})` | — → — | 活性检测 |

### 2.2 数据 RPC

| 方法 | 参数 → 返回值 | 功能 |
|---|---|---|
| `PutReplica(pair Pair, _ *struct{})` | (key, value) → — | 本地存储一个键值副本 |
| `GetData(key string, reply *string)` | key → value | 若 key 存在则返回 value |
| `DeleteData(key string, reply *bool)` | key → success | 本地删除 key，返回是否成功 |
| `TransferKeys(args *RangeArgs, reply *[]Pair)` | (start, end] → pairs | 返回 hash 值落在范围内的所有键值对，并从本地删除 |
| `HasKey(key string, reply *bool)` | key → bool | 检查 key 是否存在于本地 |

**`RangeArgs` 辅助结构：**

```go
type RangeArgs struct {
    Start *big.Int
    End   *big.Int
}
```

---

## 阶段三：核心 Chord 算法（私有方法）

### 3.1 查找

| 函数 | 伪代码逻辑 |
|---|---|
| `findSuccessor(id *big.Int) *ChordEntry` | 1. 若 `id ∈ (self.id, successor.id]` → 返回 successor<br>2. `n' = closestPrecedingNode(id)`<br>3. 若 `n' == self` → 返回 successor（兜底）<br>4. RPC 调用 `n'.FindSuccessor(id)` → 递归/转发<br>5. RPC 失败时：依次尝试 successorList 中的下一个 |
| `closestPrecedingNode(id *big.Int) *ChordEntry` | 从 fingerTable（从大到小）和 successorList 中，找到 ID 最大且居 `id` 之前的节点 |
| `getLiveSuccessor() *ChordEntry` | 依次 ping successorList 中的节点，返回第一个存活者 |

### 3.2 稳定化（周期性后台任务）

| 函数 | 周期 | 逻辑 |
|---|---|---|
| `stabilize()` | ~500ms | 1. RPC 获取 successor 的前驱<br>2. 若该前驱在 self 和 successor 之间 → 更新 successor<br>3. 调用 `successor.Notify(self)` |
| `fixFingers()` | ~500ms | 1. `nextFinger++ mod M`<br>2. `fingerTable[nextFinger] = findSuccessor(addPowerOf2(self.id, nextFinger))` |
| `checkPredecessor()` | ~1s | Ping 前驱；若失联 → `predecessor = nil` |
| `maintainSuccessorList()` | ~1s | 1. RPC 获取 successor 的 successorList<br>2. 重建：`[successor] + successor.successorList[:MAX-1]`，去重去自身 |
| `runStabilizationLoop()` | goroutine 主循环 | 循环执行：stabilize → fixFingers → checkPredecessor → maintainSuccessorList，通过 `stabilizeStop` channel 控制退出 |

---

## 阶段四：生命周期方法（实现 `DhtNode` 接口）

### 4.1 `Run(wg *sync.WaitGroup)`

1. 设置 `online = true`
2. 启动 goroutine 运行 RPC 服务器（TCP 监听 `self.Addr`）
3. 监听器就绪后 `wg.Done()`

**注意**：与原始实现不同，`Create()` 或 `Join()` 会在此之后由 `main.go` 调用，稳定化循环在 `Create/Join` 中启动。

### 4.2 `Create()`

1. `self.id = hash(self.Addr)`
2. `predecessor = nil`
3. `successor = &ChordEntry{self.Addr, self.id}`（环上唯一节点，指向自己）
4. `successorList = []*ChordEntry{successor}`
5. `fingerTable[i] = successor`（全部指向自己）
6. 启动 `runStabilizationLoop()` goroutine

### 4.3 `Join(addr string) bool`

1. `self.id = hash(self.Addr)`
2. 初始化 fingerTable 全部指向自己，successorList = [self]
3. RPC 调用 `addr.FindSuccessor(self.id)` → 获取初始 successor
4. 设置 `successor = result`，`successorList[0] = successor`
5. RPC 调用 successor：`TransferKeys(range)` — 从 successor 拉取现在应该归属自己的键（它们原本被 successor 保管）
6. 启动 `runStabilizationLoop()` goroutine
7. 成功返回 `true`，失败返回 `false`

**键范围确定**：新节点 n 加入后，其职责区间为 `(predecessor.id, n.id]`。初始 predecessor 为 nil，需要通过 stabilize/notify 逐步确定。在 Join 时可以先从 successor 拉取一些键，后续稳定化会修正。

### 4.4 `Quit()`

1. 将所有本地数据通过 `PutReplica` 逐个复制到 successor
2. RPC 通知 predecessor：将其 successor 设为 self.successor
3. RPC 通知 successor：将其 predecessor 设为 self.predecessor
4. 向 `stabilizeStop` 发送信号，停止稳定化循环
5. 关闭 RPC 服务器

### 4.5 `ForceQuit()`

1. 向 `stabilizeStop` 发送信号
2. 立即关闭 RPC 服务器（不做任何清理）

---

## 阶段五：数据操作（实现 `DhtNode` 接口）

### 5.1 `Put(key string, value string) bool`

1. `keyID = hash(key)`
2. `target = findSuccessor(keyID)`
3. 对 i = 0 到 REPLICA_R-1：
   - RPC 调用 `target.Addr.PutReplica(Pair{key, value})`
   - `target = 顺延 successorList 下一个`（通过 RPC `GetSuccessors` 获取下一批）
4. 只要至少有一个副本写入成功，返回 `true`

### 5.2 `Get(key string) (bool, string)`

1. `keyID = hash(key)`
2. `target = findSuccessor(keyID)`
3. RPC 调用 `target.Addr.GetData(key)`；若找到 → 返回
4. 若未找到或 RPC 失败：依次尝试 successorList 中的下一个节点
5. 返回 `(ok, value)`

**设计理由**：由于 Put 时写入 REPLICA_R 个副本，即使主节点 force-quit，其后继节点仍有副本，可从后继读取。

### 5.3 `Delete(key string) bool`

1. `keyID = hash(key)`
2. `target = findSuccessor(keyID)`
3. 对 i = 0 到 REPLICA_R-1：
   - RPC 调用 `target.Addr.DeleteData(key)`，记录返回结果
   - `target = 顺延 successorList 下一个`
4. 只要至少有一个节点确实持有该 key 且成功删除，返回 `true`；全部不存在则返回 `false`

---

## 阶段六：修改 `node/factory.go`

```go
func NewNode(port int) DhtNode {
    node := new(ChordNode)   // 原为 new(Node)
    node.Init(portToAddr(localAddress, port))
    return node
}
```

原有的 `node.go` 中的 `Node` 实现可保留作为参考，不会被引用。

---

## 阶段七：错误处理与鲁棒性

| 场景 | 处理策略 |
|---|---|
| RPC 超时 | 复用 `RemoteCall` 的 10 秒 `DialTimeout` 模式 |
| successor 失联 | `getLiveSuccessor()` 依次遍历 successorList，返回第一个存活节点 |
| predecessor 失联 | `checkPredecessor()` 周期性 ping，失联则置 nil |
| findSuccessor 中继跳失败 | 尝试 successorList 中下一个存活节点作为中转 |
| successorList 耗尽 | 操作返回失败（在测试容错率 1%~15% 内可接受） |
| 并发安全 | 环状态用 `ringLock` 保护；数据用 `dataLock` 保护；读操作用 `RLock` |

---

## 阶段八：测试验证

```bash
go test ./node -run TestBasic -v -timeout 0
go test ./node -run TestForceQuit -v -timeout 0
go test ./node -run TestQuitAndStabilize -v -timeout 0
go test ./node -run TestDelete -v -timeout 0
```

**可调优参数**：

- 稳定化间隔（缩短可加快收敛，但增加 RPC 负载）
- `SUCC_LIST_L`（更大 = 更强容错）
- `REPLICA_R`（更大 = 数据更安全，但写入开销更大）

---

## 文件变更清单

| 文件 | 操作 |
|---|---|
| `node/chord.go` | **新建** — 全部 ChordNode 实现（预计 500~700 行） |
| `node/factory.go` | **修改** — `new(Node)` 改为 `new(ChordNode)` |
| `node/interface.go` | 不变 |
| `node/addr.go` | 不变 |
| `node/node.go` | 保留作为参考（不再被 factory 引用） |
| `main.go` | 不变 |
