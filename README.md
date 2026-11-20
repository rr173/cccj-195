# snapshot-audit — 快照事务历史的只读离线审计器

导入**已经发生**的快照事务历史，回答两个问题：

1. 某次分析（读取）是否读到了**不合法版本**（未来版本 / 回滚版本 / 未提交版本 /
   过期版本 / 值不符 / 缺键 / 多键）；
2. 各次读取虽然都符合快照语义，但是否**无法按声明版本序串行解释**
   （依赖图中存在环）。

审计器**不调度事务、不修改原库、不通过重试修复历史**。证据不足时输出
`unknown` 及缺口清单，绝不把缺证据当作"无异常"；只有证据足够的确证违例才报
`invalid`。

另提供**观察依赖血缘**查询（`lineage` / `verify-lineage`）：给定原始历史与一
个 `commit_pos`，重建该位置的键值并追溯每个结果版本的写入事务在写入前实际观
察到的版本（见下文专节）。

还提供**事务负载画像**（`profile` / `verify-profile`）：从单次采集历史直接回
答"哪个时间段最忙""哪些实际键被频繁读取或更新""哪些闭区间被反复扫描"，
按固定时间窗、逐键热度与区间覆盖热度给出确定性 JSON（见下文专节）。

## 运行

```bash
# 审计（退出码：0=valid 1=invalid 2=unknown 3=输入错误）
python3 -m snapshot_audit audit --input samples/write_skew.json
python3 -m snapshot_audit audit --input-dir /path/to/history_dir --out result.json

# 独立核验见证（不信任审计器存的结论，从原始历史重新推导）
python3 -m snapshot_audit verify-witness --input samples/write_skew.json \
    --witness result.json

# 观察依赖血缘（退出码：0=complete 2=unknown/truncated 3=输入或用法错误）
python3 -m snapshot_audit lineage --input samples/lineage_demo.json \
    --commit-pos 3 --key 3 --out lin.json
python3 -m snapshot_audit lineage --input samples/lineage_demo.json \
    --commit-pos 2 --lo 1 --hi 9 --max-depth 8 --max-nodes 5000

# 独立核验血缘（从原始历史重建目标状态与观察依赖图，不信任待核验 JSON）
python3 -m snapshot_audit verify-lineage --lineage lin.json

# 事务负载画像（退出码：0=complete 2=存在 unknown 缺口 3=输入或用法错误）
python3 -m snapshot_audit profile --input samples/workload_profile_demo.json \
    --window-width 10 --top-n 10 --out profile.json
# 独立核验画像（从原始事务记录重算窗口/分类/排名/分段/unknown 传播）
python3 -m snapshot_audit verify-profile --profile profile.json

# 跨运行回归对比（两份独立原始历史，分别从原始数据重新审计）
python3 -m snapshot_audit compare \
    --baseline-input old.json   --baseline-run-id run-2026-10 \
    --candidate-input new.json  --candidate-run-id run-2026-11 \
    --mapping rename.json \
    --manifest-out manifest.json --out regression.json --ledger ledger.db
#   退出码：0=无回归/改善 1=回归 2=unknown（证据不足/歧义） 3=输入或用法错误

# 独立核验回归报告（重新审计、重建规范化 finding、重跑映射与匹配）
python3 -m snapshot_audit verify-diff --manifest manifest.json \
    --report regression.json --ledger ledger.db \
    --baseline-input old.json --candidate-input new.json \
    --mapping rename.json

# 测试
python3 -m unittest discover -s tests -t .

# 基准（生成规模集，记录索引计划/候选比较数/RSS）
python3 bench/run_bench.py            # 完整集，写入 bench/BENCH.md
python3 bench/run_bench.py --quick    # 最小集
```

## 跨运行回归对比（compare / verify-diff）

面向数据仓库在两个采集时点产生的**两份独立历史**。两侧都从原始历史重新
审计，绝不读取旧的 `result.json` 作为事实，也不修改任一原始历史。

### 比较清单（manifest）

`compare` 先生成并自签名一份比较清单，固定：两份输入的规范化绝对路径、
`run_id`、内容 SHA-256（目录按排序后的逐文件摘要聚合）、身份映射文件的
SHA-256、审计配置和生成器版本。运行期间输入或映射发生变化、两侧 `run_id`
重复、映射一对多/多对一、映射引用不存在的事务、提交范围或记录不完整时，
拒绝产出正常回归结论（硬错误退出码 3，或带可定位原因的 `unknown`），绝不把
“无变化”当作成功。

### 事务身份

* 同名事务默认按 `txn_id` 隐式对齐；
* 事务被重命名时**只能**通过显式的一对一映射对齐（映射文件为
  `{"mappings":[{"baseline_txn_id":...,"candidate_txn_id":...}]}` 或同等
  列表）；未映射事务仍进入新增/消失统计；
* 规范化事务 id：映射对统一收敛到 baseline id，同名收敛到裸 id，单侧私有
  id 带侧前缀，永不碰撞。

### 规范化 finding 身份

身份不依赖文件行号、`commit_pos`、SQLite 自增主键或 `version_id` 是否重新
编号：

* 读异常：事务身份 + 操作序号 + 异常类别 + 键或闭区间 + 观察/期望的**语义
  值**；
* 依赖环：事务边 + 边类型 + 键/区间 + 前后继写语义，并对环做**规范化旋转**，
  同一环从不同节点开始身份相同；同一节点环存在多个合法见证（不同边类型/
  键的组合）时枚举全部见证，无法唯一对应记为 `ambiguous`；
* `version_id` 和原件位置只作为**证据**，单独变化只会置 `evidence_changed`
  标志，绝不单独造成新故障；
* 同一事务/操作/键发生语义冲突记 `identity_conflict`，不会被错误归为持续或
  已解决；
* unknown 缺口按稳定的缺口类别 + 事务/操作范围 + 证据语义比较，证据缺失绝不
  降级成 `valid`/`resolved`。

### 报告与 SQLite 账本

确定性 JSON 报告 + 事务写入、分批落盘的 SQLite 账本，至少包含：每侧审计摘要
与输入哈希；新增 invalid、消失/解决 invalid、持续 invalid、finding 证据改变、
valid/unknown 状态转移、数据缺口变化、未映射事务、`ambiguous`、
`identity_conflict`。每行保留两侧规范化身份、原始证据位置、变化原因和可追溯
`run_id`。结果按稳定键排序，重复运行**字节级一致**（仅操作侧的精确 RSS/耗时
放在未签名的 `runtime_stats`）。匹配走身份哈希索引，不做无索引笛卡尔积；
统计真实的 `index_hits`、`candidate_comparisons`、`rows_written`、峰值 RSS
与耗时。

### verify-diff

接收比较清单、回归报告和两份原始历史，重新审计、重建规范化 finding、重跑
身份映射与匹配，再逐项核对报告签名/哈希、分类、计数、证据位置与排序，并独立
确认每条“解决”只有在 candidate 证据完整且对应 finding 确实不再出现时才成立。
报告、映射或输入被改动一字、删除一条 ambiguous/identity_conflict 记录都会失败
并指出不一致位置。退出码：0=核验通过，1=核验失败，3=无法加载。

## 输入合同

两种等价格式：

* 单文件 JSON（`--input`）：一个对象包含全部内容；
* 目录 JSONL（`--input-dir`，流式摄入）：`header.json` + `commits.jsonl` +
  `transactions.jsonl`（每行一条记录）。

```jsonc
{
  "format": "snapshot-history/1",
  "initial_state": [                 // 初始键值；初始不存在有统一语义（found=false）
    {"key": 1, "value": 0, "version_id": "ix"}
  ],
  "commit_log": {
    "first_commit_pos": 1,           // 完整范围声明
    "last_commit_pos": 2,
    "complete": true,                // 声明该范围包含全部提交
    "commits": [
      {"txn_id": "T1", "commit_pos": 1,   // 唯一且递增
       "writes": [
         {"key": 1, "version_id": "x1", "predecessor": "ix", "value": 1},
         {"key": 2, "version_id": "y1", "predecessor": "iy", "tombstone": true}
       ]}
    ]
  },
  "transactions": [
    {"txn_id": "T1", "status": "committed",
     "snapshot_pos": 0,               // 固定快照点
     "start_time": 10, "end_time": 20,
     "operations": [                  // 按 seq 从 0 连续编号
       {"seq": 0, "type": "point_read", "key": 2, "found": true,
        "value": 0, "version_id": "iy"},
       {"seq": 1, "type": "range_read", "lo": 0, "hi": 100,
        "entries": [{"key": 2, "value": 0, "version_id": "iy"}],
        "complete": true},            // 结果结束标志
       {"seq": 2, "type": "put", "key": 1, "value": 1},
       {"seq": 3, "type": "delete", "key": 2}
     ]},
    {"txn_id": "Tx", "status": "aborted",   // 回滚事务：不参与提交图
     "writes": [{"key": 5, "version_id": "bad1", "value": 9}]}  // 仅用于归因
  ]
}
```

合同要点：

* 键为 int64；值为原样 JSON 标量（按规范化编码比较，`1` 与 `1.0` 不同）；
* 每个事务对一键最多写一次；可以先写后读（自身写覆盖快照）；
* 提交版本含明确 `version_id` 与 `predecessor`，delete 为墓碑版本；
* 日志包含声明范围内**全部**提交的写集；
* 范围读记录**完整**返回列表与结束标志；
* 仅分析成功提交的事务；回滚事务的写**绝不**作为可读版本。

违反完备性的地方（未声明完整范围、commit_pos 缺号/重复/不递增、操作序号缺失、
扫描未结束、版本链断裂、写集与日志不符……）全部记为 **gap**，阻止 `valid`
结论。

## 判定逻辑

1. **读核对**：每次读取的期望值 = 本事务此前自身写 覆盖 snapshot_pos 处已提交
   状态。点读逐项比对；范围读比对闭区间内全部非墓碑键（缺项/多项/错误版本/
   错误值都定位到具体操作）。异常分类互不混淆：
   `future_version_read`（未来版本）、`aborted_version_read`（回滚版本）、
   `unknown_version_read`（未提交/不存在版本，仅日志完整时可确证）、
   `cross_key_version_read`、`stale_version_read`、`value_mismatch`、
   `missing_key`、`extra_key`。其中未来/回滚版本即使日志不完整也可确证。
2. **依赖图**（仅在证据完整且读核对全部通过时构建）：
   * `ww`：同键提交版本按 commit_pos 的写-写序（声明版本序）；
   * `wr`：read-from（含读到墓碑/不存在的"删除者在读者前"边）；
   * `rw`：读到的快照版本 → 其后继写的反依赖；范围读还覆盖当时不存在、
     后来被插入的键（phantom），从**实际键变化**（版本索引）找，不展开
     int64 域，也不只关联扫描返回过的键；
   * `realtime`：A 结束早于 B 开始（传递约简后落盘，保持可达性不变）。
   * 本事务自身写不产生自环。
3. **判定**：图有环 → `invalid`，输出一条最短的具体环（每条边带键/区间、
   版本、操作与原件位置），环经独立证据核验；无环 → `valid`，输出拓扑次序
   及按该次序重放能解释全部观察的**重放证书**；图无法完整建立（如超出内存
   预算）→ `unknown`，绝不把"未发现环"写成 `valid`。

## verify-witness

`verify-witness` 从原始历史重新检查，不相信审计器存的结论：

* 环见证：逐条边重新推导（版本存在性、链邻接、读操作覆盖键/区间、快照可见
  版本、后继版本、自身写排除、实时关系），并检查环闭合与简单性；
* 重放证书：检查次序覆盖全部已提交事务、尊重声明版本序与实时关系，并按固定
  操作顺序重放全部读取；
* 异常见证：重新运行读核对，要求在同一操作复现同类异常。

退出码：0=核验通过，1=核验失败，2=无可核验内容（如 unknown 结论）。

## 观察依赖血缘（lineage / verify-lineage）

`lineage` 回答"某个历史位置的值是怎么被观察出来的"：给定原始历史与
`commit_pos`，先**按历史 commit_pos 重建目标状态**（点查一个键的当前值，或
闭区间内全部非墓碑键值），再对每个结果版本递归追溯：哪个已提交事务写入了
它、该事务在写操作之前实际读到了哪些版本、这些版本各自的上游观察依赖。

**语义边界（重要）**：输入格式只记录"每个事务观察到了什么"，不记录计算表
达式或输出列映射。因此写入事务在写操作前的**全部**读取都标为**事务级观察
集合**——边"V 的写入者观察过 U"绝不表示 U 在业务因果上产生了 V。输出中
`lineage_semantics` 恒为 `observation_dependency`。

### 图模型

* **节点** = 已提交版本（含初始值、墓碑），保留写入事务、写操作序号
  （`write_op_seq`）、版本语义（`initial` / `committed_value` /
  `tombstone`）与原件位置（`loc`）。
* **边** = 写入事务在写操作前的读观察，按 `kind` 区分：
  * `point_read`：点读（含 `found=false` 的缺失观察：`absent_tombstone`
    指向当时可见的墓碑版本，`absent_no_version` 无目标节点）；
  * `range_read_item`：范围读返回项；
  * `range_predicate`：区间完备性谓词观察（"快照时该闭区间内只有这些活
    键"），以及对区间内可见墓碑版本的逐项缺失观察。**范围查询中不存在
    的键不生成值节点**：谓词观察只记录区间、快照点与返回键集，绝不伪造
    一个读到的值；
  * `own_write`：事务内先写后读（`intra_txn: true`）。这是事务内依赖，
    在版本图上表现为指向同事务另一版本节点的边，**绝不形成事务级自环**。
* 血缘**按版本和读取时的 `snapshot_pos` 展开**，不沿每键最新版本或当前依
  赖图倒推。读操作记录了 `version_id` 的边指向实际读到的版本（即使是异常
  读）；未记录版本 id 的读按读取时快照可见版本解析并标
  `version_recorded: false`。
* 递归路径去重（每个版本只展开一次）且输出稳定排序；同一上游版本被多个
  操作观察时**各保留各的证据**（每条边带自己的读操作与位置）。异常读造
  成的版本级环以稳定的 SCC 列表输出在 `cycles` 中，不无限递归；冲突证据
  （如同一 commit 同键两写）标为 unknown 边界，不静默消解。

### unknown 的局部化

每个节点/边独立携带 `status`（`complete` / `unknown`）与缺口清单：缺失/冲
突版本链、未声明完整的日志范围、未结束的范围扫描、缺失操作记录、无法归因
的观察值、回滚/未提交版本观察等，只把**受影响**的节点/边标 unknown 并说明
缺口；无关分支的缺口不污染可证明的独立结果（例如键 99 的版本链断裂不影响
键 1 的完整血缘）。整体 `status` 只有在全部结果、节点、边均可证明且无截
断时才是 `complete`。

### 结果绑定、确定性与截断

结果绑定：原始输入规范路径与内容 SHA-256（目录按排序逐文件摘要聚合）、查
询位置、键/区间、格式版本（`observation-lineage/1`），并对签名载荷计算
`lineage_sha256`。执行期间输入发生变化（完成后重哈希比对）时**拒绝给出成
功结果**（退出码 3）。输出为确定性 JSON：节点按 id、边按稳定键、游标按
（深度， 版本） 排序；精确的 RSS/耗时放在未签名的 `runtime_stats` 中。

`--max-depth` / `--max-nodes` 限制展开深度与节点数。达到限制时
`status="truncated"`，`truncation.cursor` 给出明确的恢复游标（版本 id、深
度、原因），部分结果绝不标为完整。

### 规模

版本、事务操作与读观察全部在磁盘 SQLite 索引上（复用审计器的版本/操作索
引，另有 `versions(txn_id, key)` 支持事务内先写后读解析），按 （深度， 版本
id) 确定性最佳优先遍历**目标可达子图**：不做全历史读写对笛卡尔积，也不把
所有节点载入内存（节点数受 `--max-nodes` 硬约束）。`stats` 报告实际的索
引命中（`version_lookups`、`range_probes`、`range_keys_scanned`）、展开节
点/边数、候选读操作检查数（`read_ops_examined`），`runtime_stats` 报告峰
值 RSS 与耗时。

### verify-lineage

`verify-lineage` 从原始历史重新加载并重建目标状态与观察依赖图，逐项核对：
输入哈希、查询范围、版本与读操作证据、节点/边、unknown 传播、排序、截断状
态与结果签名——**不信任待核验 JSON 内的任何结论**。改动输入、移除一条依
赖边、把 unknown 改成 complete、改变范围谓词证据、改动查询或打乱数组顺
序，都会失败并指出不一致位置（如 `lineage.edges[3].status`、
`input.sha256`）。退出码：0=核验通过，1=核验失败，3=无法加载。

```bash
python3 -m snapshot_audit lineage --input samples/lineage_demo.json \
    --commit-pos 3 --key 3 --out lin.json
python3 -m snapshot_audit verify-lineage --lineage lin.json
# 也可用 --input/--input-dir 显式指定原始历史（默认取血缘中绑定的路径），
# 用 --commit-pos/--key/--lo/--hi 断言待核验血缘回答的就是当前问题
```

## 事务负载画像（profile / verify-profile）

`profile` 是只读的负载分析，不判断读是否合法，也不统计错误数或血缘节点数——
那些是别的工具的指标，绝不换名充当负载。它从**单次采集的原始事务记录**回答
值班问题：什么时间段最忙、哪些实际键被频繁读取或更新、哪些闭区间被反复扫描。

### 时间窗

* 零点 = 所有**成功提交事务**中最早的 `end_time`；事务按其 `end_time` 到达
  归入固定宽度桶 `floor((end_time - anchor) / width)`，恰落在边界上的到达按
  有限精度规则吸附到最近桶（`boundary_snap_epsilon`）；
* 只统计 `committed` 事务，`aborted` 事务完全不参与（计数、热度、覆盖都不
  含）；
* 每个窗口列出：事务数与事务 id、操作总量、点读/范围读/写入三类计数，以及
  可解释的事务耗时汇总（`sum/min/max/mean` + 已解释与无法解释的事务）；
* **零事务窗口是已知的零（`complete`），与无法得知负载的窗口严格区分**：
  缺日志、操作证据冲突、扫描未结束等只把受影响窗口/指标标为 `unknown` 并附
  缺口清单，绝不当作零。无法归属窗口的事务（如缺 `end_time`）进
  `unbinned_transactions`，不静默丢弃。

### 两种含义不同的热度

1. **逐键事件热度 `key_heat`**：点读对目标键计一次；每个 `put`/`delete` 的写
   入键各计一次；范围读只把**已完成扫描完整返回的非墓碑键**计入逐键排名。
   top-N 按总事件数降序、键升序稳定排序（并列顺序确定）。
2. **范围扫描覆盖热度 `scan_coverage`**：一次完整扫描对其整个**闭区间**贡献
   一次覆盖，**即使返回零行也计数**；键域表示成互不重叠的分段，相邻且覆盖
   次数相同的分段合并。逐键返回数与扫描覆盖数是两种不同的量，分开报告。

### 区间合并的取舍与 oracle

生产实现把每个闭区间写成两个端点事件（`lo` 起点、`hi` 终点，终点在同坐标
的起点之后处理，精确保留闭端点）写入 SQLite，再按坐标顺序聚合成段——等价于
端点事件外部排序后扫描。**绝不按区间宽度生成逐键记录**：成本随区间/端点数
增长，不随键域宽度增长。测试里的独立 oracle（`tests/profile_oracle.py`）才
使用在有限小域上逐键枚举覆盖次数再合并等值相邻键的暴力法，并与生产实现逐项
交叉核对；生产代码不含该枚举法。`samples/sparse_int64_range.json` 用横跨整个
int64 的稀疏区间证明只有 6 个端点事件、4 个分段（见 `bench/PROFILE_BENCH.md`
的 `sparse-int64` 行）。

### 绑定、确定性、unknown 传播与核验

画像绑定输入规范路径、内容 SHA-256（目录按排序逐文件摘要聚合）、窗口参数与
格式版本（`workload-profile/1`），并对签名载荷计算 `profile_sha256`；重复运行
字节级一致，精确 RSS/耗时放在未签名的 `runtime_stats`。生成期间输入发生变化
（结束后重哈希不一致）时拒绝产出成功结果（退出码 3）。

`verify-profile` 从原始数据**独立重建**窗口归属、逐类计数、热度排名、覆盖分
段、unknown 传播与摘要，不以画像自带结论或其签名代替复算；再逐项 diff 签名
载荷。篡改任一计数、把事务移入相邻桶、删去覆盖分段、把 `unknown` 改成
`complete`、修改源文件或仅改签名都会失败并指出字段/位置（如
`windows[0].transaction_count`、`scan_coverage.segments`、`input.sha256`）。
资源测量字段按明确规则核验：峰值 RSS 允许 2 倍容差，墙钟时间仅校验存在与类
型、环境相关不作为硬失败。退出码：0=核验通过，1=核验失败，3=无法加载。

### 规模与内存

逐键热度在有界的内存计数器里聚合，超过上限才把**已聚合**的计数批量落盘
SQLite（不是每事件一行）；端点事件、事务-桶归属、已提交 id 集合都在磁盘表
上，操作按有序游标分批流式读取。`stats` 报告输入记录数、热度事件数、扫描区
间数、端点处理量、生成分段数与不同热度键数，`runtime_stats` 报告峰值 RSS 与
耗时。基准：`python3 bench/run_profile_bench.py`（`--quick` 跑最小集，结果写
`bench/PROFILE_BENCH.md`）。

## 规模与内存

* 版本、操作、边全部存于磁盘上的 SQLite（索引计划见审计输出 `index_plan`）；
* 区间从不展开整个 int64 域；点读/范围读的边生成是索引探测
  （`versions(key, commit_pos)`），实时边用排序扫描（O(n log n) 候选比较），
  不做无差别笛卡尔积；
* 边流式落盘；仅 SCC/拓扑排序在内存中进行，受 `--memory-budget-mb` 约束，
  超预算则 `unknown`（缺口为 `graph_exceeds_memory_budget`）；
* 审计输出记录候选比较数（`edge_candidates`、`version_lookups`、
  `range_keys_scanned`、`realtime_edges` 等）与峰值 RSS；
* 冲突证据列表过长时按 `--max-report` 截断，但总数精确、环见证不截断。

## 样例（samples/）

| 文件 | 预期 |
|---|---|
| `write_skew.json` | `invalid`：T1↔T2 双向反依赖环 |
| `phantom_cycle.json` | `invalid`：phantom 边参与的环 |
| `valid_serial.json` | `valid`：实时对齐的写偏，带重放证书 |
| `dirty_reads.json` | `invalid`：未来版本读 + 回滚版本读 |
| `incomplete_log.json` | `unknown`：声明完整范围缺 commit_pos |
| `lineage_demo.json` | `valid`；血缘演示：范围谓词、墓碑缺失观察、事务内先写后读 |
| `workload_profile_demo.json` | 负载画像样例：跨窗口桶、空结果扫描、墓碑、回滚事务排除、覆盖分段合并 |
| `sparse_int64_range.json` | 画像稀疏区间样例：横跨整个 int64 的闭区间只产生 2 个端点事件 |

## 复现基准

```bash
python3 bench/gen_scale.py --out /tmp/h --txns 2000 --keys 500 --reads 3 \
    --ranges 1 --range-width 40 --skew-pairs 300 --seed 7
python3 -m snapshot_audit audit --input-dir /tmp/h --out /tmp/h.result.json
python3 -m snapshot_audit verify-witness --input-dir /tmp/h \
    --witness /tmp/h.result.json
python3 bench/run_bench.py   # 汇总表写入 bench/BENCH.md
python3 bench/run_profile_bench.py   # 负载画像规模/资源表写入 bench/PROFILE_BENCH.md
```
