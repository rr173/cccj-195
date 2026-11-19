# snapshot-audit — 快照事务历史的只读离线审计器

导入**已经发生**的快照事务历史，回答两个问题：

1. 某次分析（读取）是否读到了**不合法版本**（未来版本 / 回滚版本 / 未提交版本 /
   过期版本 / 值不符 / 缺键 / 多键）；
2. 各次读取虽然都符合快照语义，但是否**无法按声明版本序串行解释**
   （依赖图中存在环）。

审计器**不调度事务、不修改原库、不通过重试修复历史**。证据不足时输出
`unknown` 及缺口清单，绝不把缺证据当作"无异常"；只有证据足够的确证违例才报
`invalid`。

## 运行

```bash
# 审计（退出码：0=valid 1=invalid 2=unknown 3=输入错误）
python3 -m snapshot_audit audit --input samples/write_skew.json
python3 -m snapshot_audit audit --input-dir /path/to/history_dir --out result.json

# 独立核验见证（不信任审计器存的结论，从原始历史重新推导）
python3 -m snapshot_audit verify-witness --input samples/write_skew.json \
    --witness result.json

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

## 复现基准

```bash
python3 bench/gen_scale.py --out /tmp/h --txns 2000 --keys 500 --reads 3 \
    --ranges 1 --range-width 40 --skew-pairs 300 --seed 7
python3 -m snapshot_audit audit --input-dir /tmp/h --out /tmp/h.result.json
python3 -m snapshot_audit verify-witness --input-dir /tmp/h \
    --witness /tmp/h.result.json
python3 bench/run_bench.py   # 汇总表写入 bench/BENCH.md
```
