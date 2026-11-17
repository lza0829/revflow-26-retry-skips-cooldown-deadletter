# 07-revflow（冲流台）

冲流台 — 正向与冲正流水净额归并：正向装载 → 冲正关联入账 → 净额归并落余额 → 未平/超额隔离 → 流水净额与余额互证。

**不是** 多源 Feed 归并，**不是** 作业依赖编排，**不是** CDC 变更流，**不是** 质检签发，**不是** 数仓分层建设，**不是** 交班过户。  
**也不是** 收银/退款 CRUD 后台、电商订单、报表 CSV 看板——主链是**流水管道与净额状态**，不是业务单据编辑台。

## 启动

```bash
# 后端
cd backend
pip install -r requirements.txt
set DATA_DIR=%CD%\data
set FIXTURES_DIR=%CD%\..\fixtures
uvicorn app.main:app --host 127.0.0.1 --port 10700

# 前端
cd frontend
npm install
npm run dev
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://127.0.0.1:5700 |
| 接口 | http://127.0.0.1:10700 |

## 主链

账户登记 → 装载正向流水 → 装载冲正并关联原单 → 跑净额归并（已配平落余额，未平/超额进隔离）→ 互证报告核对「流水净额 vs 账户余额」

## 未平隔离重试

隔离项不是无限重试，也不是永不重试——按**冷却 + 次数上限 + 死信**推进：

| 概念 | 落库位置 | 说明 |
| --- | --- | --- |
| 已重试次数 `retries` | `quarantine_retry_state` | 扩展表，不复用隔离行；调度内存不计数 |
| 次数上限 `max_retries` | 同上 | 默认 3，`QUARANTINE_RETRY_MAX` 可调 |
| 下次可重试 `next_at` | 同上 | 首次失败后 `base * factor^(n-1)`，封顶 `QUARANTINE_RETRY_COOLDOWN_MAX_SECONDS` |
| 冷却间隔 `cooldown_interval_seconds` | 同上 | 与 `next_at` 同一次写入；进度只此一处，隔离行不另存计数 |
| 状态 | 同上 | `冷却中` / `可重试` / `死信` / `已解除` |
| 每次尝试明细 | `quarantine_retry_attempts` | 触发方式、结果、原因、余额变动、次数与 next_at |
| 冷却证明 | `quarantine_cooldown_proofs` | 隔离键、间隔、剩余秒数、是否放行、是否死信、来源、评估时刻 |

### 门禁路径（本仓择一，写明）

采用 **「再配入口未到点 / 已死信直接拒绝，不改余额」**：

* 手动 `POST /api/quarantine/{id}/retry` 与自动 `POST /api/quarantine/retry-due` 共用同一闸门：
  冷却未满（`next_at` 未到）或已达死信上限，**在进入结算之前**返回 409，次数、`next_at`、余额都不动；
  拒绝本身也落一条 `allowed=0` 的冷却证明，可审计。
* 自动扫描只取 `state ∈ {冷却中, 可重试}` 且 `retries < max_retries` 且已到点的行；
  `死信` 行永远不进入配平，停在 `GET /api/quarantine?state=死信` 可查状态，敞口计入报告。
* 真正进入结算的尝试在 `BEGIN IMMEDIATE` 内一次成功或整体回滚：
  成功 = 次数 +1、补记原单与冲正为已归并、改余额、状态转 `已解除`；
  失败 = 次数 +1、按 `base * factor^(n-1)` 写新 `next_at` 与冷却间隔、余额不动；
  `retries` 达到 `max_retries` 的那次失败直接转 `死信`、`next_at` 置空。
* 次数与上次重试时刻 `last_attempt_at` 持久在扩展表，进程重启后仍可查；调度内存不计数。
* `GET /api/quarantine/retry-report` 与隔离列表、余额、互证报告同源读取：冷却中 / 可重试 / 死信 / 重试成功数与敞口。
* 死信不回灌余额；成功解除的敞口随余额一起移动，互证仍应一致。

### 冷却证明

`quarantine_cooldown_proofs` 是基线所无的独立证明实体，与扩展表**同一事务**写入，
不依赖任何内存状态。只读接口：

```bash
curl http://127.0.0.1:10700/api/quarantine/1/cooldown-proofs
```

隔离列表每行附 `cooldown_proof`（最新一条证明的 id、间隔、剩余、是否放行、是否死信）与
`cooldown_interval_seconds`，与扩展表当前行同源；未平隔离界面在列表列与「明细」中展示证明链。

```bash
# 自动扫描到期项（只碰冷却结束且未达上限的）
curl -X POST http://127.0.0.1:10700/api/quarantine/retry-due
# 手动重试单项（冷却中/死信返回 409）
curl -X POST http://127.0.0.1:10700/api/quarantine/1/retry
# 按隔离 id 只读查冷却证明
curl http://127.0.0.1:10700/api/quarantine/1/cooldown-proofs
```

## 界面约定

页面文案全部中文；入口：总览 / 账户 / 正向流水 / 冲正流水 / 归并台 / 未平隔离 / 净额互证。  
视觉：**左轨编号导航 + 右侧账册横线工作面**（Literata / JetBrains Mono，靛蓝强调）；**不是**交班台那种顶栏品牌+横向流程条。

## 与已有仓差异

| 仓 | 骨架 |
| --- | --- |
| Feedstitch | 多源解析→冲突隔离→水位 |
| Batchgate | 作业依赖→重试→水位 |
| Changeflow | 事件序→缺口→Sink 游标 |
| Checklane | 入检→规则→隔离签发 |
| Tiercraft | 贴源→明细→汇总→层间对账 |
| Shiftboard | 班次归属→交班互斥→批次过户 |
| **本仓** | **正向装载→冲正关联→净额归并→未平隔离→净额互证** |
