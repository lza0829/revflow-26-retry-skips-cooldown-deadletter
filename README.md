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
| 状态 | 同上 | `冷却中` / `可重试` / `死信` / `已解除` |
| 每次尝试明细 | `quarantine_retry_attempts` | 触发方式、结果、原因、余额变动、次数与 next_at |

* 重试前闸门同时校验冷却结束与次数未超上限；不满足返回 409 并写明原因（冷却剩余秒数 / 死信）。
* 一次尝试在 `BEGIN IMMEDIATE` 事务内完成：成功 = 次数 +1、补记原单与冲正为已归并、改余额、状态转 `已解除`；
  失败 = 次数 +1、写新 `next_at`、余额不动。计数推进不是 1 行即整体回滚，杜绝「次数没加但余额已变」。
* 达到上限进入 `死信`，自动扫描与手动重试都拒绝；可用 `GET /api/quarantine?state=死信` 单独查询。
* `GET /api/quarantine/retry-report` 与隔离列表、余额、互证报告同源读取：冷却中 / 可重试 / 死信 / 重试成功数与敞口。
* 死信不回灌余额；成功解除的敞口随余额一起移动，互证仍应一致。

```bash
# 自动扫描到期项（只碰冷却结束且未达上限的）
curl -X POST http://127.0.0.1:10700/api/quarantine/retry-due
# 手动重试单项（冷却中/死信返回 409）
curl -X POST http://127.0.0.1:10700/api/quarantine/1/retry
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
