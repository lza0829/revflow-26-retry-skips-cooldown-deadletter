<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { getJson, postJson } from '../api'

const rows = ref([])
const report = ref(null)
const attempts = ref({})
const proofs = ref({})
const openDetail = ref(null)
const filter = ref('')
const includeReleased = ref(false)
const err = ref('')
const msg = ref('')
const busy = ref(false)
const nowMs = ref(Date.now())

const stateOptions = ['冷却中', '可重试', '死信', '已解除']

const shown = computed(() => {
  if (!filter.value) return rows.value
  return rows.value.filter((r) => r.state === filter.value)
})

const counts = computed(() => {
  const base = { 冷却中: 0, 可重试: 0, 死信: 0, 已解除: 0 }
  for (const r of rows.value) base[r.state] = (base[r.state] || 0) + 1
  return base
})

async function load() {
  const params = new URLSearchParams()
  if (includeReleased.value) params.set('include_released', 'true')
  const qs = params.toString()
  rows.value = await getJson('/api/quarantine' + (qs ? `?${qs}` : ''))
  report.value = await getJson('/api/quarantine/retry-report')
}

onMounted(async () => {
  try {
    await load()
  } catch (e) {
    err.value = String(e.message || e)
  }
  timer = setInterval(() => {
    nowMs.value = Date.now()
  }, 1000)
})

let timer = null
onBeforeUnmount(() => timer && clearInterval(timer))

// 冷却倒计时归零后从服务端取一次最新状态（是否放行以服务端为准），避免每秒打接口
let lastDueCheck = 0
watch(nowMs, async () => {
  const dueNow = rows.value.some(
    (r) => r.state === '冷却中' && remainingSeconds(r) <= 0,
  )
  if (!dueNow) {
    lastDueCheck = 0
    return
  }
  if (Date.now() - lastDueCheck < 5000) return
  lastDueCheck = Date.now()
  try {
    await load()
  } catch (e) {
    err.value = String(e.message || e)
  }
})

function remainingSeconds(row) {
  if (!row.next_at || row.state === '死信' || row.state === '已解除') return 0
  const due = Date.parse(row.next_at)
  if (Number.isNaN(due)) return row.cooldown_seconds_remaining || 0
  return Math.max(0, Math.ceil((due - nowMs.value) / 1000))
}

function cooldownText(row) {
  if (row.state === '已解除') return '—'
  if (row.state === '死信') return '不再重试'
  const sec = remainingSeconds(row)
  if (sec <= 0) return '已到期'
  if (sec >= 3600) return `${Math.ceil(sec / 3600)} 小时`
  if (sec >= 60) return `${Math.ceil(sec / 60)} 分钟`
  return `${sec} 秒`
}

function intervalText(row) {
  const sec = Number(row.cooldown_interval_seconds || 0)
  if (row.state === '已解除' || row.state === '死信') return '—'
  return `${sec}s`
}

function yesNo(v) {
  return v ? '是' : '否'
}

function stateTag(row) {
  if (row.state === '死信') return 'bad'
  if (row.state === '冷却中') return 'warn'
  if (row.state === '已解除') return 'on'
  return ''
}

async function refresh() {
  await load()
  if (openDetail.value != null) {
    await loadAttempts(openDetail.value)
    await loadProofs(openDetail.value)
  }
}

async function retryOne(row) {
  busy.value = true
  err.value = ''
  msg.value = ''
  try {
    const r = await postJson(`/api/quarantine/${row.id}/retry`)
    msg.value = r.outcome === '成功'
      ? `重试成功：${r.entry_key} 已配平离开隔离（第 ${r.retries} 次）`
      : `重试失败：${r.reason}（第 ${r.retries}/${r.max_retries} 次，下次 ${r.next_at || '—'}）`
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function retryDue() {
  busy.value = true
  err.value = ''
  msg.value = ''
  try {
    const r = await postJson('/api/quarantine/retry-due')
    msg.value = `自动扫描：尝试 ${r.attempted} 项，成功 ${r.succeeded}，失败 ${r.failed}，拒绝 ${r.rejected}`
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function toggleDetail(row) {
  if (openDetail.value === row.id) {
    openDetail.value = null
    return
  }
  openDetail.value = row.id
  await loadAttempts(row.id)
  await loadProofs(row.id)
}

async function loadAttempts(id) {
  try {
    attempts.value = { ...attempts.value, [id]: await getJson(`/api/quarantine/${id}/attempts`) }
  } catch (e) {
    err.value = String(e.message || e)
  }
}

async function loadProofs(id) {
  try {
    proofs.value = { ...proofs.value, [id]: await getJson(`/api/quarantine/${id}/cooldown-proofs`) }
  } catch (e) {
    err.value = String(e.message || e)
  }
}

function yuan(cents) {
  return (Number(cents || 0) / 100).toFixed(2)
}
</script>

<template>
  <div>
    <h1>未平隔离</h1>
    <p class="lead">
      冲正无原单、账户不一致或超额等未能配平的流水。再配入口先验冷却与死信：
      未满冷却、已达死信上限一律拒绝且不动余额；次数、上次重试时刻与冷却证明均落库可查。
    </p>

    <div class="stats">
      <div class="stat">
        <p class="k">冷却中</p>
        <p class="v">{{ counts['冷却中'] }}</p>
      </div>
      <div class="stat">
        <p class="k">死信</p>
        <p class="v">{{ counts['死信'] }}</p>
      </div>
      <div class="stat">
        <p class="k">重试成功</p>
        <p class="v">{{ report ? report.retry_success_count : counts['已解除'] }}</p>
      </div>
    </div>

    <p v-if="report" class="note-strip">
      隔离敞口 ¥ {{ yuan(report.quarantine_exposure_cents) }}
      （其中死信 ¥ {{ yuan(report.dead_letter_exposure_cents) }}）
      · 余额 ¥ {{ yuan(report.account_balance_cents) }} vs 流水净额 ¥ {{ yuan(report.ledger_net_cents) }}
      · <span :class="report.consistent ? 'ok' : 'msg'">{{ report.consistent ? '同源一致' : '同源不一致' }}</span>
    </p>

    <div class="row-actions">
      <button class="btn" :disabled="busy" @click="retryDue">重试到期项</button>
      <label class="toggle">
        <input v-model="includeReleased" type="checkbox" @change="refresh" />
        含已解除
      </label>
      <label class="toggle">
        状态
        <select v-model="filter">
          <option value="">全部</option>
          <option v-for="s in stateOptions" :key="s" :value="s">{{ s }}</option>
        </select>
      </label>
    </div>
    <p v-if="msg" class="ok">{{ msg }}</p>
    <p v-if="err" class="msg">{{ err }}</p>

    <div class="panel">
      <table>
        <thead>
          <tr>
            <th>流水键</th><th>账户</th><th>原因</th><th>金额（元）</th>
            <th>重试</th><th>状态</th><th>冷却间隔</th><th>下次可重试</th>
            <th>最近尝试</th><th>冷却证明</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <template v-for="q in shown" :key="q.id">
            <tr>
              <td>{{ q.entry_key }}</td>
              <td>{{ q.account_code }}</td>
              <td><span class="tag warn">{{ q.reason }}</span></td>
              <td>{{ yuan(q.amount_cents) }}</td>
              <td class="mono">{{ q.retries }}/{{ q.max_retries }}</td>
              <td>
                <span class="tag" :class="stateTag(q)">{{ q.state }}</span>
                <span v-if="q.dead_letter" class="tag bad dl">死信</span>
              </td>
              <td class="mono">{{ intervalText(q) }}</td>
              <td>{{ cooldownText(q) }}</td>
              <td>
                <template v-if="q.last_attempt_at">
                  {{ q.last_attempt_at }}
                  <span class="tag" :class="q.last_outcome === '成功' ? 'on' : 'bad'">{{ q.last_outcome }}</span>
                </template>
                <template v-else>—</template>
              </td>
              <td class="proof-cell">
                <template v-if="q.cooldown_proof && q.cooldown_proof.proof_id">
                  <span class="tag" :class="q.cooldown_proof.allowed ? 'on' : 'warn'">
                    {{ q.cooldown_proof.allowed ? '放行' : '不放行' }}
                  </span>
                  <span class="tag" :class="q.cooldown_proof.dead_letter ? 'bad' : ''">
                    {{ q.cooldown_proof.dead_letter ? '死信' : '未死信' }}
                  </span>
                  <p class="proof-meta mono">#{{ q.cooldown_proof.proof_id }} · 余 {{ q.cooldown_proof.remaining_seconds }}s</p>
                </template>
                <template v-else>—</template>
              </td>
              <td>
                <button
                  class="btn secondary mini"
                  :disabled="busy || !q.can_retry"
                  :title="q.block_reason || '可重试'"
                  @click="retryOne(q)"
                >重试</button>
                <button class="btn secondary mini" @click="toggleDetail(q)">明细</button>
                <p v-if="!q.can_retry && q.block_reason" class="block-reason">{{ q.block_reason }}</p>
              </td>
            </tr>
            <tr v-if="openDetail === q.id" class="detail-row">
              <td colspan="11">
                <p v-if="q.last_reason" class="detail-reason">最近失败原因：{{ q.last_reason }}</p>
                <table class="inner">
                  <thead>
                    <tr><th>#</th><th>次数</th><th>触发</th><th>结果</th><th>原因</th><th>余额变动（元）</th><th>之后状态</th><th>时间</th></tr>
                  </thead>
                  <tbody>
                    <tr v-for="a in (attempts[q.id] || [])" :key="a.id">
                      <td>{{ a.id }}</td>
                      <td class="mono">{{ a.attempt_no }}</td>
                      <td>{{ a.trigger }}</td>
                      <td><span class="tag" :class="a.outcome === '成功' ? 'on' : 'bad'">{{ a.outcome }}</span></td>
                      <td>{{ a.reason || '—' }}</td>
                      <td>{{ yuan(a.balance_delta_cents) }}</td>
                      <td>{{ a.state_after }}</td>
                      <td>{{ a.attempted_at }}</td>
                    </tr>
                    <tr v-if="!(attempts[q.id] || []).length"><td colspan="8">尚无重试记录</td></tr>
                  </tbody>
                </table>

                <p class="proof-title">冷却证明（只读，与扩展表同源）</p>
                <table class="inner">
                  <thead>
                    <tr><th>#</th><th>隔离键</th><th>间隔（秒）</th><th>下次可重试</th><th>剩余（秒）</th><th>冷却有效</th><th>放行</th><th>死信</th><th>状态</th><th>来源</th><th>评估时刻</th></tr>
                  </thead>
                  <tbody>
                    <tr v-for="p in (proofs[q.id] || [])" :key="p.id">
                      <td>{{ p.id }}</td>
                      <td class="mono">{{ p.entry_key }}</td>
                      <td class="mono">{{ p.cooldown_interval_seconds }}</td>
                      <td>{{ p.next_at || '—' }}</td>
                      <td class="mono">{{ p.remaining_seconds }}</td>
                      <td>{{ yesNo(p.cooldown_active) }}</td>
                      <td><span class="tag" :class="p.allowed ? 'on' : 'warn'">{{ yesNo(p.allowed) }}</span></td>
                      <td><span class="tag" :class="p.dead_letter ? 'bad' : ''">{{ yesNo(p.dead_letter) }}</span></td>
                      <td>{{ p.state }}</td>
                      <td>{{ p.source }}</td>
                      <td>{{ p.evaluated_at }}</td>
                    </tr>
                    <tr v-if="!(proofs[q.id] || []).length"><td colspan="11">尚无冷却证明</td></tr>
                  </tbody>
                </table>
              </td>
            </tr>
          </template>
          <tr v-if="!shown.length"><td colspan="11">暂无隔离项（先跑归并）</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<style scoped>
.mono { font-family: var(--mono); }
.toggle {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
  color: var(--muted);
}
.toggle select {
  font: inherit;
  font-size: 13px;
  padding: 4px 6px;
  border: 1px solid var(--line);
  background: #fff;
  color: var(--ink);
}
.mini { padding: 5px 10px; font-size: 12px; margin-right: 6px; }
.dl { margin-left: 4px; }
.block-reason {
  margin: 6px 0 0;
  font-size: 11px;
  color: var(--warn);
  max-width: 20em;
  line-height: 1.5;
}
.detail-row td { background: rgba(36, 87, 197, 0.03); }
.detail-reason { margin: 0 0 10px; color: var(--warn); font-size: 12.5px; }
.inner th, .inner td { padding: 7px 6px; font-size: 12.5px; }
.proof-title { margin: 16px 0 8px; font-size: 12.5px; color: var(--muted); }
.proof-cell { white-space: nowrap; }
.proof-meta { margin: 4px 0 0; font-size: 11px; color: var(--muted); }
</style>
