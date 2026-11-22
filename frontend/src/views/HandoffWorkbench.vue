<template>
  <section class="page handoff-page" data-testid="handoff-workbench">
    <header class="page-head">
      <div>
        <h2>映射交接工作台</h2>
        <p class="page-desc">
          解析映射改版期间，用转换批次交付数据核对维表关联与去重窗口各自接纳的事件，再核定日志解析可提交位点。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" data-testid="handoff-reset" @click="runAction('reset')">重置场景</button>
        <button class="btn" type="button" data-testid="handoff-arrive" @click="runAction('arrive_new_batch')">新批到达</button>
        <button class="btn" type="button" data-testid="handoff-pause-ack" @click="runAction('pause_old_ack')">暂停旧批去重回执</button>
        <button class="btn primary" type="button" data-testid="handoff-resend" @click="runAction('resend_both')">重送两版</button>
        <button class="btn" type="button" data-testid="handoff-receive-ack" @click="runAction('receive_old_ack')">收到旧批回执</button>
        <button class="btn" type="button" data-testid="handoff-recover" @click="runAction('recover')">中断恢复</button>
        <button class="btn" type="button" data-testid="handoff-commit" @click="runAction('commit')">核定提交位点</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="handoff-error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="handoff-notice">{{ notice }}</p>

    <div v-if="state" class="stat-row">
      <article class="stat-card">
        <span class="stat-label">解析已提交位点</span>
        <strong class="stat-value" data-testid="committed-position">{{ state.committed_position }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">解析可提交位点</span>
        <strong class="stat-value" data-testid="committable-position">{{ state.committable_position }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">去重待回执批次</span>
        <strong class="stat-value" data-testid="dedup-pending-batches">{{ dedupPendingCount }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">两版重送次数</span>
        <strong class="stat-value" data-testid="resend-count">{{ state.resend.total }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">重叠重送去重</span>
        <strong class="stat-value" data-testid="overlap-duplicates">{{ state.resend.overlap_duplicate_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">交接一致性</span>
        <strong class="stat-value" data-testid="consistency-state">{{ state.consistency.consistent ? '一致' : '有缺口' }}</strong>
      </article>
    </div>

    <template v-if="state">
      <section class="panel" data-testid="version-section">
        <h3>映射版本与批次归属</h3>
        <div class="version-grid">
          <article
            v-for="range in state.mapping_ranges"
            :key="range.version"
            class="version-card"
            :data-testid="`version-${range.version}`"
          >
            <header>
              <strong>{{ range.name }}</strong>
              <span class="chip">{{ range.version }}</span>
            </header>
            <dl>
              <dt>归属批次</dt>
              <dd>{{ range.batch_id }}</dd>
              <dt>维表关联接纳</dt>
              <dd data-testid="`version-${range.version}-dim`">{{ formatRanges(range.dim_join) }}</dd>
              <dt>去重窗口接纳</dt>
              <dd data-testid="`version-${range.version}-dedup`">{{ formatRanges(range.dedup_stream) }}</dd>
              <dt>两侧确认</dt>
              <dd>{{ formatRanges(range.confirmed) }}</dd>
              <dt>本版重送</dt>
              <dd>{{ versionResend(range.version) }} 次</dd>
            </dl>
          </article>
        </div>
        <table class="data-table compact">
          <thead>
            <tr><th>批次</th><th>版本</th><th>到达区间</th><th>归属区间</th><th>重叠</th><th>交付次数</th><th>状态</th></tr>
          </thead>
          <tbody>
            <tr v-for="batch in state.batches" :key="batch.batch_id" :data-testid="`batch-${batch.batch_id}`">
              <td>{{ batch.batch_id }}</td>
              <td>{{ batch.version_label }}</td>
              <td>{{ formatRanges([batch.arrived_positions]) }}</td>
              <td>{{ formatRanges([batch.assigned_positions]) }}</td>
              <td>{{ batch.carried_over_positions ? formatRanges([batch.carried_over_positions]) : '—' }}</td>
              <td>{{ batch.delivery_attempts }}</td>
              <td>{{ batch.status }}</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="workbench-grid" data-testid="handoff-columns">
        <article class="panel" data-testid="dim-column">
          <h3>维表关联正在处理的订单</h3>
          <p class="panel-hint">按订单键核对，顺序保持递增。</p>
          <table class="data-table compact">
            <thead><tr><th>订单键</th><th>接纳位点</th><th>最后位点</th><th>条数</th><th>顺序</th></tr></thead>
            <tbody>
              <tr v-for="order in state.workbench.dim_processing_orders" :key="order.order_key">
                <td>{{ order.order_key }}</td>
                <td>{{ formatRanges(order.positions) }}</td>
                <td>{{ order.last_position }}</td>
                <td>{{ order.accepted_count }}</td>
                <td>{{ order.order_ok ? '正常' : '异常' }}</td>
              </tr>
              <tr v-if="!state.workbench.dim_processing_orders.length">
                <td colspan="5" class="empty-state">维表关联尚无接纳记录</td>
              </tr>
            </tbody>
          </table>
        </article>

        <article class="panel" data-testid="dedup-column">
          <h3>去重仍缺的批次</h3>
          <p class="panel-hint">回执未到期间，排队记录与实际接纳都可追溯。</p>
          <ul class="batch-list">
            <li v-for="item in state.workbench.dedup_missing_batches" :key="item.batch_id" :data-testid="`dedup-missing-${item.batch_id}`">
              <strong>{{ item.batch_id }}</strong>
              <span class="chip">{{ item.version_label }}</span>
              <div>缺口：{{ formatRanges(item.missing_positions) }}</div>
              <div>排队：{{ item.queued_positions.length ? formatRanges(item.queued_positions) : '—' }}</div>
              <div>状态：{{ item.state }}</div>
            </li>
            <li v-if="!state.workbench.dedup_missing_batches.length" class="empty-state">去重窗口没有缺口</li>
          </ul>
          <h4 v-if="state.workbench.dedup_pending_queues.length">排队明细</h4>
          <p v-if="state.workbench.dedup_pending_queues.length" class="queue-line">
            {{ queueSummary }}
          </p>
        </article>

        <article class="panel" data-testid="parse-column">
          <h3>解析可提交位点</h3>
          <p class="panel-hint">只提交从 101 起两侧都按所属版本确认过的连续前缀。</p>
          <p class="big-position">
            <span>已提交 {{ state.committed_position }}</span>
            <span>可提交 {{ state.committable_position }}</span>
            <span>终点 {{ state.final_position }}</span>
          </p>
          <div class="backlog-box" v-if="state.workbench.backlog_segments.length" data-testid="backlog-segment">
            <strong>交接积压段</strong>
            <div v-for="segment in state.workbench.backlog_segments" :key="`${segment.side}-${segment.batch_id}`">
              {{ segment.side_label }} · {{ segment.batch_id }} · 位点 {{ formatRanges(segment.positions) }} · {{ segment.reason }}
            </div>
          </div>
          <p v-else class="panel-hint" data-testid="backlog-empty">当前没有积压段。</p>
          <p class="panel-hint">缺口：{{ gapSummary }}</p>
        </article>
      </section>

      <section class="panel" data-testid="side-section">
        <h3>两侧实际接纳范围</h3>
        <table class="data-table compact">
          <thead>
            <tr><th>交接侧</th><th>接纳区间</th><th>旧映射</th><th>新映射</th><th>排队</th><th>最后位点</th><th>条数</th></tr>
          </thead>
          <tbody>
            <tr v-for="side in sideList" :key="side.side" :data-testid="`side-${side.side}`">
              <td>{{ side.label }}</td>
              <td>{{ formatRanges(side.accepted_ranges) }}</td>
              <td>{{ formatRanges(side.acceptance_ranges_by_version['map-v1']) }}</td>
              <td>{{ formatRanges(side.acceptance_ranges_by_version['map-v2']) }}</td>
              <td>{{ side.queued_ranges.length ? formatRanges(side.queued_ranges) : '—' }}</td>
              <td>{{ lastPosition(side) }}</td>
              <td>{{ side.accepted_count }}</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="panel" data-testid="receipt-section">
        <h3>接纳凭据核对</h3>
        <form class="filter-bar" @submit.prevent>
          <label class="filter-item">
            <span>交接侧</span>
            <select v-model="recordFilter.side" data-testid="receipt-filter-side">
              <option value="">全部</option>
              <option value="dim_join">维表关联</option>
              <option value="dedup_stream">去重窗口</option>
            </select>
          </label>
          <label class="filter-item">
            <span>映射版本</span>
            <select v-model="recordFilter.version" data-testid="receipt-filter-version">
              <option value="">全部</option>
              <option value="map-v1">旧映射</option>
              <option value="map-v2">新映射</option>
            </select>
          </label>
          <label class="filter-item">
            <span>位点</span>
            <input v-model="recordFilter.position" data-testid="receipt-filter-position" placeholder="如 111" />
          </label>
        </form>
        <table class="data-table compact">
          <thead>
            <tr>
              <th>回执号</th><th>交接侧</th><th>位点</th><th>实际位点</th><th>订单键</th>
              <th>批次</th><th>映射版本</th><th>交付号</th><th>说明</th><th>接纳时间</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in filteredReceipts" :key="row.receipt_id" :data-testid="`receipt-${row.side}-${row.position}`">
              <td>{{ row.receipt_id }}</td>
              <td>{{ row.side_label }}</td>
              <td>{{ row.position }}</td>
              <td>{{ row.actual_position }}</td>
              <td>{{ row.order_key }}#{{ row.order_seq }}</td>
              <td>{{ row.batch_id }}</td>
              <td>{{ row.version_label }}</td>
              <td>{{ row.delivery_id }}</td>
              <td>{{ row.reason }}</td>
              <td>{{ row.accepted_at }}</td>
            </tr>
            <tr v-if="!filteredReceipts.length">
              <td colspan="10" class="empty-state">没有符合条件的接纳凭据</td>
            </tr>
          </tbody>
        </table>
        <p class="panel-hint">
          重送去重 {{ state.resend.duplicate_count }} 条（重叠 {{ state.resend.overlap_duplicate_count }} 条），
          每侧每条只接纳一次。
        </p>
      </section>

      <section class="panel" data-testid="event-section">
        <h3>交接日志</h3>
        <ol class="event-list">
          <li v-for="event in state.events" :key="event.seq" :data-testid="`event-${event.seq}`">
            <span class="event-time">{{ event.at }}</span>
            <strong>{{ event.action }}</strong>
            <span>{{ event.detail }}</span>
          </li>
        </ol>
      </section>
    </template>

    <p v-else-if="!errorMessage" class="panel-hint">交接账本加载中…</p>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import { loadHandoffState, runHandoffAction } from '@/api/handoff'
import type { HandoffState, Range, SideState } from '@/data/handoff'

const state = ref<HandoffState | null>(null)
const errorMessage = ref('')
const notice = ref('')
const recordFilter = reactive({ side: '', version: '', position: '' })

const sideList = computed(() => Object.values(state.value?.sides ?? {}))
const dedupPendingCount = computed(
  () => state.value?.sides.dedup_stream?.missing_batches.length ?? 0,
)
const queueSummary = computed(() => {
  const queues = state.value?.workbench.dedup_pending_queues ?? []
  if (!queues.length) return '—'
  const byBatch = new Map<string, number[]>()
  for (const row of queues) {
    byBatch.set(row.batch_id, [...(byBatch.get(row.batch_id) ?? []), row.position])
  }
  return [...byBatch.entries()]
    .map(([batch, positions]) => `${batch}：${positions.sort((a, b) => a - b).join('、')}`)
    .join('；')
})
const gapSummary = computed(() => {
  const dim = state.value?.sides.dim_join?.accepted_ranges ?? []
  const dedup = state.value?.sides.dedup_stream?.accepted_ranges ?? []
  return `关联缺 ${formatNumbers(missingFrom(dim))}；去重缺 ${formatNumbers(missingFrom(dedup))}`
})
const filteredReceipts = computed(() => {
  const rows = state.value?.acceptance_receipts ?? []
  return rows.filter((row) => {
    if (recordFilter.side && row.side !== recordFilter.side) return false
    if (recordFilter.version && row.version !== recordFilter.version) return false
    if (recordFilter.position.trim() && String(row.position) !== recordFilter.position.trim()) return false
    return true
  })
})

function formatRanges(ranges: Range[] | undefined): string {
  if (!ranges || !ranges.length) return '—'
  return ranges.map(([start, end]) => (start === end ? `${start}` : `${start}-${end}`)).join('、')
}

function formatNumbers(positions: number[]): string {
  return positions.length ? formatRanges(toRanges(positions)) : '无'
}

function toRanges(positions: number[]): Range[] {
  const ordered = [...new Set(positions)].sort((a, b) => a - b)
  if (!ordered.length) return []
  const ranges: Range[] = []
  let start = ordered[0]
  let previous = ordered[0]
  for (const position of ordered.slice(1)) {
    if (position === previous + 1) {
      previous = position
      continue
    }
    ranges.push([start, previous])
    start = previous = position
  }
  ranges.push([start, previous])
  return ranges
}

function missingFrom(ranges: Range[]): number[] {
  const accepted = new Set<number>()
  for (const [start, end] of ranges) {
    for (let pos = start; pos <= end; pos += 1) accepted.add(pos)
  }
  const missing: number[] = []
  for (let pos = 101; pos <= 130; pos += 1) {
    if (!accepted.has(pos)) missing.push(pos)
  }
  return missing
}

function versionResend(version: string): number {
  return state.value?.resend.by_version?.[version] ?? 0
}

function lastPosition(side: SideState): number | string {
  return side.accepted_positions.length ? side.accepted_positions[side.accepted_positions.length - 1] : '—'
}

async function refresh() {
  state.value = await loadHandoffState()
}

async function runAction(action: string) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = await runHandoffAction(action)
    if (result.state) state.value = result.state
    else await refresh()
    notice.value = result.message
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交接动作失败'
  }
}

onMounted(async () => {
  try {
    await refresh()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交接账本加载失败'
  }
})
</script>
