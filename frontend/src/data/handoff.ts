/** 解析映射改版交接的前端类型：与后端 /api/handoff/state 对齐。 */

export type Range = [number, number]

export type AcceptanceRecord = {
  receipt_id: string
  side: string
  side_label: string
  position: number
  actual_position: string
  order_key: string
  order_seq: number
  batch_id: string
  version: string
  version_label: string
  delivery_id: string
  state: string
  reason: string
  accepted_at: string
}

export type QueueRecord = {
  queue_id: string
  side: string
  side_label: string
  position: number
  actual_position: string
  order_key: string
  batch_id: string
  version: string
  delivery_id: string
  state: string
  reason: string
  queued_at: string
}

export type VersionSummary = {
  version: string
  name: string
  batch_id: string
  by_side: Record<string, Range[]>
  confirmed: Range[]
  confirmed_count: number
  total_positions: number
  resend_count: number
}

export type MappingRange = {
  version: string
  name: string
  batch_id: string
  dim_join: Range[]
  dedup_stream: Range[]
  confirmed: Range[]
}

export type BatchRecord = {
  batch_id: string
  label: string
  version: string
  version_label: string
  arrived_positions: Range
  assigned_positions: Range
  carried_over_positions?: Range
  resend_count: number
  delivery_attempts: number
  status: string
}

export type SideState = {
  side: string
  label: string
  accepted_positions: number[]
  accepted_ranges: Range[]
  accepted_count: number
  queued_ranges: Range[]
  queued_count: number
  missing_batches: {
    batch_id: string
    version: string
    version_label: string
    missing_positions: Range[]
    queued_positions: Range[]
    state: string
  }[]
  order_states: {
    order_key: string
    positions: Range[]
    last_position: number
    accepted_count: number
    order_ok: boolean
  }[]
  acceptance_ranges_by_version: Record<string, Range[]>
}

export type Workbench = {
  dim_processing_orders: {
    order_key: string
    positions: Range[]
    last_position: number
    accepted_count: number
    order_ok: boolean
  }[]
  dedup_missing_batches: SideState['missing_batches']
  dedup_pending_queues: {
    batch_id: string
    version: string
    position: number
    order_key: string
    state: string
    reason: string
  }[]
  parse_committable_position: number
  parse_committed_position: number
  backlog_segments: BacklogSegment[]
}

export type BacklogSegment = {
  side: string
  side_label: string
  batch_id: string
  version: string
  positions: Range[]
  reason: string
}

export type HandoffState = {
  stream: {
    positions: string
    committed_start: number
    committed_position: number
    committable_position: number
    final_position: number
    committed_offset: string
  }
  committed_position: number
  committable_position: number
  final_position: number
  versions: VersionSummary[]
  mapping_ranges: MappingRange[]
  batches: BatchRecord[]
  sides: Record<string, SideState>
  workbench: Workbench
  backlog_segments: BacklogSegment[]
  resend: {
    total: number
    by_version: Record<string, number>
    by_batch: { batch_id: string; version: string; resend_count: number; delivery_attempts: number }[]
    duplicate_count: number
    overlap_duplicate_count: number
  }
  duplicates: {
    duplicate_id: string
    side: string
    position: number
    batch_id: string
    version: string
    delivery_id: string
    reason: string
    at: string
  }[]
  queues: QueueRecord[]
  receipts: AcceptanceRecord[]
  acceptance_receipts: AcceptanceRecord[]
  deliveries: {
    delivery_id: string
    batch_id: string
    version: string
    side: string
    positions: Range
    position_list: number[]
    attempt: number
    delivered_at: string
  }[]
  delivery_receipts: {
    receipt_id: string
    side: string
    batch_id: string
    version: string
    positions: Range
    accepted_count: number
    received_at: string
  }[]
  pending_receipts: {
    receipt_id: string
    side: string
    batch_id: string
    version: string
    positions: Range
    state: string
    opened_at: string
  }[]
  consistency: {
    consistent: boolean
    checked_positions: number
    issues: { type: string; [key: string]: unknown }[]
    duplicate_acceptances: number
  }
  scenario: {
    old_dedup_ack_paused: boolean
    old_ack_received: boolean
    new_batch_arrived: boolean
    interrupted: boolean
  }
  events: { seq: number; action: string; detail: string; at: string }[]
}

export type HandoffAction = {
  ok: boolean
  message: string
  state?: HandoffState
  report?: Record<string, unknown>
}
