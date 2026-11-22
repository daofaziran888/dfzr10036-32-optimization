import { request } from './client'
import type { HandoffAction, HandoffState } from '@/data/handoff'

async function readAction(response: Response): Promise<HandoffAction> {
  const payload = (await response.json()) as HandoffAction
  if (!response.ok) throw new Error(payload.message || `交接动作失败：${response.status}`)
  return payload
}

export async function loadHandoffState(): Promise<HandoffState> {
  const response = await request('/api/handoff/state')
  if (!response.ok) throw new Error('映射交接账本读取失败')
  return (await response.json()) as HandoffState
}

export async function runHandoffAction(action: string): Promise<HandoffAction> {
  const response = await request(`/api/handoff/actions/${action}`, { method: 'POST' })
  return readAction(response)
}
