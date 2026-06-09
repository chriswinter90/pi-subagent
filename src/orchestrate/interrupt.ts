import { appendRunEvent, readRunRecord, recordInterruptRequest, type RunAttemptRecord, type RunRecord } from "../artifacts/index.ts";
import { isTerminalStatus } from "./status.ts";

export interface InterruptRunOptions {
  cwd?: string;
  runId: string;
  runsDir?: string;
  attemptId?: string;
  /** @deprecated v1 compatibility alias. */
  taskId?: string;
  reason?: string;
  signal?: NodeJS.Signals;
  escalateAfterMs?: number;
  killAfterMs?: number;
}

export interface InterruptRunResult {
  status: "interrupt-requested" | "not-found" | "already-terminal" | "unsupported";
  runId: string;
  signal: NodeJS.Signals;
  interruptedAttempts: string[];
  unsupportedAttempts: string[];
  /** @deprecated v1 compatibility alias. */
  interruptedTasks: string[];
  /** @deprecated v1 compatibility alias. */
  unsupportedTasks: string[];
  record: RunRecord | null;
}

function sendProcessSignal(attempt: RunAttemptRecord, signal: NodeJS.Signals): boolean {
  const pid = attempt.process?.pid;
  if (pid === undefined) return false;
  try {
    const target = process.platform === "win32" ? pid : -(attempt.process?.processGroupId ?? pid);
    process.kill(target, signal);
    return true;
  } catch {
    try {
      process.kill(pid, signal);
      return true;
    } catch {
      return false;
    }
  }
}

function runningAttempts(record: RunRecord, targetAttemptId?: string): RunAttemptRecord[] {
  return record.attempts.filter((attempt) => {
    if (targetAttemptId !== undefined && attempt.attemptId !== targetAttemptId) return false;
    return attempt.status === "running" || attempt.status === "pending";
  });
}

async function escalate(options: InterruptRunOptions, signal: NodeJS.Signals): Promise<void> {
  const record = await readRunRecord(options).catch(() => null);
  if (record === null || isTerminalStatus(record.status)) return;
  for (const attempt of runningAttempts(record, options.attemptId ?? options.taskId)) sendProcessSignal(attempt, signal);
  await appendRunEvent(options, { type: "run.interrupt_requested", status: record.status, message: `interrupt escalation ${signal}`, data: { signal } }).catch(() => undefined);
}

function result(status: InterruptRunResult["status"], runId: string, signal: NodeJS.Signals, interruptedAttempts: string[], unsupportedAttempts: string[], record: RunRecord | null): InterruptRunResult {
  return {
    status,
    runId,
    signal,
    interruptedAttempts,
    unsupportedAttempts,
    interruptedTasks: interruptedAttempts,
    unsupportedTasks: unsupportedAttempts,
    record,
  };
}

export async function interruptRun(options: InterruptRunOptions): Promise<InterruptRunResult> {
  const signal = options.signal ?? "SIGINT";
  const record = await readRunRecord(options);
  if (record === null) {
    return result("not-found", options.runId, signal, [], [], null);
  }
  if (isTerminalStatus(record.status)) {
    return result("already-terminal", options.runId, signal, [], [], record);
  }

  const candidates = runningAttempts(record, options.attemptId ?? options.taskId);
  const interruptedAttempts: string[] = [];
  const unsupportedAttempts: string[] = [];
  for (const attempt of candidates) {
    if (sendProcessSignal(attempt, signal)) interruptedAttempts.push(attempt.attemptId);
    else unsupportedAttempts.push(attempt.attemptId);
  }

  if (interruptedAttempts.length === 0) {
    await appendRunEvent(options, { type: "run.interrupt_requested", status: record.status, message: "interrupt unsupported: no interruptable process metadata", data: { signal, unsupportedAttempts } });
    return result("unsupported", options.runId, signal, interruptedAttempts, unsupportedAttempts, record);
  }

  const updated = await recordInterruptRequest(options, signal, options.reason ?? null);
  await appendRunEvent(options, {
    type: "run.interrupt_requested",
    status: updated.status,
    message: `interrupt requested with ${signal}`,
    data: { signal, interruptedAttempts, unsupportedAttempts, reason: options.reason ?? null },
  });

  const termDelay = options.escalateAfterMs ?? 1_000;
  const killDelay = options.killAfterMs ?? 3_000;
  setTimeout(() => void escalate(options, "SIGTERM"), termDelay).unref?.();
  setTimeout(() => void escalate(options, "SIGKILL"), killDelay).unref?.();

  return result("interrupt-requested", options.runId, signal, interruptedAttempts, unsupportedAttempts, updated);
}
