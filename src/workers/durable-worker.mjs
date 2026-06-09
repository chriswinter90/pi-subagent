#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { createJiti } from "jiti";

const payloadPath = process.argv[2];
if (!payloadPath) {
  console.error("durable worker missing payload path");
  process.exit(2);
}

const jiti = createJiti(import.meta.url, { interopDefault: false });
const [{ runSubagentTask }, artifacts] = await Promise.all([
  jiti.import("../orchestrate/run.ts"),
  jiti.import("../artifacts/index.ts"),
]);

const payload = JSON.parse(await readFile(payloadPath, "utf8"));
const { input, cwd, runId, attemptId } = payload;
const heartbeatMs = Math.max(50, Number.parseInt(process.env.PI_SUBAGENT_HEARTBEAT_MS ?? "5000", 10) || 5000);
const runRef = { cwd, runId, runsDir: input?.runsDir };
const workerProcessGroupId = process.platform === "win32" ? undefined : process.pid;
await artifacts.updateAttemptProcess({
  ...runRef,
  attemptId,
  process: {
    pid: process.pid,
    processGroupId: workerProcessGroupId,
    command: "pi-subagent durable-worker",
    workerPid: process.pid,
    workerProcessGroupId,
  },
}).catch(() => undefined);
const heartbeat = setInterval(() => {
  void artifacts.recordAttemptHeartbeat({ ...runRef, attemptId }).catch(() => undefined);
}, heartbeatMs);
heartbeat.unref?.();
try {
  await runSubagentTask({ input: { ...input, async: false, onComplete: undefined }, cwd, runId, attemptId });
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  try {
    const store = await artifacts.createAttemptArtifactStore({ cwd, runId, attemptId, runsDir: input?.runsDir });
    const stderr = await store.writeTextArtifact("stderr", `${message}\n`);
    const worker = store.refFor("worker");
    const result = await store.writeResult({
      backend: payload.backend ?? "headless",
      status: "failed",
      failureKind: "internal",
      cwd,
      startedAt: payload.startedAt ?? new Date().toISOString(),
      completedAt: new Date().toISOString(),
      workspace: { mode: "shared", cwd },
      sandbox: { enabled: Boolean(input?.sandbox) },
      exitCode: null,
      signal: null,
      artifacts: [worker, stderr],
      correlationId: input?.correlationId,
      metadata: { contextLengthExceeded: false },
    });
    await artifacts.finishAttemptFromResult({ cwd, runId, runsDir: input?.runsDir }, result).catch(() => undefined);
    await artifacts.appendRunEvent({ cwd, runId, runsDir: input?.runsDir }, { type: "attempt.failed", attemptId, status: "failed", message }).catch(() => undefined);
    await artifacts.appendRunEvent({ cwd, runId, runsDir: input?.runsDir }, { type: "run.failed", status: "failed", message }).catch(() => undefined);
  } catch (writeError) {
    console.error(writeError instanceof Error ? writeError.stack ?? writeError.message : String(writeError));
  }
  process.exitCode = 1;
} finally {
  clearInterval(heartbeat);
}
