'use client';
import { useCallback, useEffect, useRef, useState } from 'react';
import { useParams } from 'next/navigation';
import {
  killWorker,
  listBranches,
  listWorkers,
  type BranchInfo,
  type WorkerInfo,
} from '@/lib/api';

// Phase 4.1 + 4.3: the recovery path, watchable live. Kill a worker
// mid-branch and this panel shows: lease goes stale → another worker
// claims with an INCREMENTED FENCE → execution resumes from the last
// checkpoint. The fence generation is the mechanism — it gets a badge,
// and every increment is logged.

const POLL_MS = 1500;

const STATUS_STYLE: Record<BranchInfo['status'], string> = {
  created: 'text-amber border-amber/40',
  running: 'text-cyan border-cyan/40',
  completed: 'text-green border-green/40',
  failed: 'text-red border-red/40',
  cancelled: 'text-text-ghost border-border',
};

function agoLabel(iso: string | null): string {
  if (!iso) return '—';
  const ms = Date.now() - new Date(iso).getTime();
  if (ms < 0) return 'now';
  if (ms < 1000) return '<1s ago';
  if (ms < 60_000) return `${Math.round(ms / 1000)}s ago`;
  return `${Math.round(ms / 60_000)}m ago`;
}

function leaseLabel(branch: BranchInfo): { text: string; expired: boolean } {
  if (branch.status !== 'running') return { text: '—', expired: false };
  if (!branch.lease_expires_at) return { text: 'no lease', expired: true };
  const ms = new Date(branch.lease_expires_at).getTime() - Date.now();
  if (ms <= 0) return { text: 'EXPIRED — reclaimable', expired: true };
  return { text: `${(ms / 1000).toFixed(1)}s left`, expired: false };
}

export function ChaosPanel() {
  const params = useParams<{ run_id: string }>();
  const runId = params.run_id;
  const [branches, setBranches] = useState<BranchInfo[]>([]);
  const [workers, setWorkers] = useState<WorkerInfo[]>([]);
  const [chaosEnabled, setChaosEnabled] = useState(false);
  const [narrative, setNarrative] = useState<string[]>([]);
  const [killNote, setKillNote] = useState<string | null>(null);
  const fencesRef = useRef<Map<string, number>>(new Map());
  const [flashing, setFlashing] = useState<Set<string>>(new Set());

  const poll = useCallback(async () => {
    const [branchRows, workerData] = await Promise.all([
      listBranches(runId),
      listWorkers(),
    ]);
    // Fence increments are the story — log every one we observe.
    const notes: string[] = [];
    const nowFlashing = new Set<string>();
    for (const b of branchRows) {
      const prev = fencesRef.current.get(b.branch_id);
      if (prev !== undefined && b.lease_generation > prev) {
        notes.push(
          `${new Date().toLocaleTimeString()} · ${b.branch_id}: fence ${prev} → ${b.lease_generation}` +
            (b.lease_owner ? ` (claimed by ${b.lease_owner})` : ' (cancelled or reclaim pending)'),
        );
        nowFlashing.add(b.branch_id);
      }
      fencesRef.current.set(b.branch_id, b.lease_generation);
    }
    if (notes.length) {
      setNarrative(prevNotes => [...notes, ...prevNotes].slice(0, 20));
      setFlashing(nowFlashing);
      setTimeout(() => setFlashing(new Set()), 2500);
    }
    setBranches(branchRows);
    setWorkers(workerData.workers);
    setChaosEnabled(workerData.chaosEnabled);
  }, [runId]);

  useEffect(() => {
    let cancelled = false;
    const tick = () => {
      if (!cancelled) poll().catch(() => undefined);
    };
    tick();
    const timer = setInterval(tick, POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [poll]);

  const handleKill = async (workerId: string) => {
    setKillNote(`sending SIGKILL to ${workerId}…`);
    const result = await killWorker(workerId);
    setKillNote(`${workerId}: ${result.detail}`);
  };

  return (
    <div className="flex flex-col gap-5 text-[11px]">
      <div>
        <div className="flex items-center gap-2">
          <span className="text-[10px] font-semibold text-text-hi uppercase tracking-wide">Workers</span>
          {!chaosEnabled && (
            <span className="text-[9px] text-text-ghost">
              kill switch off — start the API with META_HARNESS_CHAOS=1
            </span>
          )}
        </div>
        <div className="mt-2 flex flex-col gap-2">
          {workers.length === 0 && (
            <div className="text-text-ghost text-[10px]">
              No workers registered. Start one: <code>uv run meta-harness worker</code>
            </div>
          )}
          {workers.map(w => (
            <div key={w.worker_id} className="flex items-center gap-3 p-2 rounded bg-header border border-border">
              <span className={`w-1.5 h-1.5 rounded-full ${w.alive === false ? 'bg-red' : 'bg-green'}`} />
              <span className="text-text-hi font-mono">{w.worker_id}</span>
              <span className="text-text-ghost">pid {w.pid} · seen {agoLabel(w.last_seen)}</span>
              {chaosEnabled && w.local && w.alive !== false && (
                <button
                  onClick={() => handleKill(w.worker_id)}
                  className="ml-auto text-[9px] text-red uppercase tracking-wide border border-red/40 px-2 py-0.5 rounded hover:bg-red/10 transition-colors"
                  title="SIGKILL this worker mid-branch and watch another one recover the lease"
                >
                  kill -9
                </button>
              )}
            </div>
          ))}
        </div>
        {killNote && <div className="mt-1.5 text-[9px] text-amber">{killNote}</div>}
      </div>

      <div>
        <span className="text-[10px] font-semibold text-text-hi uppercase tracking-wide">Branch leases</span>
        <div className="mt-2 flex flex-col gap-2">
          {branches.length === 0 && (
            <div className="text-text-ghost text-[10px]">
              No durable branches for this run yet — fork a checkpoint to create one.
            </div>
          )}
          {branches.map(b => {
            const lease = leaseLabel(b);
            return (
              <div
                key={b.branch_id}
                className={`p-2.5 rounded bg-header border transition-colors duration-700 ${
                  flashing.has(b.branch_id) ? 'border-amber' : 'border-border'
                }`}
              >
                <div className="flex items-center gap-2">
                  <span className={`px-1.5 py-0.5 rounded border text-[9px] uppercase tracking-wide ${STATUS_STYLE[b.status]}`}>
                    {b.status}
                  </span>
                  <span className="text-text-hi font-mono truncate">{b.thread_id}</span>
                  <span
                    className={`ml-auto shrink-0 px-1.5 py-0.5 rounded text-[9px] font-mono border ${
                      flashing.has(b.branch_id)
                        ? 'text-amber border-amber/60'
                        : 'text-cyan border-cyan/30'
                    }`}
                    title="Fencing token: every (re)claim increments it; writes carrying an older generation are rejected"
                  >
                    gen {b.lease_generation}
                  </span>
                </div>
                <div className="mt-1.5 flex items-center gap-3 text-[9px] text-text-mid">
                  <span>owner: {b.lease_owner ?? '—'}</span>
                  <span className={lease.expired && b.status === 'running' ? 'text-amber' : ''}>
                    lease: {lease.text}
                  </span>
                  {b.error && <span className="text-red truncate">error: {b.error}</span>}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {narrative.length > 0 && (
        <div>
          <span className="text-[10px] font-semibold text-text-hi uppercase tracking-wide">Fence log</span>
          <div className="mt-2 flex flex-col gap-1">
            {narrative.map((line, idx) => (
              <div key={`${line}-${idx}`} className="text-[9px] font-mono text-text-mid">{line}</div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
