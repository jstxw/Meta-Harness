'use client';

// Phase 4.2 — seed replay viewer (the rare artifact).
//
// A DST run is a pure function of its seed, so a trace file IS the bug
// report. This page scrubs an exported trace: every scheduling step,
// every injected fault, and the exact step an invariant violation
// surfaces, labeled with its ID. No backend, no DB, no LLM calls —
// bundled traces load from /replays/*.json and arbitrary seeds load
// from a file exported with:
//
//   cd backend && uv run python -m sim.export --seed N --mode fenced_store -o trace.json

import { useCallback, useEffect, useMemo, useState } from 'react';
import Link from 'next/link';

type Frame = {
  step: number;
  t: number;
  action: string;
  fault: boolean;
  workers: {
    id: string;
    crashed: boolean;
    stalled: boolean;
    skew: number;
    branch: string | null;
    fence: number | null;
    position: number | null;
  }[];
  branches: {
    id: string;
    status: string;
    gen: number;
    owner: string | null;
    lease_expired: boolean;
  }[];
  file_log_len: number;
  events: number;
  new_violations: string[];
};

type Trace = {
  seed: number;
  mode: string;
  ok: boolean;
  steps: number;
  violations: string[];
  frames: Frame[];
};

type ReplayIndexEntry = { file: string; label: string; note: string };

const STATUS_COLOR: Record<string, string> = {
  created: 'text-amber',
  running: 'text-cyan',
  completed: 'text-green',
  failed: 'text-red',
  cancelled: 'text-text-ghost',
};

export default function ReplayPage() {
  const [index, setIndex] = useState<ReplayIndexEntry[]>([]);
  const [trace, setTrace] = useState<Trace | null>(null);
  const [sourceLabel, setSourceLabel] = useState<string>('');
  const [cursor, setCursor] = useState(0);
  const [playing, setPlaying] = useState(false);

  useEffect(() => {
    fetch('/replays/index.json')
      .then(r => r.json())
      .then(data => setIndex(Array.isArray(data.replays) ? data.replays : []))
      .catch(() => setIndex([]));
  }, []);

  const loadBundled = useCallback(async (entry: ReplayIndexEntry) => {
    const data = (await (await fetch(`/replays/${entry.file}`)).json()) as Trace;
    setTrace(data);
    setSourceLabel(entry.label);
    setCursor(0);
    setPlaying(false);
  }, []);

  const loadFile = useCallback(async (file: File) => {
    const data = JSON.parse(await file.text()) as Trace;
    setTrace(data);
    setSourceLabel(`${file.name} (local export)`);
    setCursor(0);
    setPlaying(false);
  }, []);

  useEffect(() => {
    if (!playing || !trace) return;
    if (cursor >= trace.frames.length - 1) {
      setPlaying(false);
      return;
    }
    const timer = setTimeout(() => setCursor(c => c + 1), 120);
    return () => clearTimeout(timer);
  }, [playing, cursor, trace]);

  const frame = trace?.frames[cursor] ?? null;
  const violationSteps = useMemo(
    () =>
      (trace?.frames ?? [])
        .map((f, i) => (f.new_violations.length ? i : -1))
        .filter(i => i >= 0),
    [trace],
  );
  const faultSteps = useMemo(
    () =>
      (trace?.frames ?? []).map((f, i) => (f.fault ? i : -1)).filter(i => i >= 0),
    [trace],
  );
  const violationsSoFar = useMemo(() => {
    if (!trace) return [];
    return trace.frames.slice(0, cursor + 1).flatMap(f => f.new_violations);
  }, [trace, cursor]);

  return (
    <div className="h-full flex flex-col bg-panel overflow-hidden">
      <div className="h-12 flex items-center justify-between px-6 bg-header border-b border-border shrink-0">
        <Link href="/" className="text-cyan text-sm font-semibold tracking-[3px] uppercase hover:text-text-hi transition-colors">
          META-HARNESS · SEED REPLAY
        </Link>
        <span className="text-[9px] text-text-ghost uppercase tracking-wide">
          deterministic — a seed fully determines this timeline
        </span>
      </div>

      <div className="flex-1 overflow-y-auto p-6 flex flex-col gap-5 text-[11px]">
        {/* source picker */}
        <div className="flex flex-wrap items-center gap-2">
          {index.map(entry => (
            <button
              key={entry.file}
              onClick={() => loadBundled(entry)}
              title={entry.note}
              className={`text-[9px] uppercase tracking-wide border px-2.5 py-1 rounded transition-colors ${
                sourceLabel === entry.label
                  ? 'text-cyan border-cyan/50'
                  : 'text-text-mid border-border hover:text-text-hi'
              }`}
            >
              {entry.label}
            </button>
          ))}
          <label className="text-[9px] uppercase tracking-wide border border-border px-2.5 py-1 rounded text-text-mid hover:text-text-hi cursor-pointer transition-colors">
            load exported trace…
            <input
              type="file"
              accept="application/json"
              className="hidden"
              onChange={e => e.target.files?.[0] && loadFile(e.target.files[0])}
            />
          </label>
        </div>

        {!trace && (
          <div className="text-text-ghost text-[10px] leading-relaxed max-w-xl">
            Pick a bundled trace, or export any seed yourself — the file is a pure
            function of the seed:
            <pre className="mt-2 p-2 bg-header rounded text-[10px]">cd backend && uv run python -m sim.export --seed N --mode fenced_store -o trace.json</pre>
          </div>
        )}

        {trace && frame && (
          <>
            {/* verdict */}
            <div className="flex items-center gap-3">
              <span className="text-text-hi font-semibold">seed {trace.seed}</span>
              <span className="text-text-mid">{trace.mode}</span>
              <span className={trace.ok ? 'text-green' : 'text-red'}>
                {trace.ok ? 'all invariants hold' : `${trace.violations.length} violation(s)`}
              </span>
              <span className="ml-auto text-text-ghost">
                step {frame.step} / {trace.steps} · t={frame.t.toFixed(1)}s
              </span>
            </div>

            {/* scrubber + markers */}
            <div>
              <div className="relative h-3 mb-1">
                {faultSteps.map(i => (
                  <span
                    key={`f${i}`}
                    title={trace.frames[i].action}
                    onClick={() => setCursor(i)}
                    className="absolute top-0 w-1 h-3 bg-amber cursor-pointer"
                    style={{ left: `${(i / Math.max(trace.frames.length - 1, 1)) * 100}%` }}
                  />
                ))}
                {violationSteps.map(i => (
                  <span
                    key={`v${i}`}
                    title={trace.frames[i].new_violations.join('; ')}
                    onClick={() => setCursor(i)}
                    className="absolute top-0 w-1.5 h-3 bg-red cursor-pointer"
                    style={{ left: `${(i / Math.max(trace.frames.length - 1, 1)) * 100}%` }}
                  />
                ))}
              </div>
              <div className="flex items-center gap-3">
                <button
                  onClick={() => setPlaying(p => !p)}
                  className="text-[9px] text-cyan uppercase tracking-wide border border-border px-2 py-0.5 rounded hover:border-cyan/40"
                >
                  {playing ? 'pause' : 'play'}
                </button>
                <input
                  type="range"
                  min={0}
                  max={trace.frames.length - 1}
                  value={cursor}
                  onChange={e => setCursor(Number(e.target.value))}
                  className="flex-1 accent-cyan"
                />
              </div>
              <div className="mt-1 flex gap-4 text-[9px] text-text-ghost">
                <span><span className="inline-block w-2 h-2 bg-amber mr-1" />injected fault</span>
                <span><span className="inline-block w-2 h-2 bg-red mr-1" />invariant violation</span>
              </div>
            </div>

            {/* current action */}
            <div
              className={`p-3 rounded bg-header border font-mono text-[11px] ${
                frame.new_violations.length
                  ? 'border-red text-red'
                  : frame.fault
                    ? 'border-amber/60 text-amber'
                    : 'border-border text-text-hi'
              }`}
            >
              {frame.action}
              {frame.new_violations.map(v => (
                <div key={v} className="mt-1 text-red font-semibold">✗ {v}</div>
              ))}
            </div>

            {/* state at this step */}
            <div className="grid grid-cols-2 gap-4">
              <div>
                <div className="text-[9px] font-semibold text-text-hi uppercase tracking-wide mb-2">Workers</div>
                <div className="flex flex-col gap-1.5">
                  {frame.workers.map(w => (
                    <div key={w.id} className="flex items-center gap-2 p-1.5 rounded bg-header border border-border font-mono text-[10px]">
                      <span className={`w-1.5 h-1.5 rounded-full ${w.crashed ? 'bg-red' : w.stalled ? 'bg-amber' : 'bg-green'}`} />
                      <span className="text-text-hi">{w.id}</span>
                      {w.crashed && <span className="text-red">dead</span>}
                      {w.stalled && <span className="text-amber">stalled</span>}
                      {w.branch && (
                        <span className="text-text-mid">
                          → {w.branch} fence {w.fence} @ iter {w.position}
                        </span>
                      )}
                      {w.skew !== 0 && <span className="text-text-ghost">skew {w.skew}s</span>}
                    </div>
                  ))}
                </div>
              </div>
              <div>
                <div className="text-[9px] font-semibold text-text-hi uppercase tracking-wide mb-2">Branches</div>
                <div className="flex flex-col gap-1.5">
                  {frame.branches.map(b => (
                    <div key={b.id} className="flex items-center gap-2 p-1.5 rounded bg-header border border-border font-mono text-[10px]">
                      <span className={STATUS_COLOR[b.status] ?? 'text-text-mid'}>{b.status}</span>
                      <span className="text-text-hi">{b.id}</span>
                      <span className="text-cyan">gen {b.gen}</span>
                      <span className="text-text-ghost">{b.owner ?? '—'}</span>
                      {b.lease_expired && <span className="text-amber">lease expired</span>}
                    </div>
                  ))}
                </div>
                <div className="mt-2 text-[9px] text-text-ghost font-mono">
                  log rows: {frame.file_log_len} · events: {frame.events}
                </div>
              </div>
            </div>

            {/* violations so far */}
            {violationsSoFar.length > 0 && (
              <div>
                <div className="text-[9px] font-semibold text-red uppercase tracking-wide mb-1">
                  Violations up to this step
                </div>
                {violationsSoFar.map(v => (
                  <div key={v} className="text-[10px] font-mono text-red">✗ {v}</div>
                ))}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
