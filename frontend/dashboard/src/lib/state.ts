"use client";

import {
  createContext,
  useContext,
  useReducer,
  type Dispatch,
  type ReactNode,
} from "react";
import { createElement } from "react";
import type {
  DashboardAction,
  DashboardState,
  TreeNode,
} from "./types";

// NOTE: the fabricated demo-run fixture (a hardcoded 0.62→0.85 score arc
// presented as a live run) was deliberately deleted. When the backend is
// unreachable the dashboard shows an explicit disconnected state instead
// of fabricating a healthy run. See documents/REPOSITIONING_PLAN.md §4.0.

const initialState: DashboardState = {
  mode: "live",
  run: null,
  tree: [],
  iterations: [],
  logEntries: [],
  forkEvents: [],
  filters: { activeFilter: "all", searchQuery: "" },
  contextTab: "chart",
  selectedNode: null,
  selectedLogLine: null,
  sseConnected: false,
  latestCheckpointId: null,
  lastError: null,
};

function reducer(state: DashboardState, action: DashboardAction): DashboardState {
  switch (action.type) {
    case "SET_MODE":
      return { ...state, mode: action.payload };
    case "SET_RUN":
      return { ...state, run: action.payload };
    case "SET_TREE":
      return { ...state, tree: action.payload };
    case "ADD_TREE_NODE": {
      const existing = state.tree.find(n => n.candidate === action.payload.candidate);
      const merged: TreeNode = existing
        ? {
            ...existing,
            ...action.payload,
            parent_candidate_name:
              action.payload.parent_candidate_name ?? existing.parent_candidate_name,
            iteration: action.payload.iteration || existing.iteration,
            hypothesis: action.payload.hypothesis ?? existing.hypothesis,
            axis: action.payload.axis ?? existing.axis,
            delta: action.payload.delta ?? existing.delta,
          }
        : action.payload;
      const without = state.tree.filter(n => n.candidate !== action.payload.candidate);
      return { ...state, tree: [...without, merged] };
    }
    case "SET_CHECKPOINT_ID":
      return {
        ...state,
        tree: state.tree.map(node => (
          node.candidate === action.payload.candidate
            ? { ...node, checkpointId: action.payload.checkpointId }
            : node
        )),
      };
    case "APPLY_FRONTIER_UPDATE":
      return {
        ...state,
        tree: state.tree.map(node => ({
          ...node,
          status:
            action.payload.bestCandidate && node.candidate === action.payload.bestCandidate
              ? "best"
              : action.payload.bestCandidate && node.status === "best"
                ? (action.payload.frontier.includes(node.candidate) ? "accepted" : "rejected")
                : action.payload.frontier.includes(node.candidate)
                  ? "accepted"
                  : node.status,
          delta:
            action.payload.bestCandidate && node.candidate === action.payload.bestCandidate
              ? action.payload.delta
              : node.delta,
        })),
      };
    case "SET_ITERATIONS":
      return { ...state, iterations: action.payload };
    case "ADD_LOG_ENTRY": {
      const existingIdx = state.logEntries.findIndex(entry => entry.id === action.payload.id);
      if (existingIdx === -1) {
        return { ...state, logEntries: [...state.logEntries, action.payload] };
      }
      const next = [...state.logEntries];
      next[existingIdx] = action.payload;
      return { ...state, logEntries: next };
    }
    case "SET_LOG_ENTRIES":
      return { ...state, logEntries: action.payload };
    case "ADD_FORK_EVENT":
      return { ...state, forkEvents: [...state.forkEvents, action.payload] };
    case "SET_FILTER":
      return { ...state, filters: { ...state.filters, ...action.payload } };
    case "SET_CONTEXT_TAB":
      return { ...state, contextTab: action.payload };
    case "SELECT_NODE":
      return { ...state, selectedNode: action.payload };
    case "SELECT_LOG_LINE":
      return { ...state, selectedLogLine: action.payload };
    case "SET_SSE_CONNECTED":
      return { ...state, sseConnected: action.payload };
    case "ADD_ITERATION": {
      const without = state.iterations.filter(
        i => !(i.candidateName === action.payload.candidateName && i.iteration === action.payload.iteration),
      );
      return { ...state, iterations: [...without, action.payload] };
    }
    case "SET_CHECKPOINT":
      return { ...state, latestCheckpointId: action.payload };
    case "SET_ERROR":
      return { ...state, lastError: action.payload };
    case "CANCEL_BRANCH": {
      const threadId = action.payload;
      return {
        ...state,
        tree: state.tree.map(n =>
          n.threadId === threadId ? { ...n, status: "rejected" as const } : n,
        ),
      };
    }
    case "RESET":
      return { ...initialState };
    default:
      return state;
  }
}

const StateContext = createContext<DashboardState | undefined>(undefined);
const DispatchContext = createContext<Dispatch<DashboardAction> | undefined>(undefined);

export function DashboardProvider({
  children,
  initial,
}: {
  children: ReactNode;
  initial?: Partial<DashboardState>;
}) {
  const [state, dispatch] = useReducer(reducer, { ...initialState, ...initial });
  return createElement(
    StateContext.Provider,
    { value: state },
    createElement(DispatchContext.Provider, { value: dispatch }, children),
  );
}

export function useDashboard(): DashboardState {
  const ctx = useContext(StateContext);
  if (!ctx) throw new Error("useDashboard must be used inside <DashboardProvider />");
  return ctx;
}

export function useDashboardDispatch(): Dispatch<DashboardAction> {
  const ctx = useContext(DispatchContext);
  if (!ctx) throw new Error("useDashboardDispatch must be used inside <DashboardProvider />");
  return ctx;
}

export { initialState };
export type { DashboardState, DashboardAction } from "./types";
