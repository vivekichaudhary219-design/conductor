import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import { getBasename } from "../utils/helpers";

const WebSocketContext = createContext(null);

const RECONNECT_DELAY_MS = 3000;

/**
 * Provides a shared WebSocket connection to the conductor backend's
 * /ws/workflow-status endpoint. Components call `subscribe(workflowId, cb)`
 * to receive live status-change messages for a specific workflow.
 */
export function WebSocketProvider({ children }) {
  const wsRef = useRef(null);
  const [connected, setConnected] = useState(false);
  const mountedRef = useRef(true);
  const reconnectTimerRef = useRef(null);

  // workflowId -> Set<callback>
  const listenersRef = useRef(new Map());
  // workflowIds that should be re-subscribed after reconnect
  const pendingSubscriptionsRef = useRef(new Set());

  const sendSubscribe = useCallback((ws, workflowId) => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ action: "subscribe", workflowId }));
    }
  }, []);

  const connect = useCallback(() => {
    if (!mountedRef.current) return;

    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const basename = getBasename().replace(/\/$/, "");
    const url = `${protocol}//${window.location.host}${basename}/ws/workflow-status`;

    let ws;
    try {
      ws = new WebSocket(url);
    } catch {
      reconnectTimerRef.current = setTimeout(connect, RECONNECT_DELAY_MS);
      return;
    }
    wsRef.current = ws;

    ws.onopen = () => {
      if (!mountedRef.current) return;
      setConnected(true);
      pendingSubscriptionsRef.current.forEach((id) => sendSubscribe(ws, id));
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        const { workflowId } = data;
        if (workflowId) {
          listenersRef.current.get(workflowId)?.forEach((cb) => cb(data));
        }
        listenersRef.current.get("*")?.forEach((cb) => cb(data));
      } catch {
        // ignore malformed messages
      }
    };

    ws.onclose = () => {
      if (!mountedRef.current) return;
      setConnected(false);
      reconnectTimerRef.current = setTimeout(connect, RECONNECT_DELAY_MS);
    };

    ws.onerror = () => ws.close();
  }, [sendSubscribe]);

  useEffect(() => {
    mountedRef.current = true;
    connect();
    return () => {
      mountedRef.current = false;
      clearTimeout(reconnectTimerRef.current);
      wsRef.current?.close();
    };
  }, [connect]);

  /**
   * Subscribe to status-change events for a workflow.
   * Returns an unsubscribe function to call on cleanup.
   */
  const subscribe = useCallback(
    (workflowId, callback) => {
      if (!listenersRef.current.has(workflowId)) {
        listenersRef.current.set(workflowId, new Set());
      }
      listenersRef.current.get(workflowId).add(callback);
      pendingSubscriptionsRef.current.add(workflowId);
      sendSubscribe(wsRef.current, workflowId);

      return () => {
        const cbs = listenersRef.current.get(workflowId);
        if (cbs) {
          cbs.delete(callback);
          if (cbs.size === 0) {
            listenersRef.current.delete(workflowId);
            pendingSubscriptionsRef.current.delete(workflowId);
            if (wsRef.current?.readyState === WebSocket.OPEN) {
              wsRef.current.send(
                JSON.stringify({ action: "unsubscribe", workflowId })
              );
            }
          }
        }
      };
    },
    [sendSubscribe]
  );

  return (
    <WebSocketContext.Provider value={{ connected, subscribe }}>
      {children}
    </WebSocketContext.Provider>
  );
}

export function useWebSocketContext() {
  return useContext(WebSocketContext);
}
