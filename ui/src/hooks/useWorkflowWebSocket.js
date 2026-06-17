import { useCallback, useEffect } from "react";
import { useWebSocketContext } from "../plugins/WebSocketProvider";

/**
 * Subscribes to real-time workflow status changes via WebSocket.
 *
 * @param {string} workflowId - the workflow to watch
 * @param {function} onUpdate - called with the status-change message object
 */
export function useWorkflowWebSocket(workflowId, onUpdate) {
  const ctx = useWebSocketContext();

  const stableOnUpdate = useCallback(
    (msg) => onUpdate(msg),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [workflowId]
  );

  useEffect(() => {
    if (!workflowId || !ctx?.subscribe) return;
    const unsubscribe = ctx.subscribe(workflowId, stableOnUpdate);
    return unsubscribe;
  }, [workflowId, ctx, stableOnUpdate]);

  return { connected: ctx?.connected ?? false };
}
