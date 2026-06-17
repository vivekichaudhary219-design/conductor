/*
 * Copyright 2025 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.rest.websocket;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * WebSocket handler that maintains client sessions and broadcasts workflow status change events.
 *
 * <p>Clients connect to {@code /ws/workflow-status} and send a JSON subscription message:
 *
 * <pre>{"action":"subscribe","workflowId":"&lt;id&gt;"}</pre>
 *
 * The server then pushes status-change events as JSON:
 *
 * <pre>{"type":"WORKFLOW_STATUS","workflowId":"...","workflowName":"...","status":"...","event":"..."}</pre>
 */
@Component
public class WorkflowStatusWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log =
            LoggerFactory.getLogger(WorkflowStatusWebSocketHandler.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    /** workflowId → sessions subscribed to that workflow. */
    private final Map<String, Set<WebSocketSession>> workflowSubscriptions =
            new ConcurrentHashMap<>();

    /** Sessions subscribed to all workflows (wildcard). */
    private final Set<WebSocketSession> globalSubscriptions =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        log.debug("WebSocket connection established: {}", session.getId());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            Map<String, Object> payload =
                    objectMapper.readValue(message.getPayload(), Map.class);
            String action = (String) payload.get("action");
            String workflowId = (String) payload.get("workflowId");

            if ("subscribe".equals(action)) {
                if (workflowId == null || "*".equals(workflowId)) {
                    globalSubscriptions.add(session);
                } else {
                    workflowSubscriptions
                            .computeIfAbsent(
                                    workflowId,
                                    k -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
                            .add(session);
                }
                log.debug("Session {} subscribed to workflowId={}", session.getId(), workflowId);
            } else if ("unsubscribe".equals(action)) {
                if (workflowId != null) {
                    Set<WebSocketSession> sessions = workflowSubscriptions.get(workflowId);
                    if (sessions != null) {
                        sessions.remove(session);
                    }
                } else {
                    globalSubscriptions.remove(session);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse WebSocket message from {}: {}", session.getId(), e.getMessage());
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        globalSubscriptions.remove(session);
        workflowSubscriptions.values().forEach(sessions -> sessions.remove(session));
        log.debug("WebSocket connection closed: {} status={}", session.getId(), status);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.warn("WebSocket transport error for session {}: {}", session.getId(), exception.getMessage());
    }

    /**
     * Broadcasts a workflow status-change event to all sessions subscribed to the given workflow
     * and to any global (wildcard) subscribers.
     */
    public void broadcastWorkflowUpdate(
            String workflowId, String workflowName, String status, String event) {
        try {
            Map<String, String> payload =
                    Map.of(
                            "type", "WORKFLOW_STATUS",
                            "workflowId", workflowId,
                            "workflowName", workflowName,
                            "status", status,
                            "event", event);
            TextMessage message = new TextMessage(objectMapper.writeValueAsString(payload));

            Set<WebSocketSession> specific =
                    workflowSubscriptions.getOrDefault(workflowId, Collections.emptySet());
            for (WebSocketSession s : specific) {
                sendSafe(s, message);
            }
            for (WebSocketSession s : globalSubscriptions) {
                sendSafe(s, message);
            }
        } catch (Exception e) {
            log.error("Failed to build broadcast message for workflowId={}", workflowId, e);
        }
    }

    private void sendSafe(WebSocketSession session, TextMessage message) {
        if (session.isOpen()) {
            try {
                synchronized (session) {
                    session.sendMessage(message);
                }
            } catch (IOException e) {
                log.warn("Failed to send message to session {}: {}", session.getId(), e.getMessage());
            }
        }
    }
}
