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
package com.netflix.conductor.rest.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

import com.netflix.conductor.rest.websocket.WorkflowStatusWebSocketHandler;

/**
 * Registers the WebSocket endpoint {@code /ws/workflow-status} for real-time workflow status
 * streaming.
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final WorkflowStatusWebSocketHandler workflowStatusWebSocketHandler;

    public WebSocketConfig(WorkflowStatusWebSocketHandler workflowStatusWebSocketHandler) {
        this.workflowStatusWebSocketHandler = workflowStatusWebSocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(workflowStatusWebSocketHandler, "/ws/workflow-status")
                .setAllowedOrigins("*");
    }
}
