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

import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import com.netflix.conductor.core.listener.WorkflowStatusListener;
import com.netflix.conductor.model.WorkflowModel;

/**
 * {@link WorkflowStatusListener} that broadcasts real-time workflow status changes to connected
 * WebSocket clients via {@link WorkflowStatusWebSocketHandler}.
 *
 * <p>Marked {@code @Primary} so Spring injects this bean (rather than the default stub) into
 * {@code WorkflowExecutorOps}.
 */
@Component
@Primary
public class WorkflowWebSocketStatusListener implements WorkflowStatusListener {

    private final WorkflowStatusWebSocketHandler webSocketHandler;

    public WorkflowWebSocketStatusListener(WorkflowStatusWebSocketHandler webSocketHandler) {
        this.webSocketHandler = webSocketHandler;
    }

    @Override
    public void onWorkflowCompleted(WorkflowModel workflow) {
        broadcast(workflow, "completed");
    }

    @Override
    public void onWorkflowTerminated(WorkflowModel workflow) {
        broadcast(workflow, "terminated");
    }

    @Override
    public void onWorkflowStarted(WorkflowModel workflow) {
        broadcast(workflow, "started");
    }

    @Override
    public void onWorkflowPaused(WorkflowModel workflow) {
        broadcast(workflow, "paused");
    }

    @Override
    public void onWorkflowResumed(WorkflowModel workflow) {
        broadcast(workflow, "resumed");
    }

    @Override
    public void onWorkflowFinalized(WorkflowModel workflow) {
        broadcast(workflow, "finalized");
    }

    @Override
    public void onWorkflowRestarted(WorkflowModel workflow) {
        broadcast(workflow, "restarted");
    }

    @Override
    public void onWorkflowRerun(WorkflowModel workflow) {
        broadcast(workflow, "rerun");
    }

    @Override
    public void onWorkflowRetried(WorkflowModel workflow) {
        broadcast(workflow, "retried");
    }

    private void broadcast(WorkflowModel workflow, String event) {
        webSocketHandler.broadcastWorkflowUpdate(
                workflow.getWorkflowId(),
                workflow.getWorkflowName(),
                workflow.getStatus().name(),
                event);
    }
}
