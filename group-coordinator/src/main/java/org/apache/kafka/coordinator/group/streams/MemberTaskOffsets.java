/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;

import java.util.List;

/**
 * The last task offsets and task end-offsets reported by a member through its heartbeat.
 * <p>
 * This is transient, in-memory soft state that is not backed by any record in the
 * {@code __consumer_offsets} topic. It is refreshed every {@code taskOffsetIntervalMs} by the
 * member and is therefore not persisted across coordinator failover; the member re-reports it on
 * its next heartbeat. It is surfaced in the Streams group describe response.
 * <p>
 * The offsets are stored in the describe-response shape so that they can be returned without
 * further conversion.
 *
 * @param taskOffsets    Cumulative changelog offsets for the member's tasks.
 * @param taskEndOffsets Cumulative changelog end offsets for the member's tasks.
 */
public record MemberTaskOffsets(
    List<StreamsGroupDescribeResponseData.TaskOffset> taskOffsets,
    List<StreamsGroupDescribeResponseData.TaskOffset> taskEndOffsets
) {

    public MemberTaskOffsets {
        taskOffsets = taskOffsets == null ? List.of() : List.copyOf(taskOffsets);
        taskEndOffsets = taskEndOffsets == null ? List.of() : List.copyOf(taskEndOffsets);
    }

    /**
     * Converts the task offsets reported in a heartbeat request into the describe-response shape.
     *
     * @param requestOffsets The task offsets from the heartbeat request, possibly {@code null}.
     * @return The offsets mapped to the describe-response shape, or an empty list if none.
     */
    public static List<StreamsGroupDescribeResponseData.TaskOffset> fromRequest(
        List<StreamsGroupHeartbeatRequestData.TaskOffset> requestOffsets
    ) {
        if (requestOffsets == null) {
            return List.of();
        }
        return requestOffsets.stream()
            .map(offset -> new StreamsGroupDescribeResponseData.TaskOffset()
                .setSubtopologyId(offset.subtopologyId())
                .setPartition(offset.partition())
                .setOffset(offset.offset()))
            .toList();
    }
}
