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
package org.apache.kafka.streams.integration.utils;

import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.api.streams.assignor.TopologyDescriber;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A broker-side {@link TaskAssignor} that puts every active task on the lexicographically first member and
 * leaves the other members empty. The result is deliberately unbalanced, so a client running under it
 * demonstrably follows the assignment the broker computed rather than one it could have computed itself.
 */
public class AllTasksToFirstMemberAssignor implements TaskAssignor {

    public static final String NAME = "all-to-first";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public GroupAssignment assign(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber) {
        final List<String> memberIds = groupSpec.memberIds().stream().sorted().collect(Collectors.toList());
        if (memberIds.isEmpty()) {
            return new GroupAssignment(Map.of());
        }

        final Map<String, Set<Integer>> allActiveTasks = new HashMap<>();
        for (final String subtopologyId : topologyDescriber.subtopologies()) {
            final Set<Integer> partitions = new HashSet<>();
            for (int partition = 0; partition < topologyDescriber.maxNumInputPartitions(subtopologyId); partition++) {
                partitions.add(partition);
            }
            allActiveTasks.put(subtopologyId, partitions);
        }

        final Map<String, MemberAssignment> assignments = new HashMap<>();
        assignments.put(memberIds.get(0), new MemberAssignment(allActiveTasks, new HashMap<>()));
        for (final String memberId : memberIds.subList(1, memberIds.size())) {
            assignments.put(memberId, new MemberAssignment(new HashMap<>(), new HashMap<>()));
        }
        return new GroupAssignment(assignments);
    }
}
