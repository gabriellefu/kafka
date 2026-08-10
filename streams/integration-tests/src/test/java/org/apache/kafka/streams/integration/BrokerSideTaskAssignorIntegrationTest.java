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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.StreamsGroupMemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.AllTasksToFirstMemberAssignor;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.purgeLocalStreamsState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that a "streams" group follows the assignment computed by a custom broker-side task assignor
 * (KIP-1357), including an assignment no client-side assignor would ever produce.
 */
@Timeout(600)
@Tag("integration")
public class BrokerSideTaskAssignorIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final int NUM_PARTITIONS = 4;
    private static final int NUM_KEYS = 100;

    public static final EmbeddedKafkaCluster CLUSTER;
    static {
        final Properties brokerProps = new Properties();
        // The only registered assignor, so it is also the cluster-wide default.
        brokerProps.put(GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNORS_CONFIG, AllTasksToFirstMemberAssignor.class.getName());
        brokerProps.put(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0");
        CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, brokerProps);
    }

    private static Admin admin;

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
        final Properties adminConfig = new Properties();
        adminConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        admin = Admin.create(adminConfig);
    }

    @AfterAll
    public static void closeCluster() {
        Utils.closeQuietly(admin, "admin");
        CLUSTER.stop();
    }

    private String appId;
    private String inputTopic;
    private String outputTopic;
    private final List<Properties> streamsConfigurations = new ArrayList<>();

    @BeforeEach
    public void createTopics(final TestInfo testInfo) throws InterruptedException {
        appId = safeUniqueTestName(testInfo);
        inputTopic = appId + "-input";
        outputTopic = appId + "-output";
        CLUSTER.createTopic(inputTopic, NUM_PARTITIONS, 1);
        CLUSTER.createTopic(outputTopic, NUM_PARTITIONS, 1);
    }

    @AfterEach
    public void cleanup() throws Exception {
        purgeLocalStreamsState(streamsConfigurations);
        streamsConfigurations.clear();
        CLUSTER.deleteAllTopics();
    }

    @Test
    public void shouldRunWithTheAssignmentComputedByTheBrokerSideAssignor() throws Exception {
        produceInput();
        final KafkaStreams streams1 = new KafkaStreams(topology(), props("-1"));
        final KafkaStreams streams2 = new KafkaStreams(topology(), props("-2"));
        try {
            startApplicationAndWaitUntilRunning(asList(streams1, streams2), Duration.ofSeconds(60));

            // An instance that ends up with no tasks reaches RUNNING before the group has settled, so wait for
            // the assignment to be handed out in full before looking at how it is spread.
            final AtomicReference<StreamsGroupDescription> description = new AtomicReference<>();
            TestUtils.waitForCondition(() -> {
                description.set(admin.describeStreamsGroups(List.of(appId)).all().get().get(appId));
                return totalActiveTaskCount(description.get()) == NUM_PARTITIONS;
            }, 60_000L, "The group was not assigned all " + NUM_PARTITIONS + " active tasks");

            assertThat(description.get().assignorName(), equalTo(Optional.of(AllTasksToFirstMemberAssignor.NAME)));

            // Every task went to a single member, which is what the broker-side assignor decided and what no
            // client-side assignor would have come up with.
            final List<Integer> activeTaskCounts = description.get().members().stream()
                .map(BrokerSideTaskAssignorIntegrationTest::activeTaskCount)
                .sorted()
                .collect(Collectors.toList());
            assertThat(activeTaskCounts, equalTo(List.of(0, NUM_PARTITIONS)));

            // The instance holding every task still processes all of the input.
            waitUntilMinKeyValueRecordsReceived(consumerConfig(), outputTopic, NUM_KEYS, 120_000L);
        } finally {
            streams1.close(Duration.ofSeconds(60));
            streams2.close(Duration.ofSeconds(60));
        }
    }

    private static int totalActiveTaskCount(final StreamsGroupDescription description) {
        return description.members().stream()
            .mapToInt(BrokerSideTaskAssignorIntegrationTest::activeTaskCount)
            .sum();
    }

    private static int activeTaskCount(final StreamsGroupMemberDescription member) {
        return member.assignment().activeTasks().stream()
            .mapToInt(taskIds -> taskIds.partitions().size())
            .sum();
    }

    private Properties props(final String stateDirSuffix) {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId + stateDirSuffix).getPath());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        config.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name());
        streamsConfigurations.add(config);
        return config;
    }

    private Topology topology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.Integer()))
            .to(outputTopic, Produced.with(Serdes.Integer(), Serdes.Integer()));
        return builder.build();
    }

    private void produceInput() {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        final List<KeyValue<Integer, Integer>> data = IntStream.range(0, NUM_KEYS)
            .mapToObj(i -> KeyValue.pair(i, i))
            .collect(Collectors.toList());
        IntegrationTestUtils.produceKeyValuesSynchronously(inputTopic, data, producerConfig, CLUSTER.time);
    }

    private Properties consumerConfig() {
        final Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, appId + "-verifier");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        return config;
    }
}
