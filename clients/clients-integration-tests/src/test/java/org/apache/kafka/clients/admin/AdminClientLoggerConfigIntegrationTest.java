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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.LogLevelConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.logger.LoggingController;

import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterEach;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(types = {Type.KRAFT})
public class AdminClientLoggerConfigIntegrationTest {

    private final ClusterInstance clusterInstance;

    AdminClientLoggerConfigIntegrationTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @AfterEach
    public void tearDown() {
        // log levels are JVM-wide, so restore the log4j2.yaml levels for the next test
        Configurator.reconfigure();
    }

    @ClusterTest
    public void testDescribeConfigsForLog4jLogLevels() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            LoggerFactory.getLogger("kafka.cluster.Replica").trace("Message to create the logger");
            Config loggerConfig = describeBrokerLoggers(admin);
            String kafkaLogLevel = loggerConfig.get("kafka").value();
            ConfigEntry clusterReplicaLogLevel = loggerConfig.get("kafka.cluster.Replica");
            // we expect the log level to be inherited from the first ancestor with a level configured
            assertEquals(kafkaLogLevel, clusterReplicaLogLevel.value());
            assertEquals("kafka.cluster.Replica", clusterReplicaLogLevel.name());
            assertEquals(ConfigEntry.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG, clusterReplicaLogLevel.source());
            assertFalse(clusterReplicaLogLevel.isReadOnly());
            assertFalse(clusterReplicaLogLevel.isSensitive());
            assertTrue(clusterReplicaLogLevel.synonyms().isEmpty());
        }
    }

    @ClusterTest
    public void testIncrementalAlterConfigsForLog4jLogLevels() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            String ancestorLogger = "kafka";
            Config initialLoggerConfig = describeBrokerLoggers(admin);
            String initialAncestorLogLevel = initialLoggerConfig.get("kafka").value();
            String initialControllerServerLogLevel = initialLoggerConfig.get("kafka.server.ControllerServer").value();
            String initialLogCleanerLogLevel = initialLoggerConfig.get("kafka.log.LogCleaner").value();
            String initialReplicaManagerLogLevel = initialLoggerConfig.get("kafka.server.ReplicaManager").value();

            String newAncestorLogLevel = LogLevelConfig.DEBUG_LOG_LEVEL;
            List<AlterConfigOp> alterAncestorLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry(ancestorLogger, newAncestorLogLevel), AlterConfigOp.OpType.SET));
            // Test validateOnly does not change anything
            alterBrokerLoggers(admin, alterAncestorLoggerEntry, true);
            Config validatedLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(initialAncestorLogLevel, validatedLoggerConfig.get(ancestorLogger).value());
            assertEquals(initialControllerServerLogLevel, validatedLoggerConfig.get("kafka.server.ControllerServer").value());
            assertEquals(initialLogCleanerLogLevel, validatedLoggerConfig.get("kafka.log.LogCleaner").value());
            assertEquals(initialReplicaManagerLogLevel, validatedLoggerConfig.get("kafka.server.ReplicaManager").value());

            // test that we can change them and unset loggers still use the root's log level
            alterBrokerLoggers(admin, alterAncestorLoggerEntry);
            Config changedAncestorLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(newAncestorLogLevel, changedAncestorLoggerConfig.get(ancestorLogger).value());
            assertEquals(newAncestorLogLevel, changedAncestorLoggerConfig.get("kafka.server.ControllerServer").value());
            assertEquals(newAncestorLogLevel, changedAncestorLoggerConfig.get("kafka.log.LogCleaner").value());
            assertEquals(newAncestorLogLevel, changedAncestorLoggerConfig.get("kafka.server.ReplicaManager").value());

            // alter the LogCleaner's logger so we can later test resetting it
            List<AlterConfigOp> alterLogCleanerLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry("kafka.log.LogCleaner", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SET));
            alterBrokerLoggers(admin, alterLogCleanerLoggerEntry);
            Config changedBrokerLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(LogLevelConfig.ERROR_LOG_LEVEL, changedBrokerLoggerConfig.get("kafka.log.LogCleaner").value());

            // properly test various set operations and one delete
            List<AlterConfigOp> alterLogLevelsEntries = List.of(
                new AlterConfigOp(new ConfigEntry("kafka.server.ControllerServer", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry("kafka.log.LogCleaner", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry("kafka.server.ReplicaManager", LogLevelConfig.TRACE_LOG_LEVEL), AlterConfigOp.OpType.SET));
            alterBrokerLoggers(admin, alterLogLevelsEntries);
            Config alteredLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(newAncestorLogLevel, alteredLoggerConfig.get(ancestorLogger).value());
            assertEquals(LogLevelConfig.INFO_LOG_LEVEL, alteredLoggerConfig.get("kafka.server.ControllerServer").value());
            assertEquals(LogLevelConfig.ERROR_LOG_LEVEL, alteredLoggerConfig.get("kafka.log.LogCleaner").value());
            assertEquals(LogLevelConfig.TRACE_LOG_LEVEL, alteredLoggerConfig.get("kafka.server.ReplicaManager").value());
        }
    }

    /**
     * 1. Assume kafka logger == TRACE
     * 2. Change kafka.server.ControllerServer logger to INFO
     * 3. Unset kafka.server.ControllerServer via AlterConfigOp.OpType.DELETE (resets it to the kafka logger - TRACE)
     * 4. Change kafka logger to ERROR
     * 5. Ensure the kafka.server.ControllerServer logger's level is ERROR (the current kafka logger level)
     */
    @ClusterTest
    public void testIncrementalAlterConfigsForLog4jLogLevelsCanResetLoggerToCurrentRoot() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            String ancestorLogger = "kafka";
            // step 1 - configure kafka logger
            String initialAncestorLogLevel = LogLevelConfig.TRACE_LOG_LEVEL;
            List<AlterConfigOp> alterAncestorLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry(ancestorLogger, initialAncestorLogLevel), AlterConfigOp.OpType.SET));
            alterBrokerLoggers(admin, alterAncestorLoggerEntry);
            Config initialLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(initialAncestorLogLevel, initialLoggerConfig.get(ancestorLogger).value());
            assertEquals(initialAncestorLogLevel, initialLoggerConfig.get("kafka.server.ControllerServer").value());

            // step 2 - change ControllerServer logger to INFO
            List<AlterConfigOp> alterControllerLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry("kafka.server.ControllerServer", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET));
            alterBrokerLoggers(admin, alterControllerLoggerEntry);
            Config changedControllerLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(initialAncestorLogLevel, changedControllerLoggerConfig.get(ancestorLogger).value());
            assertEquals(LogLevelConfig.INFO_LOG_LEVEL, changedControllerLoggerConfig.get("kafka.server.ControllerServer").value());

            // step 3 - unset ControllerServer logger
            List<AlterConfigOp> deleteControllerLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry("kafka.server.ControllerServer", ""), AlterConfigOp.OpType.DELETE));
            alterBrokerLoggers(admin, deleteControllerLoggerEntry);
            Config deletedControllerLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(initialAncestorLogLevel, deletedControllerLoggerConfig.get(ancestorLogger).value());
            assertEquals(initialAncestorLogLevel, deletedControllerLoggerConfig.get("kafka.server.ControllerServer").value());

            String newAncestorLogLevel = LogLevelConfig.ERROR_LOG_LEVEL;
            List<AlterConfigOp> newAlterAncestorLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry(ancestorLogger, newAncestorLogLevel), AlterConfigOp.OpType.SET));
            alterBrokerLoggers(admin, newAlterAncestorLoggerEntry);
            Config newAncestorLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(newAncestorLogLevel, newAncestorLoggerConfig.get(ancestorLogger).value());
            assertEquals(newAncestorLogLevel, newAncestorLoggerConfig.get("kafka.server.ControllerServer").value());
        }
    }

    @ClusterTest
    public void testIncrementalAlterConfigsForLog4jLogLevelsCanSetToRootLogger() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            Config initialLoggerConfig = describeBrokerLoggers(admin);
            String initialRootLogLevel = initialLoggerConfig.get(LoggingController.ROOT_LOGGER).value();
            String newRootLogLevel = LogLevelConfig.DEBUG_LOG_LEVEL;

            List<AlterConfigOp> alterRootLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry(LoggingController.ROOT_LOGGER, newRootLogLevel), AlterConfigOp.OpType.SET));

            alterBrokerLoggers(admin, alterRootLoggerEntry, true);
            Config validatedRootLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(initialRootLogLevel, validatedRootLoggerConfig.get(LoggingController.ROOT_LOGGER).value());

            alterBrokerLoggers(admin, alterRootLoggerEntry);
            Config changedRootLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(newRootLogLevel, changedRootLoggerConfig.get(LoggingController.ROOT_LOGGER).value());
        }
    }

    @ClusterTest
    public void testIncrementalAlterConfigsForLog4jLogLevelsCannotResetRootLogger() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            List<AlterConfigOp> deleteRootLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry(LoggingController.ROOT_LOGGER, ""), AlterConfigOp.OpType.DELETE));

            ExecutionException e = assertThrows(ExecutionException.class, () -> alterBrokerLoggers(admin, deleteRootLoggerEntry));
            assertInstanceOf(InvalidRequestException.class, e.getCause());
        }
    }

    @ClusterTest
    public void testIncrementalAlterConfigsForLog4jLogLevelsDoesNotWorkWithInvalidConfigs() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            String validLoggerName = "kafka.server.KafkaRequestHandler";
            ConfigEntry expectedValidLoggerLogLevel = describeBrokerLoggers(admin).get(validLoggerName);

            List<AlterConfigOp> appendLogLevelEntries = List.of(
                new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
                new AlterConfigOp(new ConfigEntry("kafka.network.SocketServer", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.APPEND)); // append is not supported
            assertInstanceOf(InvalidRequestException.class,
                assertThrows(ExecutionException.class, () -> alterBrokerLoggers(admin, appendLogLevelEntries)).getCause());
            assertEquals(expectedValidLoggerLogLevel, describeBrokerLoggers(admin).get(validLoggerName));

            List<AlterConfigOp> subtractLogLevelEntries = List.of(
                new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
                new AlterConfigOp(new ConfigEntry("kafka.network.SocketServer", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SUBTRACT)); // subtract is not supported
            assertInstanceOf(InvalidRequestException.class,
                assertThrows(ExecutionException.class, () -> alterBrokerLoggers(admin, subtractLogLevelEntries)).getCause());
            assertEquals(expectedValidLoggerLogLevel, describeBrokerLoggers(admin).get(validLoggerName));

            List<AlterConfigOp> invalidLogLevelLogLevelEntries = List.of(
                new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
                new AlterConfigOp(new ConfigEntry("kafka.network.SocketServer", "OFF"), AlterConfigOp.OpType.SET)); // OFF is not a valid log level
            assertInstanceOf(InvalidConfigurationException.class,
                assertThrows(ExecutionException.class, () -> alterBrokerLoggers(admin, invalidLogLevelLogLevelEntries)).getCause());
            assertEquals(expectedValidLoggerLogLevel, describeBrokerLoggers(admin).get(validLoggerName));

            List<AlterConfigOp> invalidLoggerNameLogLevelEntries = List.of(
                new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
                new AlterConfigOp(new ConfigEntry("Some Other LogCleaner", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SET)); // invalid logger name is not supported
            assertInstanceOf(InvalidConfigurationException.class,
                assertThrows(ExecutionException.class, () -> alterBrokerLoggers(admin, invalidLoggerNameLogLevelEntries)).getCause());
            assertEquals(expectedValidLoggerLogLevel, describeBrokerLoggers(admin).get(validLoggerName));
        }
    }

    private ConfigResource brokerLoggerConfigResource() {
        return new ConfigResource(ConfigResource.Type.BROKER_LOGGER, String.valueOf(clusterInstance.brokerIds().iterator().next()));
    }

    private void alterBrokerLoggers(Admin admin, Collection<AlterConfigOp> entries) throws ExecutionException, InterruptedException {
        alterBrokerLoggers(admin, entries, false);
    }

    private void alterBrokerLoggers(Admin admin, Collection<AlterConfigOp> entries, boolean validateOnly) throws ExecutionException, InterruptedException {
        ConfigResource resource = brokerLoggerConfigResource();
        admin.incrementalAlterConfigs(Map.of(resource, entries), new AlterConfigsOptions().validateOnly(validateOnly))
            .values().get(resource).get();
    }

    private Config describeBrokerLoggers(Admin admin) throws ExecutionException, InterruptedException {
        ConfigResource resource = brokerLoggerConfigResource();
        return admin.describeConfigs(List.of(resource)).values().get(resource).get();
    }
}
