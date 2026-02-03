/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.ingres.IngresConnectorConfig.SnapshotMode;
import io.debezium.connector.ingres.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.Flaky;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotTest;

@Flaky("DBZ-8114")
public class IncrementalSnapshotIT extends AbstractIncrementalSnapshotTest<IngresConnector> {

    private IngresConnection connection;

    @BeforeEach
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        TestHelper.dropTables(connection, "a", "b", "c", "debezium_signal");
        connection.execute(
                "CREATE TABLE a (pk int not null, aa int, primary key (pk))",
                "CREATE TABLE b (pk int not null, aa int, primary key (pk))",
                "CREATE TABLE c (pk1 int, pk2 int, pk3 int, pk4 int, aa int)",
                "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(255))");
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Print.disable();
    }

    @AfterEach
    public void after() throws SQLException {
        /*
         * Since all DDL operations are forbidden during Ingres CDC,
         * we have to ensure the connector is properly shut down before dropping tables.
         */
        stopConnector();
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        assertConnectorNotRunning();
        if (connection != null) {
            connection.rollback();
            TestHelper.dropTables(connection, "a", "b", "c", "debezium_signal");
            connection.close();
        }
    }

    @Override
    protected void waitForConnectorToStart() {
        super.waitForConnectorToStart();
        try {
            waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        }
        catch (InterruptedException e) {
            throw new DebeziumException(e);
        }
    }

    @Override
    protected Class<IngresConnector> connectorClass() {
        return IngresConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected String topicName() {
        return TestHelper.topicName("a");
    }

    @Override
    protected List<String> topicNames() {
        return List.of(topicName(), TestHelper.topicName("b"));
    }

    @Override
    protected String noPKTopicName() {
        return TestHelper.topicName("c");
    }

    @Override
    protected String tableName() {
        return "a";
    }

    @Override
    protected List<String> tableNames() {
        return List.of(tableName(), "b");
    }

    @Override
    protected String noPKTableName() {
        return "c";
    }

    @Override
    protected String tableDataCollectionId() {
        return TestHelper.TEST_DATABASE + '.' + tableName();
    }

    @Override
    protected List<String> tableDataCollectionIds() {
        return tableNames().stream().map(name -> TestHelper.TEST_DATABASE + '.' + name).collect(Collectors.toList());
    }

    @Override
    protected String noPKTableDataCollectionId() {
        return TestHelper.TEST_DATABASE + "." + noPKTableName();
    }

    @Override
    protected String signalTableName() {
        return "debezium_signal";
    }

    @Override
    protected String signalTableNameSanitized() {
        return TestHelper.TEST_DATABASE + '.' + signalTableName();
    }

    protected String tableIncludeList() {
        return String.join(",", tableDataCollectionIds());
    }

    @Override
    protected Builder config() {
        return TestHelper.defaultConfig()
                .with(IngresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(IngresConnectorConfig.SIGNAL_DATA_COLLECTION, this::signalTableNameSanitized)
                .with(IngresConnectorConfig.MSG_KEY_COLUMNS, noPKTableDataCollectionId() + ":pk1,pk2,pk3,pk4")
                .with(IngresConnectorConfig.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(IngresConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(IngresConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 200);
    }

    @Override
    protected Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        Builder config = config()
                .with(IngresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(IngresConnectorConfig.STORE_ONLY_CAPTURED_TABLES_DDL, storeOnlyCapturedDdl);
        return signalTableOnly
                ? config.with(IngresConnectorConfig.TABLE_EXCLUDE_LIST, this::tableDataCollectionId)
                : config.with(IngresConnectorConfig.TABLE_INCLUDE_LIST, this::tableIncludeList);
    }

    @Override
    protected int defaultIncrementalSnapshotChunkSize() {
        return 3;
    }

    @Override
    protected String connector() {
        return TestHelper.TEST_CONNECTOR;
    }

    @Override
    protected String server() {
        return TestHelper.TEST_DATABASE;
    }

    @Test
    @Disabled("Ingres does not support DDL operations on tables defined for replication")
    @Override
    public void snapshotPreceededBySchemaChange() throws Exception {
        super.snapshotPreceededBySchemaChange();
    }
}
