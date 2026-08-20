/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.ingres.IngresConnectorConfig.SnapshotMode;
import io.debezium.connector.ingres.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Reproduces the mixed-owner defect described in: {@code
 * IngresStreamingChangeEventSource.execute()} refreshes the shared {@code Tables} registry once
 * per distinct table owner found in {@code table.include.list}, but passes
 * {@code removeTablesNotFoundInJdbc=true} on every one of those calls.
 */
public class MixedOwnerStreamingIT extends AbstractAsyncEngineConnectorTest {

    private static final String TABLE1 = "categories";
    private static final String TABLE2 = "cm_lob";

    private static IngresConnection ownerOneConnection;
    private static IngresConnection ownerTwoConnection;

    @BeforeAll
    static void beforeAll() throws SQLException {
        ownerOneConnection = TestHelper.testConnection();
        ownerTwoConnection = TestHelper.secondaryUserConnection();
    }

    @AfterAll
    static void afterAll() throws SQLException {
        if (ownerOneConnection != null) {
            ownerOneConnection.close();
        }
        if (ownerTwoConnection != null) {
            ownerTwoConnection.close();
        }
    }

    @BeforeEach
    void before() throws SQLException {
        ownerOneConnection.execute("DROP TABLE IF EXISTS " + TABLE1);
        ownerTwoConnection.execute("DROP TABLE IF EXISTS " + TABLE2);

        ownerOneConnection.execute(
                "CREATE TABLE " + TABLE1 + " (id int not null primary key, name varchar(50))");
        ownerTwoConnection.execute(
                "CREATE TABLE " + TABLE2 + " (id int not null primary key, name varchar(50))");

        ownerTwoConnection.execute("GRANT SELECT ON " + TABLE2 + " TO " + TestHelper.TEST_SCHEMA);

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void after() throws SQLException {
        // DDL is forbidden while Ingres CDC is streaming, so the connector must be fully stopped
        // before tables can be dropped.
        stopConnector();
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        assertConnectorNotRunning();

        ownerOneConnection.rollback().execute("DROP TABLE IF EXISTS " + TABLE1);
        ownerTwoConnection.rollback().execute("DROP TABLE IF EXISTS " + TABLE2);
    }

    @Test
    void streamsChangesForEveryOwnerInMixedOwnerIncludeList() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(IngresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(IngresConnectorConfig.TABLE_INCLUDE_LIST,
                        TestHelper.includePrefix(TestHelper.TEST_SCHEMA, TABLE1) + ","
                                + TestHelper.includePrefix(TestHelper.TEST_SCHEMA2, TABLE2))
                .build();

        start(IngresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        ownerOneConnection.execute("INSERT INTO " + TABLE1 + " VALUES (1, 'owner-one')");
        ownerTwoConnection.execute("INSERT INTO " + TABLE2 + " VALUES (1, 'owner-two')");
        waitForAvailableRecords();

        SourceRecords records = consumeRecordsByTopic(2);

        List<SourceRecord> owner1Records = records.recordsForTopic(TestHelper.topicName(TABLE1));
        List<SourceRecord> owner2Records = records.recordsForTopic(TestHelper.topicName(TestHelper.TEST_SCHEMA2, TABLE2));

        assertThat(owner1Records)
                .as("insert into '%s' (owner '%s') should have streamed", TABLE1, TestHelper.TEST_SCHEMA)
                .hasSize(1);
        assertThat(owner2Records)
                .as("insert into '%s' (owner '%s') should have streamed", TABLE2, TestHelper.TEST_SCHEMA2)
                .hasSize(1);

        VerifyRecord.isValidInsert(owner1Records.get(0), "id", 1);
        VerifyRecord.isValidInsert(owner2Records.get(0), "id", 1);
    }
}
