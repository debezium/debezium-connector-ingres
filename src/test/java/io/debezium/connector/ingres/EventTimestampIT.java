/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.ingres.IngresConnectorConfig.SnapshotMode;
import io.debezium.connector.ingres.util.TestHelper;
import io.debezium.data.Envelope.FieldName;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * Verifies that the {@code ts_ms} field of streamed change events reflects the actual transaction
 * commit time recorded by the database, rather than the wall-clock time at which the connector
 * happened to process the change.
 *
 */
public class EventTimestampIT extends AbstractAsyncEngineConnectorTest {

    private IngresConnection connection;

    @BeforeEach
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute(
                "DROP TABLE IF EXISTS time_test_table",
                "CREATE TABLE time_test_table (id int not null, val varchar(30), primary key (id))");
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Print.enable();
    }

    @AfterEach
    public void after() throws SQLException {
        stopConnector();
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        assertConnectorNotRunning();
        if (connection != null) {
            connection.rollback()
                    .execute("DROP TABLE time_test_table")
                    .close();
        }
    }

    @Test
    public void eventTimestampShouldReflectCommitTimeNotProcessingTime() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(IngresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(IngresConnectorConfig.TABLE_INCLUDE_LIST, TestHelper.includePrefix("time_test_table"))
                .build();

        start(IngresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        // Insert while the connector is live and streaming. Unlike the offline-schema-change pattern
        // used elsewhere in this test suite, Ingres CDC publications only capture changes made while a
        // publication for the table is actively open - a commit made while the connector is fully
        // stopped is not replayed on restart, so this test (unlike those) keeps the connector running
        // throughout and never inserts into an offline gap.
        final Instant beforeCommit = Instant.now();
        connection.execute("INSERT INTO time_test_table VALUES(1, 'a')");
        final Instant afterCommit = Instant.now();

        waitForAvailableRecords();

        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> table = records.recordsForTopic(TestHelper.topicName("time_test_table"));
        assertThat(table).hasSize(1);
        assertNoRecordsToConsume();

        final Struct value = (Struct) table.get(0).value();
        final Struct source = value.getStruct(FieldName.SOURCE);
        final long tsMs = source.getInt64(FieldName.TIMESTAMP);
        final Instant eventTimestamp = Instant.ofEpochMilli(tsMs);

        final long toleranceMs = TimeUnit.SECONDS.toMillis(10);

        assertThat(eventTimestamp)
                .as("event timestamp should be close to the actual commit time reported by the database, "
                        + "correctly converted from the server's local clock via source.timezone")
                .isAfter(beforeCommit.minusMillis(toleranceMs))
                .isBefore(afterCommit.plusMillis(toleranceMs));
    }
}
