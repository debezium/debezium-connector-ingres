/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import io.debezium.config.Configuration;
import io.debezium.connector.ingres.IngresConnectorConfig.SnapshotMode;
import io.debezium.connector.ingres.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.Flaky;
import io.debezium.transforms.outbox.AbstractEventRouterTest;
import io.debezium.transforms.outbox.EventRouter;

/**
 * An integration test for Ingres and the {@link EventRouter} for outbox.
 *
 * @author Chris Cranford, Lars M Johansson
 */
@Flaky("DBZ-8114")
public class OutboxEventRouterIT extends AbstractEventRouterTest<IngresConnector> {

    private static final String SETUP_OUTBOX_TABLE = "CREATE TABLE outbox (" +
            "id varchar(64) not null primary key, " +
            "aggregatetype varchar(255) not null, " +
            "aggregateid varchar(255) not null, " +
            "type varchar(255) not null, " +
            "payload char(100))";

    private IngresConnection connection;

    @BeforeEach
    @Override
    protected void beforeEach() throws Exception {
        connection = TestHelper.testConnection();

        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        super.beforeEach();
    }

    @AfterEach
    protected void afterEach() throws Exception {
        stopConnector();
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        assertConnectorNotRunning();
        if (connection != null && connection.isConnected()) {
            connection.rollback();
            TestHelper.dropTable(connection, tableName());
            connection.close();
        }
    }

    @Override
    protected Class<IngresConnector> getConnectorClass() {
        return IngresConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder(boolean initialSnapshot) {
        final SnapshotMode snapshotMode = initialSnapshot ? SnapshotMode.INITIAL : SnapshotMode.NO_DATA;
        return TestHelper.defaultConfig()
                .with(IngresConnectorConfig.SNAPSHOT_MODE, snapshotMode.getValue())
                .with(IngresConnectorConfig.TABLE_INCLUDE_LIST, TestHelper.includePrefix(tableName()));
    }

    @Override
    protected String getSchemaNamePrefix() {
        return topicName();
    }

    @Override
    protected Schema getPayloadSchema() {
        return Schema.OPTIONAL_STRING_SCHEMA;
    }

    @Override
    protected String tableName() {
        return "outbox";
    }

    @Override
    protected String topicName() {
        return TestHelper.topicName(tableName());
    }

    @Override
    protected void createTable() throws Exception {
        TestHelper.dropTable(connection, tableName());
        connection.execute(SETUP_OUTBOX_TABLE);
    }

    @Override
    protected String createInsert(String eventId,
                                  String eventType,
                                  String aggregateType,
                                  String aggregateId,
                                  String payloadJson,
                                  String additional) {
        StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO outbox VALUES (");
        insert.append("'").append(eventId).append("', ");
        insert.append("'").append(aggregateType).append("', ");
        insert.append("'").append(aggregateId).append("', ");
        insert.append("'").append(eventType).append("', ");
        if (payloadJson != null) {
            insert.append("'").append(payloadJson).append("'");
        }
        else {
            insert.append("NULL");
        }
        if (additional != null) {
            insert.append(additional);
        }
        insert.append(")");
        return insert.toString();
    }

    @Override
    protected void waitForSnapshotCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
    }

    @Override
    protected void alterTableWithExtra4Fields() throws Exception {
        connection.execute("ALTER TABLE outbox add version integer default 1 not null");
        connection.execute("ALTER TABLE outbox add somebooltype boolean not null default false");
        connection.execute("ALTER TABLE outbox add createdat timestamp");
        connection.execute("ALTER TABLE outbox add is_deleted boolean default false");
    }

    @Override
    protected void alterTableWithTimestampField() throws Exception {
        connection.execute("ALTER TABLE outbox add createdat ansidate");
    }

    @Override
    protected void alterTableModifyPayload() throws Exception {
        connection.execute("ALTER TABLE outbox ALTER payload char(200)");
    }

    @Override
    protected String getAdditionalFieldValues(boolean deleted) {
        return ", 1, true, '2019-03-24', " + (deleted ? "true" : "false");
    }

    @Override
    protected String getAdditionalFieldValuesTimestampOnly() {
        return ", '2019-03-24'";
    }

}
