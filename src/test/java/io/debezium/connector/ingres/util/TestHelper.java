/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres.util;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.ingres.IngresConnection;
import io.debezium.connector.ingres.IngresConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.util.Testing;

public class TestHelper {

    // TODO: change these values as per your test database
    public static final String TEST_DATABASE = "dbz";
    public static final String TEST_CONNECTOR = "ingres_server";
    public static final String TEST_SCHEMA = "u1";
    public static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history.txt").toAbsolutePath();

    /**
     * Key for schema parameter used to store a source column's type name.
     */
    public static final String TYPE_NAME_PARAMETER_KEY = "__debezium.source.column.type";

    /**
     * Key for schema parameter used to store a source column's type length.
     */
    public static final String TYPE_LENGTH_PARAMETER_KEY = "__debezium.source.column.length";

    /**
     * Key for schema parameter used to store a source column's type scale.
     */
    public static final String TYPE_SCALE_PARAMETER_KEY = "__debezium.source.column.scale";

    public static void setup() {

        // TODO: change these values as per your test database
        setDefaultProperty("database.dbname", TEST_DATABASE);
        setDefaultProperty("database.user", "u1");
        setDefaultProperty("database.password", "password");
        setDefaultProperty("database.hostname", "hostname.com");
        setDefaultProperty("database.port", "12345");

        // Bound how long any statement (including teardown's DROP TABLE) can run before being
        // cancelled, so a stuck lock/connection fails fast instead of hanging on the ~10 minute
        // framework default.
        setDefaultProperty("database.query.timeout.ms", "90000");

        // Set this to give enough delay to debug Debezium events
        setDefaultProperty("debezium.test.records.waittime", "30");
        setDefaultProperty("debezium.test.engine.waittime", "30");
    }

    /**
     * Sets a system property only if it hasn't already been set (e.g. via {@code -D} on the Maven
     * command line), so callers can override any of these without editing this file.
     */
    private static void setDefaultProperty(String key, String defaultValue) {
        if (System.getProperty(key) == null) {
            System.setProperty(key, defaultValue);
        }
    }

    public static String getDBName() {
        return TEST_DATABASE;
    }

    public static String getDBPrefix() {
        return TEST_DATABASE + "." + TEST_SCHEMA + ".";
    }

    public static String getSchemaPrefix() {
        return TEST_SCHEMA + ".";
    }

    public static String topicName(String tableName) {
        return getDBPrefix() + tableName;
    }

    /**
     * How to define the prefix database/schema/table name for including tables for the connector
     * @param tableName
     * @return String full table name with database and schema
     */
    public static String includePrefix(String tableName) {
        return getDBPrefix() + tableName;
    }

    public static JdbcConfiguration adminJdbcConfig() {
        setup();
        return JdbcConfiguration.copy(Configuration.fromSystemProperties(IngresConnectorConfig.DATABASE_CONFIG_PREFIX))
                .build();
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        return adminJdbcConfig();
    }

    /**
     * Returns a default configuration suitable for most test cases. Can be amended/overridden in individual tests as
     * needed.
     */
    public static Configuration.Builder defaultConfig() {
        return Configuration.copy(defaultJdbcConfig().map(key -> IngresConnectorConfig.DATABASE_CONFIG_PREFIX + key))
                .withDefault(CommonConnectorConfig.TOPIC_PREFIX, TEST_DATABASE)
                .withDefault(RelationalDatabaseConnectorConfig.SNAPSHOT_LOCK_TIMEOUT_MS, TimeUnit.SECONDS.toMillis(30))
                .withDefault(IngresConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .withDefault(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .withDefault(IngresConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .withDefault(IngresConnectorConfig.CDC_TIMEOUT, 3)
                .withDefault(CommonConnectorConfig.EXECUTOR_SHUTDOWN_TIMEOUT_MS, 30_000);
    }

    public static IngresConnection adminConnection() {
        return new IngresConnection(TestHelper.adminJdbcConfig());
    }

    public static void dropTable(IngresConnection connection, String table) throws SQLException {
        connection.execute("drop table if exists " + table);
    }

    public static void dropTables(IngresConnection connection, String... tables) throws SQLException {
        for (String table : tables) {
            dropTable(connection, table);
        }
    }

    private static class LazyConnectionHolder {
        static final IngresConnection INSTANCE = new IngresConnection(TestHelper.defaultJdbcConfig());
    }

    public static IngresConnection testConnection() {
        setup();
        return LazyConnectionHolder.INSTANCE;
    }
}
