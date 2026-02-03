/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.ingres.util.TestHelper;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.util.Testing;

/**
 * Integration test to verify different Oracle datatypes as captured during initial snapshotting.
 *
 * @author Jiri Pechanec, Lars M Johansson
 */
public class SnapshotDatatypesIT extends AbstractIngresDatatypesTest {

    private String testMethodName;

    @BeforeAll
    public static void beforeClass() throws SQLException {
        AbstractIngresDatatypesTest.beforeClass();
        createTables();

        insertStringTypes();
        insertFpTypes();
        insertIntTypes();
        insertTimeTypes();
        insertClobTypes();
    }

    @BeforeEach
    public void before(TestInfo testInfo) throws Exception {
        testMethodName = testInfo.getTestMethod().get().getName();
        init(TemporalPrecisionMode.ADAPTIVE);
    }

    @Override
    protected void init(TemporalPrecisionMode temporalPrecisionMode) throws Exception {
        initializeConnectorTestFramework();
        Testing.Debug.enable();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        Configuration config = connectorConfig()
                .with(IngresConnectorConfig.TIME_PRECISION_MODE, temporalPrecisionMode)
                .build();

        start(IngresConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
    }

    protected Builder connectorConfig() {
        return TestHelper.defaultConfig()
                .with(IngresConnectorConfig.TABLE_INCLUDE_LIST, getTableIncludeList());
    }

    private String getTableIncludeList() {
        switch (testMethodName) {
            case "stringTypes":
                return TestHelper.includePrefix("type_string");
            case "fpTypes":
            case "fpTypesAsString":
            case "fpTypesAsDouble":
                return TestHelper.includePrefix("type_fp");
            case "intTypes":
                return TestHelper.includePrefix("type_int");
            case "timeTypes":
            case "timeTypesAsAdaptiveMicroseconds":
            case "timeTypesAsConnect":
                return TestHelper.includePrefix("type_time");
            case "clobTypes":
                return TestHelper.includePrefix("type_clob");
            default:
                throw new IllegalArgumentException("Unexpected test method: " + testMethodName);
        }
    }

    @Override
    protected boolean insertRecordsDuringTest() {
        return false;
    }
}
