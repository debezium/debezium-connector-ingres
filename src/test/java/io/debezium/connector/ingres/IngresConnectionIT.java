/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import org.junit.Test;

import io.debezium.connector.ingres.util.TestHelper;
import io.debezium.util.Testing;

/**
 * Integration test for {@link IngresConnection}
 */
public class IngresConnectionIT implements Testing {

    @Test
    public void testBasicSqlConnection() throws Exception {
        try (IngresConnection connection = TestHelper.adminConnection()) {
            connection.connect();
        }
    }
}
