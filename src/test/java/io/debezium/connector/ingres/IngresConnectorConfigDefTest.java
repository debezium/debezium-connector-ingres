/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import io.debezium.config.ConfigDefinitionMetadataTest;

public class IngresConnectorConfigDefTest extends ConfigDefinitionMetadataTest {

    public IngresConnectorConfigDefTest() {
        super(new IngresConnector());
    }
}
