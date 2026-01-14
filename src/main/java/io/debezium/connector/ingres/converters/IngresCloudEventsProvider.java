/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres.converters;

import io.debezium.connector.ingres.Module;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.SerializerType;

/**
 * An implementation of {@link CloudEventsProvider} for Ingres.
 *
 * @author Chris Cranford, Lars M Johansson
 *
 */
public class IngresCloudEventsProvider implements CloudEventsProvider {
    @Override
    public String getName() {
        return Module.name();
    }

    @Override
    public CloudEventsMaker createMaker(RecordAndMetadata recordAndMetadata, SerializerType contentType, String dataSchemaUriBase, String cloudEventsSchemaName) {
        return new IngresCloudEventsMaker(recordAndMetadata, contentType, dataSchemaUriBase, cloudEventsSchemaName);
    }
}
