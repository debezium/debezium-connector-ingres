/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import io.debezium.schema.SchemaFactory;

public class IngresSchemaFactory extends SchemaFactory {

    private static final IngresSchemaFactory SCHEMA_FACTORY = new IngresSchemaFactory();

    public IngresSchemaFactory() {
        super();
    }

    public static IngresSchemaFactory get() {
        return SCHEMA_FACTORY;
    }
}
