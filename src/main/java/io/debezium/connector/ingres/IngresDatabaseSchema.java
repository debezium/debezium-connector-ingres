/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Logical representation of Ingres schema.
 *
 * @author Jiri Pechanec, Laoflch Luo, Lars M Johansson
 *
 */
public class IngresDatabaseSchema extends HistorizedRelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngresDatabaseSchema.class);

    public IngresDatabaseSchema(IngresConnectorConfig connectorConfig, TopicNamingStrategy<TableId> topicNamingStrategy,
                                IngresValueConverters valueConverters, SchemaNameAdjuster schemaNameAdjuster,
                                IngresConnection connection, CustomConverterRegistry customConverterRegistry, IngresTaskContext taskContext) {
        super(
                connectorConfig,
                topicNamingStrategy,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverters,
                        new IngresDefaultValueConverter(valueConverters, connection),
                        schemaNameAdjuster,
                        customConverterRegistry,
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getFieldNamer(),
                        connectorConfig.multiPartitionMode(),
                        connectorConfig.getEventConvertingFailureHandlingMode()),
                true,
                connectorConfig.getKeyMapper(), taskContext);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        LOGGER.debug("Applying schema change event {}", schemaChange);

        // just a single table per DDL event for Ingres
        Table table = schemaChange.getTables().iterator().next();
        buildAndRegisterSchema(table);
        tables().overwriteTable(table);

        TableChanges tableChanges = null;
        if (schemaChange.getType() == SchemaChangeEventType.CREATE) {
            tableChanges = new TableChanges();
            tableChanges.create(table);
        }
        else if (schemaChange.getType() == SchemaChangeEventType.ALTER) {
            tableChanges = new TableChanges();
            tableChanges.alter(table);
        }

        record(schemaChange, tableChanges);
    }

    @Override
    protected DdlParser getDdlParser() {
        return null;
    }

    @Override
    public Tables tables() {
        return super.tables();
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return super.getTableFilter();
    }
}
