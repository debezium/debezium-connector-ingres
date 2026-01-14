/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

public class IngresPartition extends AbstractPartition implements Partition {

    private static final String PARTITION_KEY = "databaseName";

    public IngresPartition(String databaseName) {
        super(databaseName);
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(PARTITION_KEY, databaseName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final IngresPartition other = (IngresPartition) obj;
        return Objects.equals(databaseName, other.databaseName);
    }

    @Override
    public int hashCode() {
        return databaseName.hashCode();
    }

    @Override
    public String toString() {
        return "IngresPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    static class Provider implements Partition.Provider<IngresPartition> {
        private final IngresConnectorConfig connectorConfig;

        Provider(IngresConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<IngresPartition> getPartitions() {
            return Collections.singleton(new IngresPartition(connectorConfig.getLogicalName()));
        }
    }
}
