/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres.snapshot.lock;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.connector.ingres.IngresConnector;
import io.debezium.connector.ingres.IngresConnectorConfig;
import io.debezium.snapshot.spi.SnapshotLock;

@ConnectorSpecific(connector = IngresConnector.class)
public class ExclusiveSnapshotLock implements SnapshotLock {

    @Override
    public String name() {
        return IngresConnectorConfig.SnapshotLockingMode.EXCLUSIVE.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
        return Optional.empty();
        // FIXME implement exclusive locking
        // return Optional.of(String.format("SET LOCKMODE ON %s WHERE level=table, READLOCK = EXCLUSIVE", tableId));
    }
}
