/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import java.time.Duration;
import java.util.Objects;

import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.StaleOffsetsResult;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset change position, the combination of the commit, change, and begin records along
 * with the transaction id, is compared against the value captured when the monitor was last
 * consulted, and when the position has not moved, a stale result is reported. The combination
 * is used rather than the commit record alone so that progress within a single large
 * transaction is not reported as stale.
 *
 * @author Chris Cranford
 */
public class IngresOffsetActivityMonitor implements OffsetActivityMonitor<IngresPartition, IngresOffsetContext> {

    private final Duration checkInterval;

    private TxLogPosition previousPosition;

    public IngresOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public StaleOffsetsResult checkForStaleOffsets(IngresPartition partition, IngresOffsetContext offsetContext) {
        final TxLogPosition position = offsetContext.getChangePosition();

        // Check for stale state
        StaleOffsetsResult result = StaleOffsetsResult.fresh();
        if (Objects.equals(previousPosition, position)) {
            result = StaleOffsetsResult.stale(
                    ("Offset position %s has not changed in at least %d milliseconds. " +
                            "This may indicate the database is idle, there are no changes for the captured tables, " +
                            "or that the connector is no longer receiving records from the CDC log stream.")
                            .formatted(position, checkInterval.toMillis()));
        }

        // Update tracked stats
        previousPosition = position;

        return result;
    }

}