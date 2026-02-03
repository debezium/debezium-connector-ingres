/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import java.util.Optional;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

public class IngresChangeEventSourceFactory implements ChangeEventSourceFactory<IngresPartition, IngresOffsetContext> {

    private final IngresConnectorConfig configuration;
    private final MainConnectionProvidingConnectionFactory<IngresConnection> connectionFactory;
    private final MainConnectionProvidingConnectionFactory<IngresConnection> cdcConnectionFactory;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<IngresPartition, TableId> dispatcher;
    private final Clock clock;
    private final IngresDatabaseSchema schema;
    private final SnapshotterService snapshotterService;

    public IngresChangeEventSourceFactory(IngresConnectorConfig configuration,
                                          MainConnectionProvidingConnectionFactory<IngresConnection> connectionFactory,
                                          MainConnectionProvidingConnectionFactory<IngresConnection> cdcConnectionFactory,
                                          ErrorHandler errorHandler, EventDispatcher<IngresPartition, TableId> dispatcher,
                                          Clock clock, IngresDatabaseSchema schema, SnapshotterService snapshotterService) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.cdcConnectionFactory = cdcConnectionFactory;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.snapshotterService = snapshotterService;
    }

    @Override
    public SnapshotChangeEventSource<IngresPartition, IngresOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<IngresPartition> snapshotProgressListener,
                                                                                                        NotificationService<IngresPartition, IngresOffsetContext> notificationService) {
        return new IngresSnapshotChangeEventSource(
                configuration,
                connectionFactory,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener,
                notificationService,
                snapshotterService);
    }

    @Override
    public StreamingChangeEventSource<IngresPartition, IngresOffsetContext> getStreamingChangeEventSource() {
        return new IngresStreamingChangeEventSource(
                configuration,
                cdcConnectionFactory.mainConnection(),
                connectionFactory.mainConnection(),
                dispatcher,
                errorHandler,
                clock,
                schema);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<IngresPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(IngresOffsetContext offsetContext,
                                                                                                                                               SnapshotProgressListener<IngresPartition> snapshotProgressListener,
                                                                                                                                               DataChangeEventListener<IngresPartition> dataChangeEventListener,
                                                                                                                                               NotificationService<IngresPartition, IngresOffsetContext> notificationService) {
        if (configuration.getSignalingDataCollectionIds().isEmpty()) {
            return Optional.empty();
        }
        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        return Optional.of(new SignalBasedIncrementalSnapshotChangeEventSource<>(
                configuration,
                connectionFactory.mainConnection(),
                dispatcher, schema, clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService));
    }
}
