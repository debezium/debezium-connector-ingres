/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * The main task executing streaming from Ingres.
 * Responsible for lifecycle management the streaming code.
 *
 * @author Jiri Pechanec, Laoflch Luo, Lars M Johansson
 *
 */
public class IngresConnectorTask extends BaseSourceTask<IngresPartition, IngresOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngresConnectorTask.class);

    private static final String CONTEXT_NAME = "ingres-server-connector-task";

    private volatile IngresTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile IngresConnection dataConnection;
    private volatile ErrorHandler errorHandler;
    private volatile IngresDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected ChangeEventSourceCoordinator<IngresPartition, IngresOffsetContext> start(Configuration config) {
        final IngresConnectorConfig connectorConfig = new IngresConnectorConfig(config);
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        MainConnectionProvidingConnectionFactory<IngresConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new IngresConnection(connectorConfig.getJdbcConfig()));
        MainConnectionProvidingConnectionFactory<IngresConnection> cdcConnectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new IngresConnection(connectorConfig.getCdcJdbcConfig()));
        dataConnection = connectionFactory.mainConnection();
        try {
            dataConnection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }

        final IngresValueConverters valueConverters = new IngresValueConverters(connectorConfig.getDecimalMode(), connectorConfig.getTemporalPrecisionMode(),
                connectorConfig.binaryHandlingMode());
        schema = new IngresDatabaseSchema(connectorConfig, topicNamingStrategy, valueConverters, schemaNameAdjuster, dataConnection);
        schema.initializeStorage();

        Offsets<IngresPartition, IngresOffsetContext> previousOffsets = getPreviousOffsets(new IngresPartition.Provider(connectorConfig),
                new IngresOffsetContext.Loader(connectorConfig));

        // Manual Bean Registration
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, dataConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.VALUE_CONVERTER, valueConverters);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);

        // Service providers
        registerServiceProviders(connectorConfig.getServiceRegistry());

        final IngresPartition partition = previousOffsets.getTheOnlyPartition();
        final IngresOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

        validateAndLoadSchemaHistory(connectorConfig, dataConnection::validateLogPosition, previousOffsets, schema,
                snapshotterService.getSnapshotter());

        taskContext = new IngresTaskContext(connectorConfig, schema);

        final Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new ErrorHandler(IngresConnector.class, connectorConfig, queue, errorHandler);

        final IngresEventMetadataProvider metadataProvider = new IngresEventMetadataProvider();

        final SignalProcessor<IngresPartition, IngresOffsetContext> signalProcessor = new SignalProcessor<>(
                IngresConnector.class,
                connectorConfig,
                Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                previousOffsets);

        final EventDispatcher<IngresPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                null,
                connectorConfig.createHeartbeat(topicNamingStrategy, schemaNameAdjuster, null, null),
                schemaNameAdjuster,
                new IngresTransactionMonitor(
                        connectorConfig,
                        metadataProvider,
                        schemaNameAdjuster,
                        (record) -> {
                            queue.enqueue(new DataChangeEvent(record));
                        },
                        topicNamingStrategy.transactionTopic()),
                signalProcessor);

        final NotificationService<IngresPartition, IngresOffsetContext> notificationService = new NotificationService<>(
                getNotificationChannels(),
                connectorConfig,
                IngresSchemaFactory.get(),
                dispatcher::enqueueNotification);

        final ChangeEventSourceCoordinator<IngresPartition, IngresOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                IngresConnector.class,
                connectorConfig,
                new IngresChangeEventSourceFactory(connectorConfig, connectionFactory, cdcConnectionFactory, errorHandler, dispatcher, clock, schema,
                        snapshotterService),
                new DefaultChangeEventSourceMetricsFactory<>(),
                dispatcher, schema,
                signalProcessor,
                notificationService,
                snapshotterService);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {

        return queue.poll().stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());
    }

    @Override
    protected void doStop() {
        try {
            if (dataConnection != null) {
                // Server may have an active in-progress transaction associated with the connection and if so,
                // it will throw an exception during shutdown because the active transaction exists. This
                // is meant to help avoid this by rolling back the current active transaction, if exists.
                if (dataConnection.isConnected()) {
                    try {
                        dataConnection.rollback();
                    }
                    catch (SQLException e) {
                        // ignore
                    }
                }
                dataConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return IngresConnectorConfig.ALL_FIELDS;
    }
}
