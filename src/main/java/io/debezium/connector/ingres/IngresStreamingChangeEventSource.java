/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import static java.lang.Thread.currentThread;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ingres.cdc.CDCLogStream;
import com.ingres.cdc.CDCPublication;
import com.ingres.cdc.CDCStreamException;
import com.ingres.cdc.api.CDCTransaction;
import com.ingres.cdc.api.DataRecord;
import com.ingres.cdc.api.LogRecord;
import com.ingres.cdc.api.OperationsRecord;
import com.ingres.cdc.api.RelationRecord;
import com.ingres.cdc.internal.LoggedDataRecord;
import com.ingres.cdc.records.HeaderRecord;
import com.ingres.cdc.records.RecordType;
import com.ingres.cdc.records.operations.BeginRecord;
import com.ingres.cdc.records.operations.CommitRecord;
import com.ingres.cdc.records.operations.TruncateRecord;
import com.ingres.cdc.transactions.CDCTransactionEngine;
import com.ingres.gcf.util.SqlData;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;
import io.debezium.util.Stopwatch;

public class IngresStreamingChangeEventSource implements StreamingChangeEventSource<IngresPartition, IngresOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngresStreamingChangeEventSource.class);

    private static final String RECEIVED_GENERIC_RECORD = "Received {} ElapsedT [{}ms]";
    private static final String RECEIVED_UNKNOWN_RECORD_TYPE = "Received unknown record-type {} ElapsedT [{}ms]";

    private final IngresConnectorConfig connectorConfig;
    private final IngresConnection dbConnection;
    private final EventDispatcher<IngresPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final IngresDatabaseSchema schema;
    private IngresOffsetContext effectiveOffsetContext;
    private final Map<Integer, TableId> idToTableId = new ConcurrentHashMap<>();

    public IngresStreamingChangeEventSource(IngresConnectorConfig connectorConfig,
                                              IngresConnection dataConnection, IngresConnection metadataConnection,
                                              EventDispatcher<IngresPartition, TableId> dispatcher, ErrorHandler errorHandler,
                                              Clock clock, IngresDatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.dbConnection = metadataConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
    }

    @Override
    public void init(IngresOffsetContext offsetContext) {
        this.effectiveOffsetContext = offsetContext == null
                ? new IngresOffsetContext(
                        connectorConfig,
                        new TxLogPosition(null, null, null, null),
                        null,
                        false)
                : offsetContext;
    }

    /**
     * Executes this source. Implementations should regularly check via the given context if they should stop. If that's
     * the case, they should abort their processing and perform any clean-up needed, such as rolling back pending
     * transactions, releasing locks etc.
     *
     * @param context contextual information for this source's execution
     * @throws InterruptedException in case the snapshot was aborted before completion
     */
    @Override
    public void execute(ChangeEventSourceContext context, IngresPartition partition, IngresOffsetContext offsetContext)
            throws InterruptedException {

    	
    	LOGGER.info("Starting Ingres CDC streaming tableIDs: {}", schema.tableIds());
        // Need to refresh schema before CDCEngine is started, to capture columns added in off-line schema evolution
        schema.tableIds().stream().map(TableId::schema).distinct().forEach(schemaName -> {
            try {
                dbConnection.readSchema(
                        schema.tables(),
                        null,
                        schemaName,
                        schema.getTableFilter(),
                        null,
                        true);
            }
            catch (SQLException e) {
                LOGGER.error("Caught SQLException", e);
                errorHandler.setProducerThrowable(e);
            }
        });

        TxLogPosition lastPosition = offsetContext.getChangePosition();
        
        HeaderRecord lastBeginRecord = lastPosition.getBeginRecord();
        HeaderRecord lastChangeRecord = lastPosition.getChangeRecord();
        HeaderRecord lastCommitRecord = lastPosition.getCommitRecord();
        HeaderRecord beginRecord = (lastBeginRecord == null) ? lastCommitRecord : lastBeginRecord; 
        try(CDCTransactionEngine engine = getTransactionEngine(context, schema, beginRecord).build()) {
        	/*
             * Recover Stage. In this stage, we replay event from 'beginLsn' to 'commitLsn', and rebuild the transactionCache.
             */
            if (beginRecord != null && beginRecord.compareTo(lastCommitRecord) < 0) {
                LOGGER.info("Begin recover: from lastBeginRecord='{}' to lastCommitRecord='{}'", lastBeginRecord, lastCommitRecord);
                boolean recovering = true;
                while (context.isRunning() && recovering) {

                    if (context.isPaused()) {
                        LOGGER.info("Streaming will now pause");
                        context.streamingPaused();
                        context.waitSnapshotCompletion();
                        LOGGER.info("Streaming resumed");
                    }

                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                    
                    LogRecord streamRecord = engine.get();
                    switch(streamRecord.getType()) {
                    case RELATION:
						handleMetadata(partition, offsetContext, engine, (RelationRecord)streamRecord);
						break;
					case ERROR:
						LOGGER.error(RECEIVED_GENERIC_RECORD, streamRecord, 0);
                        break;
					case TIMEOUT:
						LOGGER.trace(RECEIVED_GENERIC_RECORD, streamRecord, 0);
						break;
					case TRANSACTION:
						CDCTransaction transactionRecord = (CDCTransaction)streamRecord;
						HeaderRecord commitRecord = transactionRecord.getEndRecord().getHeader();
	                    if (commitRecord.compareTo(lastCommitRecord) < 0) {
	                        LOGGER.info("Skipping transaction with id: '{}' since commitRecord='{}' < lastCommitRecord='{}'",
	                                transactionRecord.getTransactionId(), commitRecord, lastCommitRecord);
	                        break;
	                    }
	                    if (commitRecord.equals(lastCommitRecord) && lastChangeRecord.equals(lastCommitRecord)) {
	                        LOGGER.info("Skipping transaction with id: '{}' since commitRecord='{}' == lastCommitLsn='{}' and lastChangeRecord='{}' == lastCommitRecord",
	                                transactionRecord.getTransactionId(), commitRecord, lastCommitRecord, lastChangeRecord);
	                        break;
	                    }
	                    if (commitRecord.compareTo(lastCommitRecord) > 0) {
	                        LOGGER.info("Recover finished: from lastBeginRecord='{}' to lastCommitRecord='{}', currentRecord='{}'",
	                                lastBeginRecord, lastCommitRecord, transactionRecord.getBeginRecord().getHeader());
	                        recovering = false;
	                    }
	                    handleTransaction(engine, partition, offsetContext, transactionRecord, recovering);
						break;
					default:
						LOGGER.warn(RECEIVED_UNKNOWN_RECORD_TYPE, streamRecord, 0);
						break;
                    }
                }
            }

            /*
             * Main Handler Loop
             */
            while (context.isRunning()) {

                if (context.isPaused()) {
                    LOGGER.info("Streaming will now pause");
                    context.streamingPaused();
                    context.waitSnapshotCompletion();
                    LOGGER.info("Streaming resumed");
                }

                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                LogRecord streamRecord = engine.get();
                switch (streamRecord.getType()) {
                case TRANSACTION:
                    handleTransaction(engine, partition, offsetContext, (CDCTransaction) streamRecord, false);
                    break;
                case RELATION:
                    handleMetadata(partition, offsetContext, engine, (RelationRecord) streamRecord);
                    break;
                case TIMEOUT:
                    LOGGER.trace(RECEIVED_GENERIC_RECORD, streamRecord, 0);
                    break;
                default:
                    LOGGER.warn(RECEIVED_UNKNOWN_RECORD_TYPE, streamRecord, 0);
                }
            }
        }
        catch (InterruptedException e) {
            LOGGER.error("Caught InterruptedException", e);
            errorHandler.setProducerThrowable(e);
            currentThread().interrupt();
        }
        catch (Exception e) {
            LOGGER.error("Caught Exception", e);
            errorHandler.setProducerThrowable(e);
        }
    }

    @Override
    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        // NOOP
    }

    @Override
    public IngresOffsetContext getOffsetContext() {
        return effectiveOffsetContext;
    }
    
	public CDCTransactionEngine.Builder getTransactionEngine(ChangeEventSourceContext context,
            IngresDatabaseSchema schema, HeaderRecord beginRecord) throws SQLException {
    		return CDCTransactionEngine.builder(getLogStreamEngine(schema, beginRecord)).onTimeout(r -> {
				if (context.isRunning()) {
					LOGGER.trace(RECEIVED_GENERIC_RECORD, r, 0);
				}
			});
    }
    
    public CDCLogStream.Builder getLogStreamEngine(IngresDatabaseSchema schema, HeaderRecord beginRecord) throws SQLException {
    	CDCLogStream.Builder builder = CDCLogStream.builder(dbConnection.datasource())
				.timeout(connectorConfig.getCdcTimeout())
				.startPosition(beginRecord)
				;

		schema.tableIds().forEach(tid -> {
			LOGGER.info("Table ID: {}", tid);
			CDCPublication p = new CDCPublication();
			p.name(tid.identifier())
				.table(tid.table())
				.owner(tid.schema())
				.attributes(CDCPublication.FULL_BEFORE_IMAGE)
				.columns(schema.tableFor(tid).retrieveColumnNames().toArray(String[]::new));
			builder.publication(p);
		});
		
		if (LOGGER.isInfoEnabled()) {
			LOGGER.info("CDCLogStream begin record set to '{}'", beginRecord);
		}

		return builder;
	}

    private void handleTransaction(CDCTransactionEngine engine, IngresPartition partition,
                                   IngresOffsetContext offsetContext, CDCTransaction transactionRecord,
                                   boolean recover)
            throws InterruptedException, CDCStreamException {
    	Stopwatch stopwatchOverall = Stopwatch.reusable();
        stopwatchOverall.start();

        HeaderRecord lastChangeRecord = offsetContext.getChangePosition().getChangeRecord();
        BeginRecord beginRecord = transactionRecord.getBeginRecord();
        OperationsRecord endRecord = transactionRecord.getEndRecord();
        long transactionId = beginRecord.getHeader().getTransactionId();

        Stopwatch stopwatch = Stopwatch.reusable();
        stopwatch.start();
        
        LocalDateTime beginTs = beginRecord.getTime();
        HeaderRecord beginHeader = beginRecord.getHeader();
        HeaderRecord endHeader = endRecord.getHeader();
        HeaderRecord restartHeader = engine.getLowestHeaderRecord().orElse(endHeader);
        restartHeader = (beginHeader.compareTo(restartHeader) <= 0)
				? beginHeader
				: restartHeader;
        
        if (!recover) {
            updateChangePosition(offsetContext, endHeader, beginHeader, transactionId, restartHeader);
            dispatcher.dispatchTransactionStartedEvent(
                    partition,
                    String.valueOf(transactionId),
                    offsetContext,
                    beginTs.atZone(ZoneId.systemDefault()).toInstant());
        }

        LOGGER.info("Received {} Time [{}] Owner [{}] ElapsedT [{}ms]",
                beginRecord, beginTs, beginRecord.getOwner(), stop(stopwatch));

        if (RecordType.COMMIT.equals(endRecord.getType())) {
            HeaderRecord commitHeader = endRecord.getHeader();
            LocalDateTime commitTime = endRecord.unwrap(CommitRecord.class).getTime();

            Map<String, SqlData> before = null;
            Map<String, SqlData> after = null;
            for (DataRecord streamRecord : transactionRecord.getDataRecords()) {
                stopwatch.start();
                before = after = null;
                HeaderRecord changeHeader = streamRecord.getHeader();

                if (recover && changeHeader.compareTo(lastChangeRecord) <= 0) {
                    LOGGER.info("Skipping already processed record {}", changeHeader);
                    continue;
                }

                //FIXME  test labels when we have multiple publications?                
                Optional<TableId> tableId = Optional.ofNullable(streamRecord.getId()).map(idToTableId::get);

                updateChangePosition(offsetContext, null, changeHeader, transactionId, null);

                switch (streamRecord.getType()) {
                    case INSERT:
                        after = streamRecord.unwrap(LoggedDataRecord.class).getAfterImage().asMap();
                        handleOperation(partition, offsetContext, Operation.CREATE, before, after, tableId.orElseThrow());

                        LOGGER.debug("Received {} ElapsedT [{}ms] Data After [{}]",
                                streamRecord, stop(stopwatch), after);
                        break;
                    case UPDATE:
                        before = streamRecord.unwrap(LoggedDataRecord.class).getBeforeImage().asMap();
                        after = streamRecord.unwrap(LoggedDataRecord.class).getAfterImage().asMap();
                        handleOperation(partition, offsetContext, Operation.UPDATE, before, after, tableId.orElseThrow());

                        LOGGER.debug("Received {} ElapsedT [{}ms] Data Before [{}]",
                                streamRecord, stop(stopwatch), before);
                        break;
                    case DELETE:
                        before = streamRecord.unwrap(LoggedDataRecord.class).getBeforeImage().asMap();
                        handleOperation(partition, offsetContext, Operation.DELETE, before, after, tableId.orElseThrow());

                        LOGGER.debug("Received {} ElapsedT [{}ms] Data Before [{}]",
                                streamRecord, stop(stopwatch), before);
                        break;
                    case TRUNCATE:
                    	TruncateRecord truncateRecord = streamRecord.unwrap(TruncateRecord.class);
                    	tableId = Optional.of(new TableId(this.dbConnection.getRealDatabaseName(), truncateRecord.getOwner(), truncateRecord.getTableName()));
                        handleOperation(partition, offsetContext, Operation.TRUNCATE, before, after, tableId.orElseThrow());
                        
                        LOGGER.debug(RECEIVED_GENERIC_RECORD, streamRecord, stop(stopwatch));
                        break;
                    case RELATION:
                    case TIMEOUT:
                        LOGGER.debug(RECEIVED_GENERIC_RECORD, streamRecord, stop(stopwatch));
                        break;
                    default:
                        LOGGER.debug(RECEIVED_UNKNOWN_RECORD_TYPE, streamRecord, stop(stopwatch));
                }
            }

            stopwatch.start();
            updateChangePosition(offsetContext, commitHeader, commitHeader, transactionId, restartHeader);
            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, commitTime.atZone(ZoneId.systemDefault()).toInstant());
            

            LOGGER.debug("Received {} Time [{}] Owner [{}] ElapsedT [{}ms]",
                    endRecord, commitTime, beginRecord.getOwner(), stop(stopwatch));

            LOGGER.debug("Handle Transaction Events [{}], ElapsedT [{}ms]",
                    transactionRecord.getDataRecords().size(), stop(stopwatchOverall));
        }
        else if (RecordType.ROLLBACK.equals(endRecord.getType())) {

            if (!recover) {
                updateChangePosition(offsetContext, endHeader, endHeader, transactionId, restartHeader);
                offsetContext.getTransactionContext().endTransaction();
            }
            stopwatchOverall.stop();
            LOGGER.debug(RECEIVED_GENERIC_RECORD, endRecord, stop(stopwatch));
        }
    }

    private void handleMetadata(IngresPartition partition, IngresOffsetContext offsetContext, CDCTransactionEngine engine,
                                RelationRecord metaDataRecord)
            throws InterruptedException {
        long start = System.nanoTime();
        this.idToTableId.put(metaDataRecord.getObjectId(), new TableId(this.dbConnection.getRealDatabaseName(), metaDataRecord.getOwner(), metaDataRecord.getTableName()));
        TableId tableId = idToTableId.get(metaDataRecord.getObjectId());
        
        offsetContext.event(tableId, Instant.now());

        dispatcher.dispatchSchemaChangeEvent(partition, offsetContext, null, receiver -> {
            final SchemaChangeEvent event = SchemaChangeEvent.ofAlter(
                    partition,
                    offsetContext,
                    tableId.catalog(),
                    tableId.schema(),
                    "n/a",
                    schema.tableFor(tableId));
            if (!schema.skipSchemaChangeEvent(event)) {
                receiver.schemaChangeEvent(event);
            }
        });
        long end = System.nanoTime();
        LOGGER.debug(RECEIVED_GENERIC_RECORD, metaDataRecord, (end - start) / 1000000d);
    }

    private void updateChangePosition(IngresOffsetContext offsetContext,
                                      HeaderRecord commitSeq, HeaderRecord changeSeq, long transactionId, HeaderRecord beginSeq) {
        offsetContext.setChangePosition(
                TxLogPosition.cloneAndSet(
                        offsetContext.getChangePosition(),
                        commitSeq,
                        changeSeq,
                        transactionId,
                        beginSeq));
    }

    private void handleOperation(IngresPartition partition, IngresOffsetContext offsetContext, Operation operation,
                                 Map<String, SqlData> before, Map<String, SqlData> after, TableId tableId)
            throws InterruptedException {
        offsetContext.event(tableId, clock.currentTime());

        dispatcher.dispatchDataChangeEvent(partition, tableId,
                new IngresChangeRecordEmitter(partition, offsetContext, operation,
                        IngresChangeRecordEmitter.convertData2Array(before, schema.schemaFor(tableId)),
                        IngresChangeRecordEmitter.convertData2Array(after, schema.schemaFor(tableId)),
                        clock, connectorConfig));
    }

    private long stop(Stopwatch stopwatch) {
		return stopwatch.stop().durations().statistics().getTotal().toMillis();
	}
}
