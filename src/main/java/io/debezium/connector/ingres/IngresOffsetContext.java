/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import com.ingres.cdc.records.HeaderRecord;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

public class IngresOffsetContext extends CommonOffsetContext<SourceInfo> {

    private final Schema sourceInfoSchema;
    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;

    public IngresOffsetContext(IngresConnectorConfig connectorConfig, TxLogPosition position, SnapshotType snapshot, boolean snapshotCompleted,
                                 TransactionContext transactionContext, IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        super(new SourceInfo(connectorConfig), snapshotCompleted);

        sourceInfo.setCommitRecord(position.getCommitRecord());
        sourceInfo.setChangeRecord(position.getChangeRecord());
        sourceInfo.setBeginRecord(position.getBeginRecord());
        sourceInfoSchema = sourceInfo.schema();

        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            setSnapshot(snapshot);
            sourceInfo.setSnapshot(snapshot != null ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }

        this.transactionContext = transactionContext;

        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    public IngresOffsetContext(IngresConnectorConfig connectorConfig, TxLogPosition position, SnapshotType snapshot, boolean snapshotCompleted) {
        this(connectorConfig, position, snapshot, snapshotCompleted, new TransactionContext(), new SignalBasedIncrementalSnapshotContext<>(false));
    }

    @Override
    public Map<String, ?> getOffset() {
        if (getSnapshot().isPresent()) {
            return Collect.hashMapOf(
                    AbstractSourceInfo.SNAPSHOT_KEY, getSnapshot().get().toString(),
                    SNAPSHOT_COMPLETED_KEY, snapshotCompleted,
                    SourceInfo.COMMIT_HEADER, sourceInfo.getCommitRecord() == null ? null : sourceInfo.getCommitRecord().toBase64());
        }
        else {
            return incrementalSnapshotContext.store(transactionContext.store(Collect.hashMapOf(
                    SourceInfo.COMMIT_HEADER, sourceInfo.getCommitRecord() == null ? null : sourceInfo.getCommitRecord().toBase64(),
                    SourceInfo.CHANGE_HEADER, sourceInfo.getChangeRecord() == null ? null : sourceInfo.getChangeRecord().toBase64(),
                    SourceInfo.BEGIN_HEADER, sourceInfo.getBeginRecord() == null ? null : sourceInfo.getBeginRecord().toBase64())));
        }
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    public TxLogPosition getChangePosition() {
        return new TxLogPosition(sourceInfo.getCommitRecord(), sourceInfo.getChangeRecord(), sourceInfo.getBeginRecord(), sourceInfo.getTxId());
    }

    public void setChangePosition(TxLogPosition position) {
        sourceInfo.setBeginRecord(position.getBeginRecord());
        sourceInfo.setChangeRecord(position.getChangeRecord());
        sourceInfo.setCommitRecord(position.getCommitRecord());
        sourceInfo.setTxId(position.getTxId());
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        sourceInfo.setTimestamp(timestamp);
        sourceInfo.setTableId((TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    @Override
    public String toString() {
        return "IngresOffsetContext [" +
                "sourceInfoSchema=" + sourceInfoSchema +
                ", sourceInfo=" + sourceInfo +
                ", snapshotCompleted=" + snapshotCompleted + "]";
    }

    public static class Loader implements OffsetContext.Loader<IngresOffsetContext> {

        private final IngresConnectorConfig connectorConfig;

        public Loader(IngresConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public IngresOffsetContext load(Map<String, ?> offset) {
        	final HeaderRecord commitRecord = offset.get(SourceInfo.COMMIT_HEADER) == null ? null : HeaderRecord.deserializeFromBase64(offset.get(SourceInfo.COMMIT_HEADER).toString());
        	final HeaderRecord changeRecord = offset.get(SourceInfo.CHANGE_HEADER) == null ? null : HeaderRecord.deserializeFromBase64(offset.get(SourceInfo.CHANGE_HEADER).toString());
        	final HeaderRecord beginRecord =  offset.get(SourceInfo.BEGIN_HEADER) == null ? null : HeaderRecord.deserializeFromBase64(offset.get(SourceInfo.BEGIN_HEADER).toString());

            final SnapshotType snapshot = loadSnapshot(offset).orElse(null);
            boolean snapshotCompleted = loadSnapshotCompleted(offset);

            return new IngresOffsetContext(connectorConfig, new TxLogPosition(commitRecord, changeRecord, beginRecord), snapshot, snapshotCompleted,
                    TransactionContext.load(offset), SignalBasedIncrementalSnapshotContext.load(offset, false));
        }
    }
}
