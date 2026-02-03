/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import java.time.Instant;

import com.ingres.cdc.records.HeaderRecord;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

/**
 * Coordinates from the database log to establish the relation between the change streamed and the source log position.
 * Maps to {@code source} field in {@code Envelope}.
 *
 * @author Laoflch Luo, Xiaolin Zhang, Lars M Johansson
 *
 */
@NotThreadSafe
public class SourceInfo extends BaseSourceInfo {
    public static final String CHANGE_HEADER = "change_header";
    public static final String COMMIT_HEADER = "commit_header";
    public static final String BEGIN_HEADER = "begin_header";

    public static final String TX_ID_KEY = "txId"; // Schema name mapping collision

    private Long txId = -1L;
    private Instant timestamp;
    private TableId tableId;

    private HeaderRecord commitRecord;
    private HeaderRecord changeRecord;
    private HeaderRecord beginRecord;

    private final IngresConnectorConfig config;

    public SourceInfo(IngresConnectorConfig config) {
        super(config);
        this.config = config;
    }

    public Long getTxId() {
        return this.txId;
    }

    /**
     * @param txId - Id of the transaction whose part the change is
     */
    public void setTxId(Long txId) {
        this.txId = txId;
    }

    /**
     * @param timestamp a time at which the transaction commit was executed
     */
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public TableId getTableId() {
        return tableId;
    }

    /**
     * @param tableId - source table of the event
     */
    public void setTableId(TableId tableId) {
        this.tableId = tableId;
    }

    public void setCommitRecord(HeaderRecord commitRecord) {
        this.commitRecord = commitRecord;
    }

    public void setChangeRecord(HeaderRecord changeRecord) {
        this.changeRecord = changeRecord;
    }

    public void setBeginRecord(HeaderRecord beginRecord) {
        this.beginRecord = beginRecord;
    }

    public HeaderRecord getBeginRecord() {
        return this.beginRecord;
    }

    public HeaderRecord getChangeRecord() {
        return this.changeRecord;
    }

    public HeaderRecord getCommitRecord() {
        return this.commitRecord;
    }

    @Override
    public String toString() {
        return "SourceInfo [" +
                "serverName=" + serverName() +
                ", timestamp=" + timestamp +
                ", db=" + database() +
                ", snapshot=" + snapshotRecord +
                ", txId=" + txId + "]";
    }

    /**
     * @return timestamp of the event
     */
    @Override
    protected Instant timestamp() {
        return timestamp;
    }

    /**
     * @return name of the database
     */
    @Override
    protected String database() {
        return config.getDatabaseName();
    }
}
