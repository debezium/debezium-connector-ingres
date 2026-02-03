/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import java.util.Objects;

import com.ingres.cdc.records.HeaderRecord;

import io.debezium.connector.Nullable;

/**
 * Defines a position of change in the transaction log. The position is defined as a combination of commit LSN
 * and sequence number of the change in the given transaction.
 * The sequence number is monotonically increasing in transaction but it is not guaranteed across multiple
 * transactions so the combination is necessary to get total order.
 *
 * @author Laoflch Luo, Xiaolin Zhang, Lars M Johansson
 *
 */
public class TxLogPosition implements Nullable, Comparable<TxLogPosition> {

    public static final TxLogPosition NULL = new TxLogPosition(null, null, null, -1L);

    private final Long txId;

    private final HeaderRecord commitRecord;
    private final HeaderRecord changeRecord;
    private final HeaderRecord beginRecord;

    public TxLogPosition(HeaderRecord commitRecord, HeaderRecord changeRecord, HeaderRecord beginRecord, Long txId) {
        this.commitRecord = commitRecord;
        this.changeRecord = changeRecord;
        this.beginRecord = beginRecord;
        this.txId = txId;
    }

    public TxLogPosition(HeaderRecord commitRecord, HeaderRecord changeRecord, HeaderRecord beginRecord) {
        this.commitRecord = commitRecord;
        this.changeRecord = changeRecord;
        this.beginRecord = beginRecord;
        this.txId = null;
    }

    public static TxLogPosition cloneAndSet(TxLogPosition position, HeaderRecord commitRecord, HeaderRecord changeRecord, long txId, HeaderRecord beginRecord) {

        HeaderRecord commitRec = commitRecord;
        HeaderRecord changeRec = changeRecord;
        HeaderRecord beginRec = beginRecord;

        if (commitRec == null) {
            commitRec = position.commitRecord;
        }
        else if (position.commitRecord != null) {
            commitRec = commitRecord.compareTo(position.commitRecord) > 0 ? commitRecord : position.commitRecord;
        }
        if (changeRec == null) {
            changeRec = position.changeRecord;
        }
        else if (position.changeRecord != null) {
            changeRec = changeRecord.compareTo(position.changeRecord) > 0 ? changeRecord : position.changeRecord;
        }
        if (beginRec == null) {
            beginRec = position.beginRecord;
        }
        else if (position.beginRecord != null) {
            beginRec = beginRecord.compareTo(position.beginRecord) > 0 ? beginRecord : position.beginRecord;
        }
        return new TxLogPosition(commitRec, changeRec, beginRec, txId >= 0 ? txId : position.txId);
    }

    public Long getTxId() {
        return txId;
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
        return this == NULL ? "NULL" : commitRecord + ":" + changeRecord + ":" + txId + ":" + beginRecord;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TxLogPosition that = (TxLogPosition) o;

        return Objects.equals(commitRecord, that.commitRecord)
                && Objects.equals(changeRecord, that.changeRecord)
                && Objects.equals(txId, that.txId)
                && Objects.equals(beginRecord, that.beginRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commitRecord, changeRecord, txId, beginRecord);
    }

    @Override
    public int compareTo(TxLogPosition o) {
        final int comparison = commitRecord.compareTo(o.getCommitRecord());
        return comparison == 0 ? changeRecord.compareTo(o.getChangeRecord()) : comparison;
    }

    @Override
    public boolean isAvailable() {
        return changeRecord != null && commitRecord != null && beginRecord != null && txId != null;
    }
}
