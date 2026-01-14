/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.kafka.connect.data.Field;

import com.ingres.gcf.util.SqlData;
import com.ingres.gcf.util.SqlNull;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope.Operation;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

/**
 * Emits change data based on a single (or two in case of updates) CDC data row(s).
 *
 * @author Laoflch Luo, Xiaolin Zhang, Lars M Johansson
 *
 */
public class IngresChangeRecordEmitter extends RelationalChangeRecordEmitter<IngresPartition> {

    private final Operation operation;
    private final Object[] before;
    private final Object[] after;

    public IngresChangeRecordEmitter(IngresPartition partition, IngresOffsetContext offsetContext, Operation operation,
                                       Object[] before, Object[] after, Clock clock, IngresConnectorConfig connectorConfig) {
        super(partition, offsetContext, clock, connectorConfig);

        this.operation = operation;
        this.before = before;
        this.after = after;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return before;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return after;
    }

    @Override
    protected void emitTruncateRecord(Receiver<IngresPartition> receiver, TableSchema tableSchema) throws InterruptedException {
        receiver.changeRecord(getPartition(), tableSchema, Operation.TRUNCATE, null,
                tableSchema.getEnvelopeSchema().truncate(getOffset().getSourceInfo(), getClock().currentTimeAsInstant()),
                getOffset(), null);
    }

    /**
     * Convert columns data from Map[String,SqlData] to Object[].
     * Debezium can't convert the SqlData object to kafka direct,so use map[AnyRef](x=>x.toObject) to extract the java
     * type value from SqlData and pass to debezium for kafka
     *
     * @param data the data from Ingres cdc map[String,SqlData].
     * @author Laoflch Luo, Xiaolin Zhang
     */
    public static Object[] convertData2Array(Map<String, SqlData> data, TableSchema tableSchema) {
        return data == null ? new Object[0]
                : tableSchema.valueSchema().fields().stream()
                        .map(Field::name)
                        .map(data::get)
                        // SqlNull does not work with calls to getObject(), so we need to handle it separately
                        .map(irt -> propagate(irt.getClass().equals(SqlNull.class) ? () -> null : irt::getObject)).toArray();
    }

    private static <X> X propagate(Callable<X> callable) {
        try {
            return callable.call();
        }
        catch (RuntimeException rex) {
            throw rex;
        }
        catch (Exception ex) {
            throw new DebeziumException(ex);
        }
    }
}
