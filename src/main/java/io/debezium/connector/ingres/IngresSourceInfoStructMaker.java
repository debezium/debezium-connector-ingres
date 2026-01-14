/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

public class IngresSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private Schema schema;
    


    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("io.debezium.connector.ingres.Source")
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TX_ID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.BEGIN_HEADER, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.CHANGE_HEADER, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.COMMIT_HEADER, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final Struct ret = super.commonStruct(sourceInfo);

        if (sourceInfo.getTableId() != null) {
            ret
            .put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.getTableId().schema())
            .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.getTableId().table());
        }
        if(sourceInfo.getBeginRecord()!= null) {
			ret.put(SourceInfo.BEGIN_HEADER, sourceInfo.getBeginRecord().toBase64());
		}
        if (sourceInfo.getCommitRecord() != null) {
            ret.put(SourceInfo.COMMIT_HEADER, sourceInfo.getCommitRecord().toBase64());
        }
        if (sourceInfo.getChangeRecord() != null) {
            ret.put(SourceInfo.CHANGE_HEADER, sourceInfo.getChangeRecord().toBase64());
        }
        
        //FIXME what to do about the txId?
        if (sourceInfo.getTxId() >= 0x00) {
            ret.put(SourceInfo.TX_ID_KEY, sourceInfo.getTxId().toString());
        }
        return ret;
    }

}
