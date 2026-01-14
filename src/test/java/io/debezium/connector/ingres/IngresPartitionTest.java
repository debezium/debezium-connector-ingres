/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import io.debezium.connector.common.AbstractPartitionTest;

public class IngresPartitionTest extends AbstractPartitionTest<IngresPartition> {

    @Override
    protected IngresPartition createPartition1() {
        return new IngresPartition("database1");
    }

    @Override
    protected IngresPartition createPartition2() {
        return new IngresPartition("database2");
    }
}
