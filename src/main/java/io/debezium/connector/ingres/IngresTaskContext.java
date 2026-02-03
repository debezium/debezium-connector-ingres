/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * A state (context) associated with an Ingres task
 *
 * @author Jiri Pechanec, Lars M Johansson
 *
 */
public class IngresTaskContext extends CdcSourceTaskContext<IngresConnectorConfig> {
    public IngresTaskContext(Configuration rawConfig, IngresConnectorConfig config) {
        super(rawConfig, config, config.getCustomMetricTags());
    }
}
