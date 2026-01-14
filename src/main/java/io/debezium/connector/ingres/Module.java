/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import java.util.Properties;

import io.debezium.util.IoUtil;

/**
 * Information about this module.
 *
 * @author Laoflch Luo, Xiaolin Zhang, Lars M Johansson
 *
 */
public final class Module {

    private static final Properties INFO = IoUtil.loadProperties(Module.class, "io/debezium/connector/ingres/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }

    /**
     * @return symbolic name of the connector plugin
     */
    public static String name() {
        return "ingres";
    }

    /**
     * @return context name used in log MDC and JMX metrics
     */
    public static String contextName() {
        return "Ingres_Server";
    }
}
