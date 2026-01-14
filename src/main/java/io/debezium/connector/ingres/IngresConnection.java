/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;


import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ingres.jdbc.IngresDriver;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.util.Strings;

/**
 * {@link JdbcConnection} extension to be used with HCL Ingres
 *
 * @author Laoflch Luo, Xiaolin Zhang, Lars M Johansson
 *
 */
public class IngresConnection extends JdbcConnection {
	private final static Logger LOGGER = LoggerFactory.getLogger(IngresConnection.class);
    private static final String GET_DATABASE_NAME = "SELECT DBMSINFO('database') AS dbname";

    //FIXME Need to get the current timestamp
    private static final String GET_CURRENT_TIMESTAMP = "SELECT CURRENT_TIMESTAMP";

    private static final String QUOTED_CHARACTER = ""; // TODO: Unless DELIMIDENT is set, column names cannot be quoted

    //jdbc:ingres://usau-ninturi-01.actian.com:ii7/imadb;encrypt=on;user=the-user;password=the-password
    private static final String URL_PATTERN = "jdbc:ingres://${"
            + JdbcConfiguration.HOSTNAME + "}:${"
            + JdbcConfiguration.PORT + "}/${"
            + JdbcConfiguration.DATABASE + "};user=${"
            + JdbcConfiguration.USER + "};password=${"
            + JdbcConfiguration.PASSWORD + "};"
            + "encrypt=on";

    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(
            URL_PATTERN,
            IngresDriver.class.getCanonicalName(),
            IngresConnection.class.getClassLoader(),
            JdbcConfiguration.PORT.withDefault(IngresConnectorConfig.PORT.defaultValueAsString()));

    /**
     * actual name of the database, which could differ in casing from the database name given in the connector config.
     */
    private final String realDatabaseName;

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param config {@link Configuration} instance, may not be null.
     */
    public IngresConnection(JdbcConfiguration config) {
        super(config, FACTORY, QUOTED_CHARACTER, QUOTED_CHARACTER);
        realDatabaseName = retrieveRealDatabaseName().trim();
    }
    
	@Override
	public String resolveCatalogName(String catalogName) {
		return catalogName != null ? catalogName : realDatabaseName;
	}
	
	/**
	 * Special resolver for Ingres to read table names and add on the database name
	 */
	@Override
	public Set<TableId> readTableNames(String databaseCatalog, String schemaNamePattern, String tableNamePattern,
			String[] tableTypes) throws SQLException {
		if (tableNamePattern == null) {
			tableNamePattern = "%";
		}
		Set<TableId> tableIds = new HashSet<>();
		DatabaseMetaData metadata = connection().getMetaData();
		try (ResultSet rs = metadata.getTables(databaseCatalog, schemaNamePattern, tableNamePattern, tableTypes)) {
			while (rs.next()) {
				String catalogName = resolveCatalogName(rs.getString(1));
				String schemaName = rs.getString(2);
				String tableName = rs.getString(3);
				TableId tableId = new TableId(catalogName, schemaName, tableName);
				tableIds.add(tableId);
			}
		}
		return tableIds;
	}

    private String retrieveRealDatabaseName() {
        try {
            return queryAndMap(GET_DATABASE_NAME, singleResultMapper(rs -> rs.getString(1), "Could not retrieve database name"));
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't obtain database name", e);
        }
    }

    public String getRealDatabaseName() {
        return realDatabaseName;
    }

    public boolean validateLogPosition(Partition partition, OffsetContext offset, CommonConnectorConfig config) {
    	// Since we can read older logs this is always true
    	return true;
    }
    
	@Override
    public void readSchema(Tables tables, String databaseCatalog, String schemaNamePattern, TableFilter tableFilter,
			ColumnNameFilter columnFilter, boolean removeTablesNotFoundInJdbc) throws SQLException {
		// Before we make any changes, get the copy of the set of table IDs ...
		Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());

		// Read the metadata for the table columns ...
		DatabaseMetaData metadata = connection().getMetaData();

		// Find regular and materialized views as they cannot be snapshotted
		final Set<TableId> viewIds = new HashSet<>();
		final Set<TableId> tableIds = new HashSet<>();

		Map<TableId, List<Attribute>> attributesByTable = new HashMap<>();

		try (ResultSet rs = metadata.getTables(databaseCatalog, schemaNamePattern, null, supportedTableTypes())) {
			while (rs.next()) {
				final String catalogName = resolveCatalogName(rs.getString(1));
				final String schemaName = rs.getString(2);
				final String tableName = rs.getString(3);
				final String tableType = rs.getString(4);
				if (isTableType(tableType)) {
					TableId tableId = new TableId(catalogName, schemaName, tableName);
					if (tableFilter == null || tableFilter.isIncluded(tableId)) {
						tableIds.add(tableId);
						attributesByTable.putAll(getAttributeDetails(tableId, tableType));
					}
				} else {
					TableId tableId = new TableId(catalogName, schemaName, tableName);
					viewIds.add(tableId);
				}
			}
		}
		LOGGER.debug("{} table(s) will be scanned", tableIds.size());

		Map<TableId, List<Column>> columnsByTable = new HashMap<>();

		//Unlike other databases, Ingres getColumns returns columns for non-table entities too, so we need to query per table
		for (TableId includeTable : tableIds) {
			LOGGER.debug("Retrieving columns of table {}", includeTable);

			Map<TableId, List<Column>> cols = getColumnsDetails(databaseCatalog, schemaNamePattern,
					includeTable.table(), tableFilter, columnFilter, metadata, viewIds);
			columnsByTable.putAll(cols);
		}

		// Read the metadata for the primary keys ...
		for (Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
			// First get the primary key information, which must be done for *each* table ...
			List<String> pkColumnNames = readPrimaryKeyOrUniqueIndexNames(metadata, tableEntry.getKey());

			// Then define the table ...
			List<Column> columns = tableEntry.getValue();
			Collections.sort(columns);
			String defaultCharsetName = null; // JDBC does not expose character sets
			List<Attribute> attributes = attributesByTable.getOrDefault(tableEntry.getKey(), Collections.emptyList());
			tables.overwriteTable(tableEntry.getKey(), columns, pkColumnNames, defaultCharsetName, attributes);
		}

		if (removeTablesNotFoundInJdbc) {
			// Remove any definitions for tables that were not found in the database metadata ...
			tableIdsBefore.removeAll(columnsByTable.keySet());
			tableIdsBefore.forEach(tables::removeTable);
		}
	}

    /**
     * Returns a JDBC connection string for the current configuration.
     *
     * @return a {@code String} where the variables in {@code urlPattern} are replaced with values from the configuration
     */
    public String connectionString() {
        return connectionString(URL_PATTERN);
    }

    @Override
    public Optional<Instant> getCurrentTimestamp() throws SQLException {
        return queryAndMap(GET_CURRENT_TIMESTAMP, rs -> rs.next() ? Optional.of(rs.getTimestamp(1).toInstant()) : Optional.empty());
    }

    @Override
    public Optional<Boolean> nullsSortLast() {
        return Optional.of(false);
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        // TODO: Unless DELIMIDENT is set, table names cannot be quoted
        StringBuilder builder = new StringBuilder();

        String catalogName = tableId.catalog();
        if (!Strings.isNullOrBlank(catalogName)) {
            builder.append(catalogName).append(':');
        }

        String schemaName = tableId.schema();
        if (!Strings.isNullOrBlank(schemaName)) {
            builder.append(schemaName).append('.');
        }

        return builder.append(tableId.table()).toString();
    }

    @Override
    public String quotedColumnIdString(String columnName) {
        // TODO: Unless DELIMIDENT is set, column names cannot be quoted
        return columnName;
    }

    public DataSource datasource() {
        return new DataSource() {
            private PrintWriter logWriter;

            @Override
            public Connection getConnection() throws SQLException {
            	JdbcConfiguration config = JdbcConfiguration.copy(config()).build();
                return FACTORY.connect(config);
            }

            @Override
            public Connection getConnection(String username, String password) throws SQLException {
                JdbcConfiguration config = JdbcConfiguration.copy(config()).with(JdbcConfiguration.USER, username).with(JdbcConfiguration.PASSWORD, password).build();
                return FACTORY.connect(config);
            }

            @Override
            public PrintWriter getLogWriter() {
                return this.logWriter;
            }

            @Override
            public void setLogWriter(PrintWriter out) {
                this.logWriter = out;
            }

            @Override
            public void setLoginTimeout(int seconds) {
                throw new UnsupportedOperationException("setLoginTimeout");
            }

            @Override
            public int getLoginTimeout() {
                return (int) config().getConnectionTimeout().toSeconds();
            }

            @Override
            public java.util.logging.Logger getParentLogger() {
                return java.util.logging.Logger.getLogger("io.debezium.connector.ingres");
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> T unwrap(Class<T> iface) throws SQLException {
                if (iface.isInstance(this)) {
                    return (T) this;
                }
                throw new SQLException("DataSource of type [" + getClass().getName() + "] cannot be unwrapped as [" + iface.getName() + "]");
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) throws SQLException {
                return iface.isInstance(this);
            }
        };
    }

}
