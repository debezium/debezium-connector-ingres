/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.ingres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ingres.cdc.CDCLogStream;
import com.ingres.cdc.CDCPublication;
import com.ingres.cdc.records.HeaderRecord;
import com.ingres.cdc.records.operations.CommitRecord;

import io.debezium.connector.ingres.IngresConnectorConfig.SnapshotIsolationMode;
import io.debezium.connector.ingres.IngresOffsetContext.Loader;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

public class IngresSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<IngresPartition, IngresOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngresSnapshotChangeEventSource.class);

    private final IngresConnectorConfig connectorConfig;
    private final IngresConnection jdbcConnection;

    public IngresSnapshotChangeEventSource(IngresConnectorConfig connectorConfig,
                                             MainConnectionProvidingConnectionFactory<IngresConnection> connectionFactory,
                                             IngresDatabaseSchema schema, EventDispatcher<IngresPartition, TableId> dispatcher,
                                             Clock clock, SnapshotProgressListener<IngresPartition> snapshotProgressListener,
                                             NotificationService<IngresPartition, IngresOffsetContext> notificationService,
                                             SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = connectionFactory.mainConnection();
    }

    @Override
    protected SnapshotContext<IngresPartition, IngresOffsetContext> prepare(IngresPartition partition, boolean onDemand) {
        return new IngresSnapshotContext(partition, jdbcConnection.getRealDatabaseName(), onDemand);
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext<IngresPartition, IngresOffsetContext> snapshotContext) throws Exception {
        ((IngresSnapshotContext) snapshotContext).isolationLevelBeforeStart = jdbcConnection.connection().getTransactionIsolation();
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<IngresPartition, IngresOffsetContext> ctx) throws Exception {
        return jdbcConnection.readAllTableNames(new String[]{ "TABLE" });
    }
   
    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<IngresPartition, IngresOffsetContext> snapshotContext)
            throws SQLException, InterruptedException {
        if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.READ_UNCOMMITTED) {
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
        else if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.READ_COMMITTED) {
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
        else if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.EXCLUSIVE
                || connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.REPEATABLE_READ) {
        	
        	//FIXME: Do we need this?
            //jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            ((IngresSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint = jdbcConnection.connection().setSavepoint("ingres_schema_snapshot");

            LOGGER.info("Executing schema locking");
            //ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
            try (Statement statement = jdbcConnection.connection().createStatement()) {
                for (TableId tableId : snapshotContext.capturedTables) {
                    if (!sourceContext.isRunning()) {
                        throw new InterruptedException("Interrupted while locking table " + tableId);
                    }

                    String fullTableName = String.format("%s.%s", tableId.schema(), tableId.table());
                    Optional<String> lockingStatement = snapshotterService.getSnapshotLock().tableLockingStatement(
                            connectorConfig.snapshotLockTimeout(),
                            fullTableName);

                    if (lockingStatement.isPresent()) {
                        LOGGER.info("Locking table {}", tableId);
                        statement.execute(lockingStatement.get());
                        jdbcConnection.connection().commit();
                    }
                }
            }
        }
        else {
            throw new IllegalStateException("Unknown locking mode specified.");
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<IngresPartition, IngresOffsetContext> snapshotContext) throws SQLException {
        // Exclusive mode: locks should be kept until the end of transaction.
        // read_uncommitted mode; read_committed mode: no locks have been acquired.
        if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.REPEATABLE_READ) {
            jdbcConnection.connection().rollback(((IngresSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
            LOGGER.info("Schema locks released.");
        }
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<IngresPartition, IngresOffsetContext> ctx, IngresOffsetContext previousOffset)
            throws SQLException {
    	
    	LOGGER.info("Determining Ingres snapshot offset.");
    	
        IngresOffsetContext offset = ctx.offset;
        if (offset == null) {
            if (previousOffset != null) {
                offset = previousOffset;
            }
            else {
            	LOGGER.info("No previous offset available, determining current commit log position.");
            	HeaderRecord currentCommitRecord = null;
            	DataSource ds = jdbcConnection.datasource();
            	try(Connection c = ds.getConnection()) {
        			try(Statement s = c.createStatement()) {
        				String tableName = connectorConfig.getMarkerTable();
        				s.execute("CREATE TABLE IF NOT EXISTS " + tableName + " (c char(20))");
        				TableId markerTable = jdbcConnection.readTableNames(null, null, tableName, null).iterator().next();
        				if(markerTable == null) {
							throw new SQLException("Ingres Connector failed to detect CDC header marker table.");
						}
        				try(CDCLogStream stream = CDCLogStream.builder(ds)
        						.timeout(1)
        						.publication(new CDCPublication()
        								.name(tableName)
        								.table(tableName)
        								.owner(markerTable.schema())
        						)
        						.build()) {
        					
        					String uuid = UUID.randomUUID().toString();
        					
        					s.execute("INSERT INTO " + tableName + " values(" + "'" + uuid + "'" + ")");
        					s.execute("DELETE FROM " + tableName + " WHERE c = " + "'" + uuid + "'");
        					currentCommitRecord = Stream.generate(stream)
        					.limit(5)
        					.filter(r -> r instanceof CommitRecord)
        					.findFirst().orElseThrow(() -> new SQLException("Ingres Connector failed to read CDC header marker."))
        					.unwrap(CommitRecord.class)
        					.getHeader();
        				}
        				//FIXME: find a way to drop this and ignore this table in the schema detection
        				s.execute("DROP TABLE IF EXISTS cdc_header_marker");
        			}
        		}
            	LOGGER.info("Current commit log position determined: {}", currentCommitRecord);
                offset = new IngresOffsetContext(
                        connectorConfig,
                        new TxLogPosition(currentCommitRecord, null, null),
                        null,
                        false);
            }
            ctx.offset = offset;
        }
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<IngresPartition, IngresOffsetContext> snapshotContext,
                                      IngresOffsetContext offsetContext, SnapshottingTask snapshottingTask)
            throws SQLException, InterruptedException {
        Set<String> schemas = getTablesForSchemaChange(snapshotContext).stream().map(TableId::schema).collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables,
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        // however, for users interested only in captured tables, we need to pass also table filter
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            LOGGER.info("Reading structure of schema '{}'", schema);

            TableFilter tableFilter = null;
            if (snapshottingTask.isOnDemand()) {
                tableFilter = TableFilter.fromPredicate(snapshotContext.capturedTables::contains);
            }
            else if (connectorConfig.storeOnlyCapturedTables()) {
                tableFilter = connectorConfig.getTableFilters().dataCollectionFilter();
            }

            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    null,
                    schema,
                    tableFilter,
                    null,
                    false);
        }
    }

    @Override
    protected Collection<TableId> getTablesForSchemaChange(RelationalSnapshotContext<IngresPartition, IngresOffsetContext> snapshotContext) {
        return connectorConfig.storeOnlyCapturedTables() ? snapshotContext.capturedTables : snapshotContext.capturedSchemaTables;
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<IngresPartition, IngresOffsetContext> snapshotContext, Table table) {
        return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset, snapshotContext.catalogName, table);
    }

    @Override
    protected void completed(SnapshotContext<IngresPartition, IngresOffsetContext> snapshotContext) {
        close(snapshotContext);
    }

    @Override
    protected void aborted(SnapshotContext<IngresPartition, IngresOffsetContext> snapshotContext) {
        close(snapshotContext);
    }

    private void close(SnapshotContext<IngresPartition, IngresOffsetContext> snapshotContext) {
        try {
            jdbcConnection.connection().setTransactionIsolation(((IngresSnapshotContext) snapshotContext).isolationLevelBeforeStart);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to set transaction isolation level.", e);
        }
    }

    /**
     * Generate a valid query string for the specified table
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<IngresPartition, IngresOffsetContext> snapshotContext, TableId tableId,
                                                 List<String> columns) {
        String fullTableName = String.format("%s.%s", tableId.schema(), tableId.table());
        return snapshotterService.getSnapshotQuery().snapshotQuery(fullTableName, columns);
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class IngresSnapshotContext extends RelationalSnapshotContext<IngresPartition, IngresOffsetContext> {

        private int isolationLevelBeforeStart;
        private Savepoint preSchemaSnapshotSavepoint;

        IngresSnapshotContext(IngresPartition partition, String catalogName, boolean onDemand) {
            super(partition, catalogName, onDemand);
        }
    }

    @Override
    protected IngresOffsetContext copyOffset(RelationalSnapshotContext<IngresPartition, IngresOffsetContext> snapshotContext) {
        return new Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }

    @Override
    protected ResultSet resultSetForDataEvents(String selectStatement, Statement statement)
            throws SQLException {
        return statement.executeQuery(selectStatement);
    }
}
