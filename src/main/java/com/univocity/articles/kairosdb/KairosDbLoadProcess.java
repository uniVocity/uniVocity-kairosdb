/*******************************************************************************
 * Copyright (c) 2014 uniVocity Software Pty Ltd. All rights reserved.
 * This file is subject to the terms and conditions defined in file
 * 'LICENSE.txt', which is part of this source code package.
 ******************************************************************************/
package com.univocity.articles.kairosdb;

import java.util.*;

import javax.sql.*;

import org.apache.commons.lang.*;
import org.slf4j.*;
import org.springframework.jdbc.core.*;

import com.univocity.api.*;
import com.univocity.api.config.*;
import com.univocity.api.config.builders.*;
import com.univocity.api.data.*;
import com.univocity.api.engine.*;
import com.univocity.api.entity.custom.*;
import com.univocity.api.entity.jdbc.*;
import com.univocity.articles.databases.*;
import com.univocity.articles.kairosdb.custom.*;

public class KairosDbLoadProcess {
	private static final Logger log = LoggerFactory.getLogger(KairosDbLoadProcess.class);
	private static final String ENGINE_NAME = "KAIROS_DB_LOAD";
	private static final String SOURCE = "database";
	private static final String DESTINATION = "kairos";
	private final Database database;
	private final Database metadataDatabase;
	private final DataIntegrationEngine engine;
	private int batchSize = 10000;

	public KairosDbLoadProcess() {
		this.database = DatabaseFactory.getInstance().getDestinationDatabase();
		this.metadataDatabase = DatabaseFactory.getInstance().getMetadataDatabase();
		DataStoreConfiguration databaseConfig = createSourceDatabaseConfiguration();
		DataStoreConfiguration kairosConfig = createKairosDbConfiguration();
		EngineConfiguration config = new EngineConfiguration(ENGINE_NAME, databaseConfig, kairosConfig);

		// This step is important: it makes uniVocity "know" how to initialize a
		// data store from our KairosDataStoreConfiguration
		config.addCustomDataStoreFactories(new KairosDataStoreFactory());
		Univocity.registerEngine(config);
		engine = Univocity.getEngine(ENGINE_NAME);
		configureMappings();
	}

	/**
	 * Creates a {@link JdbcDataStoreConfiguration} configuration object with
	 * the appropriate settings for the underlying database.
	 *
	 * @return the configuration for the "database" data store.
	 */
	public DataStoreConfiguration createSourceDatabaseConfiguration() {
		// Gets a javax.sql.DataSource instance from the database object.
		DataSource dataSource = database.getDataSource();
		// Creates a the configuration of a data store named "database", with
		// the given javax.sql.DataSource
		JdbcDataStoreConfiguration config = new JdbcDataStoreConfiguration(SOURCE, dataSource);
		// when reading from tables of this database, never load more than the
		// given number of rows at once.
		// uniVocity will block any reading process until there's room for more
		// rows.
		config.setLimitOfRowsLoadedInMemory(batchSize);
		// applies any additional configuration that is database-dependent.
		// Refer to the implementations under package
		// *com.univocity.articles.importcities.databases*
		database.applyDatabaseSpecificConfiguration(config);
		return config;
	}

	/**
	 * Creates a {@link KairosDataStoreConfiguration} configuration object with
	 * the appropriate settings to connect to and store information into a
	 * KairosDB server.
	 *
	 * @return the configuration for the "Kairos" data store.
	 */
	public DataStoreConfiguration createKairosDbConfiguration() {
		KairosDataStoreConfiguration config = new KairosDataStoreConfiguration(DESTINATION, "http://75.101.231.239:8080");
		// entity observations with tag "observationKind"
		config.addEntity("observations", "observationKind");
		return config;
	}

	/**
	 * Creates a {@link MetadataSettings} configuration object defining how
	 * uniVocity should store metadata generated in mappings where
	 * {@link PersistenceSetup#usingMetadata()} has been used.
	 *
	 * The database configured for metadata storage in
	 * /src/main/resources/connection.properties will be used.
	 *
	 * This is configuration is optional and if not provided uniVocity will
	 * create its own in-memory database.
	 *
	 * @return the configuration for uniVocity's metadata storage.
	 */
	public MetadataSettings createMetadataConfiguration() {
		MetadataSettings metadata = new MetadataSettings(metadataDatabase.getDataSource());
		metadata.setMetadataTableName("metadata");
		metadata.setTemporaryTableName("metadata_tmp");
		metadata.setBatchSize(batchSize);
		metadata.setFetchSize(batchSize);
		metadata.setTransactionIsolationLevel(java.sql.Connection.TRANSACTION_READ_COMMITTED);
		return metadata;
	}

	/**
	 * Shuts down the {@link #engine} used by this process
	 */
	public void shutdown() {
		Univocity.shutdown(ENGINE_NAME);
	}

	/**
	 * Executes a data mapping cycle. The actual data mappings are defined by
	 * subclasses in the {@link #configureMappings()} method.
	 */
	public void execute() {
		engine.executeCycle();
	}

	private void configureMappings() {
		engine.addFunction(EngineScope.STATELESS, "mergeFunction", new FunctionCall<String, Object[]>() {
			@Override
			public String execute(Object[] input) {
				return StringUtils.join(input, '.');
			}
		});
		engine.addFunction(EngineScope.STATELESS, "from_s_to_ms", new FunctionCall<Long, Integer>() {
			@Override
			public Long execute(Integer timeInSeconds) {
				if (timeInSeconds == null) { //some rows in the database have nulls here. Kairos uses the current time so I did the same here.
					return System.currentTimeMillis();
				}
				return 1000L * timeInSeconds;
			}
		});

		engine.addQuery(EngineScope.CYCLE, "getRangeOfRows").onDataStore(SOURCE).
				fromString("select "
						+ "		last_id as row_from, "
						+ "		(last_id + increment_size) as row_to "
						+ "	from "
						+ "		processed_rows "
						+ "	where "
						+ "		table_name = 'observation'"
				).returnSingleRow().directly().
				onErrorHandleWith(new DataErrorHandler<Long[]>() {
					@Override
					public Long[] handleException(Throwable t) {
						throw new IllegalStateException("Unexpected error", t);
					}

					@Override
					public Long[] handleUnexpectedData(Object[][] data) {
						//the first time this is executed, the query won't produce a row, so you'll get here
						if (data == null || data.length == 0) {
							Long firstId = new JdbcTemplate(database.getDataSource()).queryForObject("select min(id) from observation order by id", Long.class);

							new JdbcTemplate(database.getDataSource()).
									execute("insert into processed_rows (table_name, last_id, increment_size) values ('observation', " + firstId + ", 1000) ");
							return new Long[] { -1L, firstId + 1000L };
						}
						throw new IllegalStateException("Unexpected data: " + Arrays.toString(data));
					}
				});

		//let's query between a range of ID's
		engine.addQuery(EngineScope.STATELESS, "observationsAfter").onDataStore(SOURCE).
				fromString("select * from observation where id > :id_from and id < :id_to order by id").returnDataset();

		DataStoreMapping mapping = engine.map(SOURCE, DESTINATION);

		//queries that do not produce datasets are used as functions. Here we use the getRangeOfRows query.
		EntityMapping map = mapping.map("{observationsAfter(getRangeOfRows())}", "observations");

		map.identity().associate("fieldUnitZoneIdentifier", "fieldUnitAddress", "deviceLabel", "observationKind").to("name").readWith("mergeFunction");
		map.value().copy("observationTimeEpochSeconds").to("timestamp").readingWith("from_s_to_ms");
		map.value().copy("observedValue").to("value");
		map.value().copy("observationKind").to("observationKind");
		map.value().read("id"); //just load it to log the IDs of those rows with null observedValue
		map.persistence().notUsingMetadata().deleteDisabled().updateDisabled().insertNewRows();

		map.addInputRowReader(new RowReader() {

			private Long lastRowId = null;

			@Override
			public void processRow(Object[] inputRow, Object[] outputRow, RowMappingContext context) {
				//we need to weed out broken data here:

				if (context.getInputValue("observedValue") == null) {
					log.warn("Discarding row ID " + context.getInputValue("id") + " with null observedValue");
					context.discardRow();
				}
				lastRowId = context.getInputValue("id", Long.class);
			}

			@Override
			public void cleanup(RowMappingContext context) {
				//at the end of this mapping, we can update the last row id.
				if (lastRowId != null) {
					new JdbcTemplate(database.getDataSource()).execute("update processed_rows set last_id = " + lastRowId + " where table_name = 'observation'");
				}
			}
		});
	}

	public static void main(String... args) {
		new Thread() { // this is just a quick and dirty example, you should use a proper task scheduler
			@Override
			public void run() {
				KairosDbLoadProcess process = null;
				try {
					process = new KairosDbLoadProcess();
					while (true) {
						process.execute();
						try {
							Thread.sleep(3000); //let's pull 1000 rows every 3 seconds.
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
				} finally {
					if (process != null) {
						process.shutdown();
					}
				}
			}
		}.start();
	}
}
