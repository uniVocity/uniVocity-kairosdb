/*******************************************************************************
 * Copyright (c) 2014 uniVocity Software Pty Ltd. All rights reserved.
 * This file is subject to the terms and conditions defined in file
 * 'LICENSE.txt', which is part of this source code package.
 ******************************************************************************/
package com.univocity.articles.kairosdb;

import javax.sql.*;

import org.apache.commons.lang.*;
import org.slf4j.*;

import com.univocity.api.*;
import com.univocity.api.config.*;
import com.univocity.api.config.builders.*;
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

		log.info("Starting {} with {} storing metadata in {}", getClass().getName(), database.getDatabaseName(), metadataDatabase.getDatabaseName());

		DataStoreConfiguration databaseConfig = createSourceDatabaseConfiguration();
		DataStoreConfiguration kairosConfig = createKairosDbConfiguration();
		MetadataSettings metadataConfig = createMetadataConfiguration();

		EngineConfiguration config = new EngineConfiguration(ENGINE_NAME, databaseConfig, kairosConfig);
		config.setMetadataSettings(metadataConfig);
		
		//This step is important: it makes uniVocity "know" how to initialize a data store from our KairosDataStoreConfiguration
		config.addCustomDataStoreFactories(new KairosDataStoreFactory());

		
		Univocity.registerEngine(config);
		engine = Univocity.getEngine(ENGINE_NAME);

		configureMappings();
	}

	/**
	 * Creates a {@link JdbcDataStoreConfiguration} configuration object with the appropriate settings
	 * for the underlying database.
	 *
	 * @return the configuration for the "database" data store.
	 */
	public DataStoreConfiguration createSourceDatabaseConfiguration() {
		//Gets a javax.sql.DataSource instance from the database object.
		DataSource dataSource = database.getDataSource();

		//Creates a the configuration of a data store named "database", with the given javax.sql.DataSource
		JdbcDataStoreConfiguration config = new JdbcDataStoreConfiguration(SOURCE, dataSource);

		//when reading from tables of this database, never load more than the given number of rows at once.
		//uniVocity will block any reading process until there's room for more rows.
		config.setLimitOfRowsLoadedInMemory(batchSize);
		
		//applies any additional configuration that is database-dependent. Refer to the implementations under package *com.univocity.articles.importcities.databases*
		database.applyDatabaseSpecificConfiguration(config);

		return config;
	}

	/**
	 * Creates a {@link KairosDataStoreConfiguration} configuration object with the appropriate settings
	 * to connect to and store information into a KairosDB server.
	 *
	 * @return the configuration for the "Kairos" data store.
	 */
	public DataStoreConfiguration createKairosDbConfiguration() {
		
		KairosDataStoreConfiguration config = new KairosDataStoreConfiguration(DESTINATION, "localhost:8080");
		
		//entity observations with tag "observationKind"
		config.addEntity("observations", "observationKind");

		return config;

	}

	/**
	 * Creates a {@link MetadataSettings} configuration object defining how uniVocity
	 * should store metadata generated in mappings where {@link PersistenceSetup#usingMetadata()} has been used.
	 *
	 * The database configured for metadata storage in /src/main/resources/connection.properties will be used.
	 *
	 * This is configuration is optional and if not provided uniVocity will create its own in-memory database.
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
	 * Executes a data mapping cycle. The actual data mappings are defined by subclasses
	 * in the {@link #configureMappings()} method.
	 */
	public void execute() {
		engine.executeCycle();
	}
	

	private void configureMappings(){
		engine.addFunction(EngineScope.STATELESS, "mergeFunction", new FunctionCall<String, Object[]>() {
			public String execute(Object[] input) {
				return StringUtils.join(input, '.');
			}
		});
		
		engine.addFunction(EngineScope.STATELESS, "from_s_to_ms", new FunctionCall<Long, Integer>(){
			public Long execute(Integer timeInSeconds) {
				return 1000L * timeInSeconds;
			}
		});
		
		
		DataStoreMapping mapping = engine.map(SOURCE, DESTINATION);
		
		EntityMapping map = mapping.map("observation", "observations");
		
		map.identity().associate("fieldUnitZoneIdentifier", "fieldUnitAddress", "deviceLabel").to("name").readWith("mergeFunction");
		map.value().copy("observationTimeEpochSeconds").to("timestamp").readingWith("from_s_to_ms");
		map.value().copy("observedValue").to("value");
		map.value().copy("observationKind").to("observationKind");
	}

	
	public static void main(String ... args){
		KairosDbLoadProcess process = new KairosDbLoadProcess();
		try{
			process.execute();
		} finally {
			process.shutdown();
		}
	}
}
