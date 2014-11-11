uniVocity-kairosdb
==================

Sample project with a custom data store that enables [uniVocity](http://www.univocity.com/pages/univocity-features) to access KairosDB

## The code

When you create a custom data store, you need to implement at least:

  * A [CustomDataStore](https://github.com/uniVocity/univocity-api/blob/master/src/main/java/com/univocity/api/entity/custom/CustomDataStore.java)
  * A [CustomDataStoreConfiguration](https://github.com/uniVocity/univocity-api/blob/master/src/main/java/com/univocity/api/entity/custom/DataStoreConfiguration.java) and
  * A [CustomDataStoreFactory](https://github.com/uniVocity/univocity-api/blob/master/src/main/java/com/univocity/api/entity/custom/CustomDataStoreFactory.java)

We created the custom data store for KairosDB [here](./src/main/java/com/univocity/articles/kairosdb/custom).

### The configuration
  
With everything in place, you can simply create your [EngineConfiguration](https://github.com/uniVocity/univocity-api/blob/master/src/main/java/com/univocity/api/config/EngineConfiguration.java) and use your custom data store configuration: 

```
	KairosDataStoreConfiguration kairosConfig = new KairosDataStoreConfiguration(DESTINATION, "localhost:8080");

	//entity observations with tag "observationKind"
	kairosConfig.addEntity("observations", "observationKind");

	EngineConfiguration engineConfig = new EngineConfiguration(ENGINE_NAME, databaseConfig, kairosConfig);
	
```

You just need to tell uniVocity how to initialize your custom data store. To do this, simply add your custom data store factory to the engine configuration:

```
	engineConfig.addCustomDataStoreFactories(new KairosDataStoreFactory());
	
```

### The mappings:

This is business as usual. Just use your custom data store as if it were anything natively supported by [uniVocity](http://www.univocity.com/pages/univocity-features).

We recommend you to read [our tutorial](http://www.univocity.com/pages/univocity-tutorial) to learn more about what you can do with [uniVocity](http://www.univocity.com/pages/univocity-features). 

```
		engine.addFunction(EngineScope.STATELESS, "mergeFunction", new FunctionCall<String, Object[]>() {
			@Override
			public String execute(Object[] input) {
				return StringUtils.join(input, '.');
			}
		});

		engine.addFunction(EngineScope.STATELESS, "from_s_to_ms", new FunctionCall<Long, Integer>() {
			@Override
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
		
		//we are just inserting here:
		map.persistence().usingMetadata().deleteDisabled().updateDisabled().insertNewRows();
```

## Setting up

### Database setup.

We created scripts to generate the required tables for you in MySQL. This project includes scripts for [HSQLDB](./src/main/resources/database/hsqldb) to create the required tables to store metadata information (if required)

Simply edit the [connection.properties](./src/main/resources/connection.properties) file with the connection details for your database of choice. The tables will be created automatically when you execute the project.


### Process setup.

Before executing the [KairosDbLoadProcess.java] (./src/main/java/com/univocity/articles/kairosdb/KairosDbLoadProcess.java) class, you need to start up your KairosDB instance.
Possibly, you'll need to edit the following snippet to use your specific connection URL: 

```
	public DataStoreConfiguration createKairosDbConfiguration() {

		KairosDataStoreConfiguration config = new KairosDataStoreConfiguration(DESTINATION, "localhost:8080");

		//entity observations with tag "observationKind"
		config.addEntity("observations", "observationKind");

		return config;

	}
```

## Executing the process 

Just execute [KairosDbLoadProcess.java] (./src/main/java/com/univocity/articles/kairosdb/KairosDbLoadProcess.java) as a java program. The process will try to connect to your database and create the required tables if they are not present, and then process will start.

If this is the first time you execute uniVocity, a pop-up will will be displayed asking if you agree with the uniVocity free license terms and conditions. Once you agree it will disappear and the process will start normally. Keep in mind that the free license is only available for non-commercial purposes and batching is disabled.



