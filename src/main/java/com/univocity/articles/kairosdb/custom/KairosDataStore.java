/*******************************************************************************
 * Copyright (c) 2014 uniVocity Software Pty Ltd. All rights reserved.
 * This file is subject to the terms and conditions defined in file
 * 'LICENSE.txt', which is part of this source code package.
 ******************************************************************************/
package com.univocity.articles.kairosdb.custom;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import org.kairosdb.client.*;
import org.kairosdb.client.builder.*;
import org.kairosdb.client.response.*;
import org.slf4j.*;

import com.univocity.api.entity.custom.*;

/**
 * This {@link CustomDataStore} demonstrates how you can implement your own data store for
 * abstract virtually any repository of information you want to integrate with uniVocity.
 *
 * @author uniVocity Software Pty Ltd - <a href="mailto:dev@univocity.com">dev@univocity.com</a>
 *
 */
class KairosDataStore implements CustomDataStore<KairosDataEntity> {

	private static final Logger log = LoggerFactory.getLogger(KairosDataStore.class);

	private final Set<KairosDataEntity> entities = new HashSet<KairosDataEntity>();

	private final KairosDataStoreConfiguration configuration;

	private HttpClient activeClient;

	//Creates a new custom data store and initializes custom entities based on our own configuration class.
	public KairosDataStore(KairosDataStoreConfiguration configuration) {
		this.configuration = configuration;
		createEntities();
	}

	private void createEntities() {
		for (Entry<String, String[]> e : configuration.entities.entrySet()) {
			entities.add(new KairosDataEntity(this, e.getKey(), e.getValue()));
		}
	}

	@Override
	public Set<KairosDataEntity> getDataEntities() {
		return entities;
	}

	@Override
	public Set<? extends CustomQuery> getQueries() {
		return Collections.emptySet();
	}

	@Override
	public CustomQuery addQuery(String queryName, String query) {
		return null;
	}

	@Override
	public void executeInTransaction(TransactionalOperation operation) {
		if (activeClient == null) {
			try {
				activeClient = new HttpClient(configuration.getUrl());
			} catch (Exception e) {
				throw new IllegalStateException("Unable to connect to KairosDB using URL: " + configuration.getUrl(), e);
			}
		}
		try {
			//operation will come from within uniVocity and is basically some processes around the the WritingProcess we created in KairosDataEntity.prepareToWrite()
			operation.execute();
		} finally {
			try {
				if (activeClient != null) {
					activeClient.shutdown();
				}
			} catch (IOException e) {
				log.error("Unexpected error shutting down connection to KairosDB", e);
			}
		}
	}

	private String describeMe(KairosDataEntity entity) {
		return "KairosDB (" + configuration.getDataStoreName() + " - " + configuration.getUrl() + " through " + entity.getEntityName();
	}

	void pushMetrics(KairosDataEntity entity, MetricBuilder builder) {
		try {
			Response response = activeClient.pushMetrics(builder);
			if (response != null) {
				for (String error : response.getErrors()) {
					log.warn("Error pushing metrics to KairosDB {}: {}", describeMe(entity), error);
				}
			}
		} catch (Exception e) {
			throw new IllegalStateException("Unable to push metrics to KairoDB " + describeMe(entity), e);
		}
	}

	@Override
	public DataStoreConfiguration getConfiguration() {
		return configuration;
	}

}
