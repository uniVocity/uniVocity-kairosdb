/*******************************************************************************
 * Copyright (c) 2014 uniVocity Software Pty Ltd. All rights reserved.
 * This file is subject to the terms and conditions defined in file
 * 'LICENSE.txt', which is part of this source code package.
 ******************************************************************************/
package com.univocity.articles.kairosdb.custom;

import java.util.*;

import org.apache.commons.lang.*;

import com.univocity.api.entity.custom.*;

/**
 * This is a configuration class for our custom data store {@link KairosDataStore}.
 * It provides methods to add configure access to KairosDB and allows us to define new custom data entities (i.e. {@link KairosDataEntity})
 *
 * @author uniVocity Software Pty Ltd - <a href="mailto:dev@univocity.com">dev@univocity.com</a>
 *
 */
public class KairosDataStoreConfiguration extends DataStoreConfiguration {

	private String url;

	final Map<String, String[]> entities = new HashMap<String, String[]>();

	public KairosDataStoreConfiguration(String dataStoreName, String url) {
		super(dataStoreName);
		if (StringUtils.isBlank(url)) {
			throw new IllegalArgumentException("KairosDB connection URL cannot be null.");
		}
		this.url = url;
	}

	public void addEntity(String entityName, String... tags) {
		entities.put(entityName, tags);
	}

	/**
	 * We don't really care about the number of rows to load in memory in the example, as we are just inserting data
	 */
	@Override
	public int getLimitOfRowsLoadedInMemory() {
		return 100;
	}

	public String getUrl() {
		return url;
	}
}
