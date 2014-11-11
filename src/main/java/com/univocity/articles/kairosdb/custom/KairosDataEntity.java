/*******************************************************************************
 * Copyright (c) 2014 uniVocity Software Pty Ltd. All rights reserved.
 * This file is subject to the terms and conditions defined in file
 * 'LICENSE.txt', which is part of this source code package.
 ******************************************************************************/
package com.univocity.articles.kairosdb.custom;

import java.util.*;

import org.apache.commons.lang.*;
import org.kairosdb.client.builder.*;

import com.univocity.api.entity.*;
import com.univocity.api.entity.custom.*;

/**
 * A custom entity for KairosDB that implements methods for data modification.
 *
 * @author uniVocity Software Pty Ltd - <a href="mailto:dev@univocity.com">dev@univocity.com</a>
 *
 */
class KairosDataEntity implements CustomDataEntity {

	private final KairosDataStore dataStore;
	private final String entityName;
	private final Set<DefaultEntityField> fields = new HashSet<DefaultEntityField>();
	private final Set<String> tags = new HashSet<String>();

	/**
	 * Creates a new instance of a custom data entity, with a given set of field names
	 *
	 * @param dataStore the data store that contains this entity (we need it to manage transactions)
	 * @param entityName the name of the new custom data entity
	 * @param fieldNames the fields in this entity.
	 */
	public KairosDataEntity(KairosDataStore dataStore, String entityName, String... tagNames) {
		this.dataStore = dataStore;
		this.entityName = entityName;

		addFields("name", "timestamp", "value");
		addFields(tagNames);

		tags.addAll(Arrays.asList(tagNames));
	}

	public String getEntityName() {
		return entityName;
	}

	public ReadingProcess preareToRead(String[] fieldNames) {
		return null;
	}

	public Set<? extends DefaultEntityField> getFields() {
		return fields;
	}

	private void addFields(String... names) {
		for (String name : names) {
			if(StringUtils.isNotBlank(name)){
				fields.add(new DefaultEntityField(name));
			}
		}
	}

	public WritingProcess prepareToWrite(String[] fieldNames) {

		final Map<String, Integer> fieldPositions = new HashMap<String, Integer>();
		for (int i = 0; i < fieldNames.length; i++) {
			fieldPositions.put(fieldNames[i], i);
		}

		return new WritingProcess() {

			MetricBuilder builder = MetricBuilder.getInstance();

			public void close() {
				// will push the metrics at the end of the batch instead of at the end of the transaction.
				// otherwise the generated JSON content can be excessively big.
				dataStore.pushMetrics(KairosDataEntity.this, builder);
			}

			public void writeNext(Object[] data) {
				String name = get("name", String.class, data);
				Metric metric = builder.addMetric(name);

				Long timestamp = get("timestamp", Long.class, data);
				Object value = get("value", Object.class, data);

				if (timestamp == null) {
					metric.addDataPoint(System.currentTimeMillis(), value);
				} else {
					metric.addDataPoint(timestamp, value);
				}

				for (String tag : tags) {
					String tagValue = get(tag, String.class, data);
					metric.addTag(tag, tagValue);
				}
			}

			private <T> T get(String field, Class<T> type, Object[] data) {
				Integer position = fieldPositions.get(field); //should never be null
				Object value = data[position.intValue()];
				return type.cast(value);
			}

			public ReadingProcess retrieveGeneratedKeys() {
				return null;
			}
		};
	}

	public UpdateProcess prepareToUpdate(String[] fieldsToUpdate, String[] fieldsToMatch) {
		return null;
	}

	public ExclusionProcess prepareToDelete(String[] fieldsToMatch) {
		return null;
	}

	public void deleteAll() {
	}
}
