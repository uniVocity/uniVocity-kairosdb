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
	 * @param dataStore the data store that contains this entity (we need it to manage open connections)
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

	@Override
	public String getEntityName() {
		return entityName;
	}

	@Override
	public ReadingProcess preareToRead(String[] fieldNames) {
		return null;
	}

	@Override
	public Set<? extends DefaultEntityField> getFields() {
		return fields;
	}

	private void addFields(String... names) {
		for (String name : names) {
			if (StringUtils.isNotBlank(name)) {
				fields.add(new DefaultEntityField(name));
			}
		}
	}

	@Override
	public WritingProcess prepareToWrite(String[] fieldNames) {

		final Map<String, Integer> fieldPositions = new HashMap<String, Integer>();
		for (int i = 0; i < fieldNames.length; i++) {
			fieldPositions.put(fieldNames[i], i);
		}

		return new WritingProcess() {

			MetricBuilder builder = MetricBuilder.getInstance();

			@Override
			public void close() {
				// will push the metrics at the end of each batch instead of at the end of the transaction.
				// otherwise the generated JSON content can be excessively big.
				dataStore.pushMetrics(KairosDataEntity.this, builder);
			}

			@Override
			public void writeNext(Object[] data) {
				String name = get("name", String.class, data, true);
				Metric metric = builder.addMetric(name);

				Long timestamp = get("timestamp", Long.class, data, false);
				Object value = get("value", Object.class, data, true);

				if (timestamp == null) {
					metric.addDataPoint(System.currentTimeMillis(), value);
				} else {
					metric.addDataPoint(timestamp, value);
				}

				for (String tag : tags) {
					String tagValue = get(tag, String.class, data, false);
					metric.addTag(tag, tagValue);
				}
			}

			private <T> T get(String field, Class<T> type, Object[] data, boolean mandatory) {
				Integer position = fieldPositions.get(field);
				if (position == null) {
					if (mandatory) {
						throw new IllegalStateException("Mandatory field '" + field + "' not mapped.");
					} else {
						return null;
					}
				}
				Object value = data[position.intValue()];
				return type.cast(value);
			}

			@Override
			public ReadingProcess retrieveGeneratedKeys() {
				return null;
			}
		};
	}

	@Override
	public UpdateProcess prepareToUpdate(String[] fieldsToUpdate, String[] fieldsToMatch) {
		return null;
	}

	@Override
	public ExclusionProcess prepareToDelete(String[] fieldsToMatch) {
		return null;
	}

	@Override
	public void deleteAll() {
	}
}
