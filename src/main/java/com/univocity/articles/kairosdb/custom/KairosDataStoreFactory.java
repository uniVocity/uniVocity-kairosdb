/*******************************************************************************
 * Copyright (c) 2014 uniVocity Software Pty Ltd. All rights reserved.
 * This file is subject to the terms and conditions defined in file
 * 'LICENSE.txt', which is part of this source code package.
 ******************************************************************************/
package com.univocity.articles.kairosdb.custom;

import com.univocity.api.entity.custom.*;

/**
 * This is a custom data store factory that "knows" how to instantiate and configure our custom data store {@link KairosDataStore}
 *
 * Use {@link EngineConfiguration#addCustomDataStoreFactories(CustomDataStoreFactory...)} to define your custom data store factories to be used
 * by a {@link DataIntegrationEngine}.
 *
 * @author uniVocity Software Pty Ltd - <a href="mailto:dev@univocity.com">dev@univocity.com</a>
 *
 */
public class KairosDataStoreFactory implements CustomDataStoreFactory<KairosDataStoreConfiguration> {

	@Override
	public CustomDataStore<?> newDataStore(KairosDataStoreConfiguration configuration) {
		return new KairosDataStore(configuration);
	}

	@Override
	public Class<KairosDataStoreConfiguration> getConfigurationType() {
		return KairosDataStoreConfiguration.class;
	}
}
