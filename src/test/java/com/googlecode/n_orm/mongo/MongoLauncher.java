package com.googlecode.n_orm.mongo;

import java.util.Map;
import java.util.TreeMap;

import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.StoreTestLauncher;

public class MongoLauncher extends StoreTestLauncher {

	@Override
	public Map<String, Object> prepare(Class<?> testClass) {

		Map<String, Object> p = new TreeMap<String, Object>();

		p.put(
			StoreSelector.STORE_DRIVERCLASS_PROPERTY,
			com.googlecode.n_orm.mongo.MongoStore.class.getName()
		);

		p.put(StoreSelector.STORE_DRIVERCLASS_STATIC_ACCESSOR, "getStore");

		return p;
	}

}

