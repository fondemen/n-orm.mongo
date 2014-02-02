package com.googlecode.n_orm.mongo;

import java.util.Map;
import java.util.HashMap;

import com.googlecode.n_orm.storeapi.Row;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;

import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;


class MongoRow implements Row {

	static final String ROW_ENTRY_NAME = "rowname";
	static final String FAM_ENTRIES_NAME = "families";

	private final String key;
	private final ColumnFamilyData values;

	public MongoRow(DBObject llRow) {
		key = MongoNameSanitizer.dirty((String) llRow.get(ROW_ENTRY_NAME));
		values = new DefaultColumnFamilyData();

		DBObject families = (DBObject) llRow.get(FAM_ENTRIES_NAME);

		if (families != null) {
			for (String family : families.keySet()) {
				Map map = new HashMap();
				DBObject columns = (DBObject) families.get(family);

				for (String k : columns.keySet()) {
					map.put(
						MongoNameSanitizer.dirty(k),
						(byte[])(columns.get(k))
					);
				}

				values.put(
					MongoNameSanitizer.dirty(family),
					map
				);
			}
		}
	}

	@Override
	public String getKey() {
		return key;
	}

	@Override
	public ColumnFamilyData getValues() {
		return values;
	}
}
