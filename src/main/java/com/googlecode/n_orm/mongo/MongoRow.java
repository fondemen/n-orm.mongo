package com.googlecode.n_orm.mongo;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;

import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.storeapi.Row;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


class MongoRow implements Row {

	static final String ROW_ENTRY_NAME = "rowname";
	static final String FAM_ENTRIES_NAME = "families";
	static final String INC_ENTRIES_NAME = "families_inc";

	private final String key;
	private final ColumnFamilyData values;

	public MongoRow(DBObject llRow) {
		key = MongoNameSanitizer.dirty((String) llRow.get(ROW_ENTRY_NAME));
		values = new DefaultColumnFamilyData();

		DBObject families     = (DBObject) llRow.get(FAM_ENTRIES_NAME);
		DBObject inc_families = (DBObject) llRow.get(INC_ENTRIES_NAME);

		if (families == null) {
			families = new BasicDBObject();
		}

		if (inc_families == null) {
			inc_families = new BasicDBObject();
		}

		for (String family : families.keySet()) {
			Map<String, byte[]> map = new HashMap<String, byte[]>();
			DBObject columns = (DBObject) families.get(family);

			for (String k : columns.keySet()) {
				map.put(
					MongoNameSanitizer.dirty(k),
					ConversionTools.convert(columns.get(k))
				);
			}

			values.put(
				MongoNameSanitizer.dirty(family),
				map
			);
		}

		for (String family: inc_families.keySet()) {
			Map<String, byte[]> map = new HashMap<String, byte[]>();
			DBObject columns = (DBObject) inc_families.get(family);

			for (String k : columns.keySet()) {
				Number n = (Number) columns.get(k);
				map.put(
					MongoNameSanitizer.dirty(k),
					ByteBuffer.allocate(4).putInt(n.intValue()).array()
				);
			}

			values.put(
				MongoNameSanitizer.dirty(family),
				map
			);
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
