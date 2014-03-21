package com.googlecode.n_orm.mongo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.Row;
import com.mongodb.DBObject;


class MongoRow implements Row {

	static final String ROW_ENTRY_ID = "_id";
	static final String ROW_ENTRY_NAME = "rowname";
	
	static final Set<String> NON_FAMILIES_ROWS = new TreeSet<String>(Arrays.asList(ROW_ENTRY_ID, ROW_ENTRY_NAME));
	
	static boolean isSpecialProperty(String family) {
		return NON_FAMILIES_ROWS.contains(family);
	}

	private final String key;
	private final ColumnFamilyData values;

	public MongoRow(DBObject llRow) {
		
		key = MongoNameSanitizer.dirty((String) llRow.get(ROW_ENTRY_NAME));
		values = new DefaultColumnFamilyData();

		for (String family : llRow.keySet()) {
			if (isSpecialProperty(family))
				continue;
			
			Map<String, byte[]> map = new HashMap<String, byte[]>();
			DBObject columns = (DBObject) llRow.get(family);

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
