package com.googlecode.n_orm.mongo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.Row;


class MongoRow implements Row {

	static final String ROW_ENTRY_NAME = "_id";
	static final String ROW_ENTRY_EXISTS = "x";
	
	static final Set<String> NON_FAMILIES_ROWS = new TreeSet<String>(Arrays.asList(ROW_ENTRY_NAME, ROW_ENTRY_EXISTS));
	
	static boolean isSpecialProperty(String family) {
		return NON_FAMILIES_ROWS.contains(family);
	}

	private final String key;
	private final ColumnFamilyData values;

	public MongoRow(Document llRow) {
		
		Object id = llRow.get(ROW_ENTRY_NAME);
		if (id instanceof String) {
			key = MongoNameSanitizer.dirty((String)id);
		} else if (id instanceof ObjectId) {
			key = ((ObjectId)id).toHexString() + KeyManagement.KEY_END_SEPARATOR;
		} else {
			throw new IllegalStateException("Row " + llRow + " has unexpected id " + id);
		}
		
		values = new DefaultColumnFamilyData();

		for (String family : llRow.keySet()) {
			if (isSpecialProperty(family))
				continue;
			
			Map<String, byte[]> map = new HashMap<String, byte[]>();
			Document columns = (Document) llRow.get(family);

			for (Entry<String, Object> kv : columns.entrySet()) {
				Object val = kv.getValue();
				if (val instanceof Binary) val = ((Binary)val).getData();
				map.put(
					MongoNameSanitizer.dirty(kv.getKey()),
					ConversionTools.convert(val)
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
