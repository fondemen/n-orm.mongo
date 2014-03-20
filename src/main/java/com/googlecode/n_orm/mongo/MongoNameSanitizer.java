package com.googlecode.n_orm.mongo;

/**
 * Created by benoit on 1/25/14.
 */
public class MongoNameSanitizer {

	public static String sanitize(String name) {
		String sanitizedName = name;
		if (name != null) {
			sanitizedName = sanitizedName.replace('$', '\uFF04');
			sanitizedName = sanitizedName.replace('.', '\uFF0E');
		}
		return sanitizedName;
	}

	public static String dirty(String name) {
		String dirtiedName = name;
		if (name != null) {
			dirtiedName = dirtiedName.replace('\uFF04', '$');
			dirtiedName = dirtiedName.replace('\uFF0E', '.');
		}
		return dirtiedName;
	}
}

