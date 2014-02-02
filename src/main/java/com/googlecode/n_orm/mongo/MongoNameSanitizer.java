package com.googlecode.n_orm.mongo;

/**
 * Created by benoit on 1/25/14.
 */
public class MongoNameSanitizer {

	public static String sanitize(String name) {
		String sanitizedName = name;
		if (name != null) {
			sanitizedName = sanitizedName.replace("$", "_Dollar_");
			sanitizedName = sanitizedName.replace(".", "_Dot_");
		}
		return sanitizedName;
	}

	public static String dirty(String name) {
		String dirtiedName = name;
		if (name != null) {
			dirtiedName = dirtiedName.replace("_Dollar_", "$");
			dirtiedName = dirtiedName.replace("_Dot_", ".");
		}
		return dirtiedName;
	}
}

