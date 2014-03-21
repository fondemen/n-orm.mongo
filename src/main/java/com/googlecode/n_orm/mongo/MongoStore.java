package com.googlecode.n_orm.mongo;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.GenericStore;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.Store;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;


public class MongoStore implements Store, GenericStore
{
	private static String DB_NAME    = "n_orm";
	private static short  MONGO_PORT =  27017;
	private static String MONGO_HOST = "localhost";
	
	private static final Set<Class<?>> SIMPLE_TYPES;

	private static String hostname;

	private static Map<Properties, MongoStore> knownStores = new ConcurrentHashMap<Properties, MongoStore>();
	
	static {
		/* FIXME: getHostAddress returns wrong IP address...
		try {
			hostname = InetAddress.getLocalHost().getHostAddress();
		} catch (Exception e) {
			Mongo.mongoLog.log(
				Level.WARNING,
				"Could not get local addr. \"localhost\" will be used."
			);
			hostname = MONGO_HOST;
		}
		*/
		
		Set<Class<?>> simpleClasses = new HashSet<Class<?>>();
		simpleClasses.add(Boolean.class);
		simpleClasses.add(String.class);
		simpleClasses.add(long.class);
		simpleClasses.add(Long.class);
		simpleClasses.add(int.class);
		simpleClasses.add(Integer.class);
		simpleClasses.add(short.class);
		simpleClasses.add(Short.class);
		simpleClasses.add(byte.class);
		simpleClasses.add(Byte.class);
//		simpleClasses.add(double.class);
//		simpleClasses.add(Double.class);
//		simpleClasses.add(float.class);
//		simpleClasses.add(Float.class);
		simpleClasses.add(Date.class);
		
		SIMPLE_TYPES = Collections.unmodifiableSet(simpleClasses);
	}
	
	private static class MSQuery {
		DBObject query;
		DBObject rowObj;
	}

	private DB mongoDB;
	private MongoClient mongoClient;

	private int    port = MONGO_PORT;
	private String host = hostname;
	private String db   = DB_NAME;

	private volatile boolean started = false;

	private Map<String, Boolean> hasTableCache = new HashMap<String, Boolean>();

	public static MongoStore getStore() {
		Properties p = new Properties();
		p.put("address", MONGO_HOST);
		p.put("port", MONGO_PORT);
		return getStore(p);
	}

	public static MongoStore getStore(String addr) {
		Properties p = new Properties();
		p.put("address", addr);
		p.put("port", MONGO_PORT);
		return getStore(p);
	}

	public static MongoStore getStore(String addr, short port) {
		Properties p = new Properties();
		p.put("address", addr);
		p.put("port", port);
		return getStore(p);
	}

	public static MongoStore getStore(Properties p) {
		synchronized (MongoStore.class) {
			MongoStore store = knownStores.get(p);

			if (store == null) {
				store = new MongoStore((String) p.get("address"), (Short) p.get("port"));
			}

			return store;
		}
	}

	public MongoStore() {
		this(MONGO_HOST, MONGO_PORT);
	}

	public MongoStore(String host) {
		this(host, MONGO_PORT);
	}

	public MongoStore(String host, short port) {
		Properties p = new Properties();
		p.put("address", host);
		p.put("port", port);

		setHost(host);
		setPort(port);

		knownStores.put(p, this);
	}

	public synchronized void start()
		throws DatabaseNotReachedException
	{
        if (started) return;

        try {
            Mongo.mongoLog.log(
                Level.FINE,
                "Trying to connect to the mongo database "+host+":"+port
            );
            mongoClient = new MongoClient(host, port);

        } catch(Exception e) {
            Mongo.mongoLog.log(
                Level.SEVERE,
                "Could not find "+host+":"+port
            );
            throw new DatabaseNotReachedException(e);
        }

        try {
            mongoDB = mongoClient.getDB(db);
            // Mongo won't complain that it couldn't connect to the database
            // until the first access attempt. Force access to the DB.
            mongoDB.getCollectionNames();

        } catch(Exception e) {
            Mongo.mongoLog.log(
                Level.SEVERE,
                "Could not find a running database on "+host+":"+port
            );
            throw new DatabaseNotReachedException(e);
        }

        started = true;
	}

    protected String sanitizeTableName(String name) {
        return name.replace('$', '_');
    }

    protected String sanitizeName(String name) {
        return MongoNameSanitizer.sanitize(name);
    }

	protected String dirtyName(String name) {
		return MongoNameSanitizer.dirty(name);
	}

	public boolean hasTable(String tableName)
		throws DatabaseNotReachedException
	{
		Boolean ret;

        String sanitizedTableName = sanitizeTableName(tableName);

		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

		try {
			ret = hasTableCache.get(sanitizedTableName);
			if (ret == null) {
				ret = mongoDB.collectionExists(sanitizedTableName);
				hasTableCache.put(sanitizedTableName, ret);
				
			}
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}

		return ret.booleanValue();
	}


	protected DBObject findLimitedRow(String table, String row)
		throws DatabaseNotReachedException
	{
		return findLimitedRow(table, row, new TreeSet<String>(), null);
	}


	protected DBObject findLimitedRow(String table, String row, Set<String> families)
		throws DatabaseNotReachedException
	{
		return findLimitedRow(table, row, families, null);
	}


	protected DBObject findLimitedRow(String table, String row, String family)
		throws DatabaseNotReachedException
	{
		Set<String> families = new TreeSet<String>();
		families.add(family);
		return findLimitedRow(table, row, families);
	}


	protected DBObject findLimitedRow(String table, String row, String family, String column)
		throws DatabaseNotReachedException
	{
		Set<String> families = new TreeSet<String>();
		families.add(family);
		return findLimitedRow(table, row, families, column);
	}

	
	protected DBObject findLimitedRow(
		String table, String row, Set<String> families, String column
	) throws DatabaseNotReachedException
	{
		if (!hasTable(table)) {
			return null;
		}

		assert families != null && !families.isEmpty();
		assert column != null ? families.size() == 1 : true;
		
		DBObject limitedRow;
		DBObject query = new BasicDBObject();
		DBObject keys = new BasicDBObject();

		query.put(MongoRow.ROW_ENTRY_NAME, row);
		
		for (String family : families) {
			if (families != null) {
				if (column != null) {
					keys.put(family + "." + column, 1);
				} else {
					keys.put(family, 1);
				}
			}
		}
		
		keys.put(MongoRow.ROW_ENTRY_NAME, 1);

		try {
			limitedRow = mongoDB.getCollection(table).findOne(query, keys);
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}

		return limitedRow;
	}


	protected DBObject getColumns(DBObject families, String familyName)
		throws DatabaseNotReachedException
	{
		DBObject cols = (DBObject) families.get(familyName);
		return (cols != null) ? cols : new BasicDBObject();
	}

	
	public void delete(MetaInformation meta, String table, String id)
		throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(id);

		try {
			mongoDB.getCollection(sanitizedTableName).remove(
				new BasicDBObject(MongoRow.ROW_ENTRY_NAME, sanitizedRowName)
			);
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}
	}
	
	private MSQuery createQuery(String sanitizedTableName, String sanitizedRowName) {
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}
		
		DBObject query = new BasicDBObject(MongoRow.ROW_ENTRY_NAME, sanitizedRowName);
		DBObject rowObj = new BasicDBObject();
		rowObj.put("$set", new BasicDBObject(MongoRow.ROW_ENTRY_EXISTS, true)); // ensure the row exists
		
		MSQuery q = new MSQuery();
		q.query = query;
		q.rowObj = rowObj;
		
		return q;
	}
	
	private void runQuery(MSQuery q, String sanitizedTableName) {
		try {
			DBCollection col = mongoDB.getCollection(sanitizedTableName);
			if (hasTableCache.put(sanitizedTableName, true) == null && !"_id".equals(MongoRow.ROW_ENTRY_NAME)) {
				DBObject idx = new BasicDBObject();
				idx.put(MongoRow.ROW_ENTRY_NAME, 1);
				DBObject idxOpts = new BasicDBObject();
				idxOpts.put("unique", true);
				col.ensureIndex(idx, idxOpts);
			}
			col.update(q.query, q.rowObj, true, false);
		} catch (MongoException e) {
			throw new DatabaseNotReachedException(e);
		}
	}

	public void insert(
			MetaInformation meta, String table, String row, ColumnFamilyData data
	) throws DatabaseNotReachedException {
        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(row);
        MSQuery q = createQuery(sanitizedTableName, sanitizedRowName);
		enrichQueryWithInsert(meta, q, sanitizedTableName, sanitizedRowName, data);
		runQuery(q, sanitizedTableName);
	}

	public void enrichQueryWithInsert(
			MetaInformation meta, MSQuery q, String sanitizedTableName, String sanitizedRowName, ColumnFamilyData data
	) throws DatabaseNotReachedException
	{
		if (data == null || data.isEmpty()) {
			return;
		}
		
		DBObject sets = (DBObject) q.rowObj.get("$set");
		if (sets == null) sets = new BasicDBObject();

		for (Map.Entry<String, Map<String, byte[]>> family : data.entrySet()) {
			String sanitizedFamilyName = sanitizeName(family.getKey());
			Map<String, byte[]> columns = family.getValue();
			
			Field f = meta == null ? null : meta.getFamilies().get(family.getKey());
			Class<?> clazz;
			boolean isCf = f != null;
			if (isCf) {
				ColumnFamily<?> cf = meta.getElement().getColumnFamily(f.getName());
				if (cf != null) {
					clazz = cf.getClazz();
				} else {
					clazz = null;
				}
			} else {
				clazz = null;
			}

			for (Map.Entry<String, byte[]> col : columns.entrySet()) {
				String sanitizedColumnName = sanitizeName(col.getKey());
				
				if (!isCf) {
					// Class for simple properties should be found property by property
					f = meta == null ? null : meta.getFamilies().get(col.getKey());
					clazz = f == null ? null : f.getType();
				}
				
				Object value;
				if (clazz == null) {
					value = col.getValue();
				} else if ( PersistingElement.class.isAssignableFrom(clazz)) {
					value = ConversionTools.convert(String.class, col.getValue());
				} else if (SIMPLE_TYPES.contains(clazz)) {
					value = ConversionTools.convert(clazz, col.getValue());
				} else {
					value = col.getValue();
				}
				
				sets.put(
					sanitizedFamilyName + "." + sanitizedColumnName,
					value
				);
			}

		}

		q.rowObj.put("$set", sets);
		
	}

	public void increment(String table, String row, Map<String, Map<String, Number>> increments) {
        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(row);
        MSQuery q = createQuery(sanitizedTableName, sanitizedRowName);
        enrichQueryWithIncrement(q, sanitizedTableName, sanitizedRowName, increments);
		runQuery(q, sanitizedTableName);
	}

	public void enrichQueryWithIncrement(MSQuery q, String sanitizedTableName, String sanitizedRowName, Map<String, Map<String, Number>> increments) {
		if (increments == null || increments.isEmpty()) {
			return;
		}
		
		DBObject incs = (DBObject) q.rowObj.get("$inc");
		if (incs == null) incs = new BasicDBObject();

		for (Map.Entry<String, Map<String, Number>> family : increments.entrySet()) {
			String sanitizedFamilyName = sanitizeName(family.getKey());
			Map<String, Number> columns = family.getValue();

			for (Map.Entry<String, Number> col : columns.entrySet()) {
				String sanitizedColumnName = sanitizeName(col.getKey());
				incs.put(
					sanitizedFamilyName + "." + sanitizedColumnName,
					col.getValue()
				);
			}

		}

		q.rowObj.put("$inc", incs);
	}
	
	public void remove(String table, String row, Map<String, Set<String>> removed) {
        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(row);
        MSQuery q = createQuery(sanitizedTableName, sanitizedRowName);
        enrichQueryWithRemove(q, sanitizedTableName, sanitizedRowName, removed);
		runQuery(q, sanitizedTableName);
	}

	public void enrichQueryWithRemove(MSQuery q, String sanitizedTableName, String sanitizedRowName, Map<String, Set<String>> removed) {

		if (removed == null || removed.isEmpty())
			return;
		
		DBObject unsets = new BasicDBObject();

		for (Map.Entry<String, Set<String>> family : removed.entrySet()) {
			String sanitizedFamilyName = sanitizeName(family.getKey());
			Set<String> columns = family.getValue();

			for (String col: columns) {
				String sanitizedColumnName = sanitizeName(col);
				unsets.put(
					sanitizedFamilyName + "." + sanitizedColumnName,
					""
				);
			}
		}
		
		q.rowObj.put("$unset", unsets);
	}
	

	public boolean exists(MetaInformation meta, String table, String row)
		throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(row);

		DBObject found = null;
		
		try {
			DBObject query = new BasicDBObject();
			DBObject keys = new BasicDBObject();

			query.put(MongoRow.ROW_ENTRY_NAME, sanitizedRowName);
			
			keys.put(MongoRow.ROW_ENTRY_NAME, 1);
			
			found = mongoDB.getCollection(sanitizedTableName).findOne(query, keys);
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}

		return found != null;
	}


	public boolean exists(
		MetaInformation meta, String table, String row, String family
	) throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(row);
		String sanitizedFamilyName = sanitizeName(family);
		
		DBObject found = null;
		DBObject query = new BasicDBObject();
		DBObject keys = new BasicDBObject();

		query.put(MongoRow.ROW_ENTRY_NAME, sanitizedRowName);
		query.put(sanitizedFamilyName, new BasicDBObject("$type", 3));
		query.put("$where", "Object.keys(this." + sanitizedFamilyName + ").length > 0");
		
		keys.put(MongoRow.ROW_ENTRY_NAME, 1);

		try {
			found = mongoDB.getCollection(sanitizedTableName).findOne(query, keys);
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}
		
		return found != null;
	}


	public CloseableKeyIterator get(
		MetaInformation meta, String table,
		Constraint c, int limit, Set<String> families
	) throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

        String sanitizedTableName = sanitizeTableName(table);

		DBObject query = null;
		if (c != null) {
			DBObject startCond = c.getStartKey() == null ? null : new BasicDBObject(MongoRow.ROW_ENTRY_NAME,
					new BasicDBObject("$gte", sanitizeName(c.getStartKey())));
			DBObject endCond = c.getEndKey() == null ? null : new BasicDBObject(MongoRow.ROW_ENTRY_NAME,
					new BasicDBObject("$lte", sanitizeName(c.getEndKey())));
			
			if (startCond != null) {
				if (endCond != null) {
					query = new BasicDBObject("$and", new Object[] {startCond, endCond});
				} else {
					query = startCond;
				}
			} else if (endCond != null) {
				query = endCond;
			}
		}

		DBObject keys = new BasicDBObject();
		if (families != null) {
			for (String family : families) {
				String sanitizedFamilyName = sanitizeName(family);
				keys.put(sanitizedFamilyName, 1);
			}
		}
		keys.put(MongoRow.ROW_ENTRY_NAME, 1);

		BasicDBObject mastoQuery = new BasicDBObject();
		mastoQuery.put("$query",   query == null ? new BasicDBObject() : query);
		mastoQuery.put("$limit",   Integer.toString(limit));
		mastoQuery.put("$orderby", new BasicDBObject(MongoRow.ROW_ENTRY_NAME, 1));

		DBCursor cur;
		try {
			cur = mongoDB.getCollection(sanitizedTableName).find(mastoQuery, keys);
		} catch (MongoException e) {
			throw new DatabaseNotReachedException(e);
		}

		return new CloseableIterator(cur);
	}


	public byte[] get(
			MetaInformation meta, String table, String row,
			String family, String key
	) throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(row);
		String sanitizedFamilyName = sanitizeName(family);
		String sanitizedKeyName = sanitizeName(key);

		byte[] ret = null;
		DBObject limitedRow, columns;

		try {
			limitedRow = findLimitedRow(
				sanitizedTableName,
				sanitizedRowName,
				sanitizedFamilyName,
				sanitizedKeyName
			);

			columns = getColumns(
				limitedRow,
				sanitizedFamilyName
			);

			if (columns.containsField(sanitizedKeyName)) {
				ret = ConversionTools.convert(columns.get(sanitizedKeyName));
			}

		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}

		return ret;
	}


	public Map<String, byte[]> get(
		MetaInformation meta, String table, String id, String family
	) throws DatabaseNotReachedException
	{
		Map<String, byte[]> map = new HashMap<String, byte[]>();

		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(id);
		String sanitizedFamilyName = sanitizeName(family);
		
		DBObject limitedRow, columns;

		try {
			limitedRow = findLimitedRow(sanitizedTableName, sanitizedRowName, sanitizedFamilyName);

			columns = getColumns(
				limitedRow,
				sanitizedFamilyName
			);

			for (String key : columns.keySet()) {
				map.put(
					dirtyName(key),
					ConversionTools.convert(columns.get(key))
				);
			}

		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}

		return map;
	}


	public Map<String, byte[]> get(
		MetaInformation meta, String table,
		String id, String family, Constraint c
	) throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(id);
		String sanitizedFamilyName = sanitizeName(family);

		DBObject limitedRow, columns;
		Map<String, byte[]> map = new HashMap<String, byte[]>();

		try {
			limitedRow = findLimitedRow(sanitizedTableName, sanitizedRowName, sanitizedFamilyName);

			columns = getColumns(
				limitedRow,
				sanitizedFamilyName
			);

			if (c != null) {
				for (String key : columns.keySet()) {
					String originalKey = dirtyName(key);
					boolean ok1 = (c.getStartKey() != null)
						? originalKey.compareTo(c.getStartKey()) >= 0
						: true;
					boolean ok2 = (c.getEndKey() != null)
						? originalKey.compareTo(c.getEndKey()) <= 0
						: true;
					if (ok1 && ok2) {
						map.put(
							originalKey,
							ConversionTools.convert(columns.get(key))
						);
					}
				}
			}
			
			else {
				for (String key : columns.keySet()) {
					map.put(
						dirtyName(key),
						ConversionTools.convert(columns.get(key))
					);
				}
			}

		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}

		return map;
	}


	public ColumnFamilyData get(
		MetaInformation meta, String table, String id, Set<String> families
	) throws DatabaseNotReachedException
	{
		TreeMap<String, Map<String, byte[]>> mapOfMaps
			= new TreeMap<String, Map<String, byte[]>>();

		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(id);
		Set<String> sanitizedFamiliesNames = new TreeSet<String>();
		for (String family : families) {
			sanitizedFamiliesNames.add(this.sanitizeName(family));
		}

		DBObject limitedRow, columns;

		try {
			limitedRow = findLimitedRow(sanitizedTableName, sanitizedRowName, sanitizedFamiliesNames);

			for (String family : families) {
				Map<String, byte[]> map = new HashMap<String, byte[]>();
				String sanitizedFamilyName = sanitizeName(family);

				columns = getColumns(limitedRow, sanitizedFamilyName);
				for (String key : columns.keySet()) {
					map.put(
						dirtyName(key),
						ConversionTools.convert(columns.get(key))
					);
				}

				mapOfMaps.put(family, map);
			}

		} catch (Exception e) {
			return null;
			//throw new DatabaseNotReachedException(e);
		}

		return new DefaultColumnFamilyData(mapOfMaps);
	}


	public void storeChanges(
		MetaInformation meta, String table, String id,
		ColumnFamilyData changed,
		Map<String, Set<String>> removed,
		Map<String, Map<String, Number>> increments
	) throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(id);
        MSQuery q = createQuery(sanitizedTableName, sanitizedRowName);

		enrichQueryWithInsert(meta, q, sanitizedTableName, sanitizedRowName, changed);
		enrichQueryWithIncrement(q, sanitizedTableName, sanitizedRowName, increments);
		enrichQueryWithRemove(q, sanitizedTableName, sanitizedRowName, removed);

		runQuery(q, sanitizedTableName);

	}


	public long count(MetaInformation meta, String table, Constraint c)
		throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

        String sanitizedTableName = sanitizeTableName(table);

		long cnt = 0;

		DBObject query = null;
		if (c != null) {
			DBObject startCond = c.getStartKey() == null ? null : new BasicDBObject(MongoRow.ROW_ENTRY_NAME,
					new BasicDBObject("$gte", sanitizeName(c.getStartKey())));
			DBObject endCond = c.getEndKey() == null ? null : new BasicDBObject(MongoRow.ROW_ENTRY_NAME,
					new BasicDBObject("$lte", sanitizeName(c.getEndKey())));
			
			if (startCond != null) {
				if (endCond != null) {
					query = new BasicDBObject("$and", new Object[] {startCond, endCond});
				} else {
					query = startCond;
				}
			} else if (endCond != null) {
				query = endCond;
			}
		}

		try {
			cnt = mongoDB.getCollection(sanitizedTableName).count(query == null ? new BasicDBObject() : query);
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}

		return cnt;
	}

	public void dropTable(String table)
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

        String sanitizedTableName = sanitizeTableName(table);

		try {
			mongoDB.getCollection(sanitizedTableName).drop();
			hasTableCache.put(sanitizedTableName, false);
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}
	}

	public void setHost(String host)
	{
		if (started) return;
		this.host = host;
	}

	public void setPort(int port)
	{
		if (started) return;
		this.port = port;
	}
	
	public void setDb(String dbname) {
		this.setDB(db);
	}

	public void setDB(String dbname)
	{
		this.db = dbname;

		if (started) {
			mongoDB = mongoClient.getDB(db);
		}
	}

	public synchronized void close()
		throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

		// TODO: save changes before closing

		// close connections
		mongoClient.close();

		// reset default values
		started = false;
		db      = DB_NAME;
		port    = MONGO_PORT;
		host    = hostname;
	}
}
