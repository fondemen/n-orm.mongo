package com.googlecode.n_orm.mongo;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

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
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;

public class MongoStore implements Store, GenericStore
{
	private static String DB_NAME    = "n_orm";
	private static short  MONGO_PORT =  27017;
	private static String MONGO_HOST = "localhost";
	
	private static final Set<Class<?>> SIMPLE_TYPES, REAL_TYPES;

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
		simpleClasses.add(boolean.class);
		simpleClasses.add(String.class);
		simpleClasses.add(long.class);
		simpleClasses.add(Long.class);
		simpleClasses.add(int.class);
		simpleClasses.add(Integer.class);
		simpleClasses.add(short.class);
		simpleClasses.add(Short.class);
		simpleClasses.add(byte.class);
		simpleClasses.add(Byte.class);
		simpleClasses.add(Date.class);
		
		SIMPLE_TYPES = Collections.unmodifiableSet(simpleClasses);

		Set<Class<?>> realClasses = new HashSet<Class<?>>();
		realClasses.add(double.class);
		realClasses.add(Double.class);
		realClasses.add(float.class);
		realClasses.add(Float.class);
		
		REAL_TYPES = Collections.unmodifiableSet(realClasses);
	}
	
	private static class MSQuery {
		Document query;
		Document rowObj;
	}

	private MongoDatabase mongoDB;
	private MongoClient mongoClient;

	private int    port = MONGO_PORT;
	private String host = hostname;
	private String db   = DB_NAME;
	private String user,pwd, authDB = "admin";
	
	private long existingTableCacheTTL = TimeUnit.MINUTES.toMillis(10);
	private long inexistingTableCacheTTL = TimeUnit.SECONDS.toMillis(3);

	private volatile boolean started = false;

	private class TableAvailablility {
		private final boolean exists;
		private final long expired;
		
		public TableAvailablility(boolean exists) {
			this.exists = exists;
			this.expired = System.currentTimeMillis() + (exists ? existingTableCacheTTL : inexistingTableCacheTTL);
		}
		
		public boolean isExpired() {
			return System.currentTimeMillis() > this.expired;
		}
	}
	private Map<String, TableAvailablility> hasTableCache = new HashMap<String, TableAvailablility>();

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
			System.out.println("finding store with " + p.toString());
			MongoStore store = knownStores.get(p);

			if (store == null) {
				store = new MongoStore((String) p.get("address"), (Short) p.get("port"));
				if (p.containsKey("user")) {
					store.user = p.getProperty("user");
					store.pwd = p.containsKey("password") ? p.getProperty("password") : "";
				}
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

	/**
	 * The port for the MongoDB connection.
	 */
	public int getPort() {
		return port;
	}

	/**
	 * The host name or ip for the MongoDB connection.
	 */
	public String getHost() {
		return host;
	}

	/**
	 * The name of the database used by this driver.
	 */
	public String getDb() {
		return db;
	}

	/**
	 * The MongoDB connection.
	 * Null if not started yet.
	 */
	public MongoClient getMongoClient() {
		return mongoClient;
	}

	/**
	 * The database used by this driver.
	 * Null if not started yet.
	 */
	public MongoDatabase getMongoDB() {
		return mongoDB;
	}

	public String getUser() {
		return this.user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPassword(String password) {
		this.pwd = password;
	}

	public String getAuthenticationDatabase() {
		return this.authDB;
	}

	public void setAuthenticationDatabase(String authDB) {
		this.authDB = authDB;
	}

	/**
	 * Minimum time during which a table known to exist will not be tested again for existence ; default value is 10 minutes.
	 */
	public long getExistingTableCacheTTL() {
		return existingTableCacheTTL;
	}

	public void setExistingTableCacheTTL(long existingTableCacheTTL) {
		this.existingTableCacheTTL = existingTableCacheTTL;
	}

	public void setExistingTableCacheTTL(int existingTableCacheTTL, TimeUnit unit) {
		this.setExistingTableCacheTTL(unit.toMillis(existingTableCacheTTL));
	}


	/**
	 * Minimum time during which a table known NOT to exist will not be tested again for existence ; default value is 3 seconds.
	 */
	public long getInexistingTableCacheTTL() {
		return inexistingTableCacheTTL;
	}

	public void setInexistingTableCacheTTL(long inexistingTableCacheTTL) {
		this.inexistingTableCacheTTL = inexistingTableCacheTTL;
	}

	public void setInexistingTableCacheTTL(int inexistingTableCacheTTL, TimeUnit unit) {
		this.setInexistingTableCacheTTL(unit.toMillis(inexistingTableCacheTTL));
	}

	public synchronized void start()
		throws DatabaseNotReachedException
	{
        if (started) return;

        try {
            Mongo.mongoLog.log(
                Level.FINE,
                "Trying to connect to the mongo database "+host+":"+port+(this.user == null ? " (no credentials)" : " (with credentials)")
			);

			if (this.user != null) {
				mongoClient = new MongoClient(new ServerAddress(host, port), Arrays.asList(MongoCredential.createCredential(this.user, this.authDB == null ? this.getDb() : this.authDB, this.pwd.toCharArray())));
			} else {
				mongoClient = new MongoClient(host, port);
			}

        } catch(Exception e) {
            Mongo.mongoLog.log(
                Level.SEVERE,
                "Could not find "+host+":"+port
            );
            throw new DatabaseNotReachedException(e);
        }

        try {
            mongoDB = mongoClient.getDatabase(db);
            // Mongo won't complain that it couldn't connect to the database
            // until the first access attempt. Force access to the DB.
            this.createTableCache();

        } catch(Exception e) {
            Mongo.mongoLog.log(
                Level.SEVERE,
                "Could not find a running database on "+host+":"+port
            );
            throw new DatabaseNotReachedException(e);
        }

        started = true;
	}
	
	private void createTableCache() {
		Map<String, TableAvailablility> hasTableCache = new HashMap<String, TableAvailablility>();
        for (String sanitizedTableName : mongoDB.listCollectionNames()) {
			hasTableCache.put(sanitizedTableName, new TableAvailablility(true));
        }
        for (Entry<String, TableAvailablility> ta : this.hasTableCache.entrySet()) {
        	if (! hasTableCache.containsKey(ta.getKey()) && !ta.getValue().isExpired()) {
    			hasTableCache.put(ta.getKey(), ta.getValue().exists ? new TableAvailablility(false) : ta.getValue());
        	}
        }
        this.hasTableCache = hasTableCache;
	}

	/**
	 * Mangles a name supposed to represent a table.
	 */
    public String sanitizeTableName(String name) {
        return name.replace('$', '_');
    }

    /**
     * Mangles a name supposed to represent a property or a column family.
     */
    protected String sanitizeName(String name) {
        return MongoNameSanitizer.sanitize(name);
    }

    /**
     * Finds back from a {@link #sanitizeName(String) sanitized name}
     * the original name of a property or a column family.
     */
	protected String dirtyName(String name) {
		return MongoNameSanitizer.dirty(name);
	}

	public boolean hasTable(String tableName)
		throws DatabaseNotReachedException
	{
		boolean ret;

        String sanitizedTableName = sanitizeTableName(tableName);

		checkIsStarted();

		try {
			TableAvailablility ta = hasTableCache.get(sanitizedTableName);
			if (ta == null || ta.isExpired()) {
				this.createTableCache();
				ta = this.hasTableCache.get(sanitizedTableName);
				if (ta == null) {
					ta = new TableAvailablility(false);
					this.hasTableCache.put(sanitizedTableName, ta);
				}
			}
			ret = ta.exists;
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}

		return ret;
	}


	protected Document findLimitedRow(String table, String row)
		throws DatabaseNotReachedException
	{
		return findLimitedRow(table, row, new TreeSet<String>(), null);
	}


	protected Document findLimitedRow(String table, String row, Set<String> families)
		throws DatabaseNotReachedException
	{
		return findLimitedRow(table, row, families, null);
	}


	protected Document findLimitedRow(String table, String row, String family)
		throws DatabaseNotReachedException
	{
		Set<String> families = new TreeSet<String>();
		families.add(family);
		return findLimitedRow(table, row, families);
	}


	protected Document findLimitedRow(String table, String row, String family, String column)
		throws DatabaseNotReachedException
	{
		Set<String> families = new TreeSet<String>();
		families.add(family);
		return findLimitedRow(table, row, families, column);
	}

	
	protected Document findLimitedRow(
		String table, String row, Set<String> families, String column
	) throws DatabaseNotReachedException
	{
		if (!hasTable(table)) {
			return null;
		}

		assert families != null && !families.isEmpty();
		assert column != null ? families.size() == 1 : true;
		
		Document query = new Document();
		Document keys = new Document(MongoRow.ROW_ENTRY_NAME, 1);

		query.put(MongoRow.ROW_ENTRY_NAME, row);
		
		for (String family : families) {
			if (families != null) {
				if (column != null) {
					keys.append(family + "." + column, 1);
				} else {
					keys.append(family, 1);
				}
			}
		}

		try {
			return mongoDB.getCollection(table).find(query).projection(keys).first();
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}
	}


	protected Document getColumns(Document families, String familyName)
		throws DatabaseNotReachedException
	{
		Document cols = (Document)families.get(familyName);
		return (cols != null) ? cols : new Document();
	}

	
	public void delete(MetaInformation meta, String table, String id)
		throws DatabaseNotReachedException
	{
		checkIsStarted();

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(id);

		try {
			mongoDB.getCollection(sanitizedTableName).deleteOne(
				new Document(MongoRow.ROW_ENTRY_NAME, sanitizedRowName)
			);
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}
	}
	
	private MSQuery createQuery(String sanitizedRowName) {
		checkIsStarted();
		
		Document query = new Document(MongoRow.ROW_ENTRY_NAME, sanitizedRowName);
		Document rowObj = new Document("$set", new Document(MongoRow.ROW_ENTRY_EXISTS, true)); // ensure the row exists
		
		MSQuery q = new MSQuery();
		q.query = query;
		q.rowObj = rowObj;
		
		return q;
	}

	private void checkIsStarted() {
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}
	}
	
	private void runQuery(MSQuery q, String sanitizedTableName) {
		try {
			MongoCollection<Document> col = mongoDB.getCollection(sanitizedTableName);
			TableAvailablility ta = hasTableCache.put(sanitizedTableName, new TableAvailablility(true));
			if ((ta == null || ta.exists == false) && !"_id".equals(MongoRow.ROW_ENTRY_NAME)) {
				col.createIndex(Indexes.ascending(MongoRow.ROW_ENTRY_NAME), new IndexOptions().unique(true));
			}
			col.updateOne(q.query, q.rowObj, new UpdateOptions().upsert(true));
		} catch (MongoException e) {
			if (e.getCode() == 11000) { // Duplicate key
				try {
					// Might tilt in some heavily concurrent environments (caught by com.googlecode.n_orm.IncrementsTest::multiThreadedIncr)
					// Retrying with the knowledge that the document already exists...
					MongoCollection<Document> col = mongoDB.getCollection(sanitizedTableName);
					col.updateOne(q.query, q.rowObj, new UpdateOptions().upsert(true));
					return;
				} catch (MongoException x) {
				}
			}
			throw new DatabaseNotReachedException(e);
		}
	}

	public void insert(
			MetaInformation meta, String table, String row, ColumnFamilyData data
	) throws DatabaseNotReachedException {
        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(row);
        MSQuery q = createQuery(sanitizedRowName);
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
		
		Document sets = (Document) q.rowObj.get("$set");
		if (sets == null) {
			sets = new Document();
			q.rowObj.append("$set", sets);
		}

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
					value = new Binary(col.getValue());
				} else if ( PersistingElement.class.isAssignableFrom(clazz) || clazz.isEnum() ) {
					value = ConversionTools.convert(String.class, col.getValue());
				} else if (REAL_TYPES.contains(clazz)) {
					value = ConversionTools.convert(clazz, col.getValue());
				} else if (SIMPLE_TYPES.contains(clazz)) {
					value = ConversionTools.convert(clazz, col.getValue());
				} else {
					value = col.getValue();
				}
				
				sets.append(
					sanitizedFamilyName + "." + sanitizedColumnName,
					value
				);
			}

		}
		
	}

	public void increment(String table, String row, Map<String, Map<String, Number>> increments) {
        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(row);
        MSQuery q = createQuery(sanitizedRowName);
        enrichQueryWithIncrement(q, sanitizedTableName, sanitizedRowName, increments);
		runQuery(q, sanitizedTableName);
	}

	public void enrichQueryWithIncrement(MSQuery q, String sanitizedTableName, String sanitizedRowName, Map<String, Map<String, Number>> increments) {
		if (increments == null || increments.isEmpty()) {
			return;
		}
		
		Document incs = (Document) q.rowObj.get("$inc");
		if (incs == null) {
			incs = new Document();
			q.rowObj.append("$inc", incs);
		}

		for (Map.Entry<String, Map<String, Number>> family : increments.entrySet()) {
			String sanitizedFamilyName = sanitizeName(family.getKey());
			Map<String, Number> columns = family.getValue();

			for (Map.Entry<String, Number> col : columns.entrySet()) {
				String sanitizedColumnName = sanitizeName(col.getKey());
				incs.append(
					sanitizedFamilyName + "." + sanitizedColumnName,
					col.getValue()
				);
			}

		}
	}
	
	public void remove(String table, String row, Map<String, Set<String>> removed) {
        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(row);
        MSQuery q = createQuery(sanitizedRowName);
        enrichQueryWithRemove(q, sanitizedTableName, sanitizedRowName, removed);
		runQuery(q, sanitizedTableName);
	}

	public void enrichQueryWithRemove(MSQuery q, String sanitizedTableName, String sanitizedRowName, Map<String, Set<String>> removed) {

		if (removed == null || removed.isEmpty())
			return;
		
		Document unsets = (Document) q.rowObj.get("$unset");
		if (unsets == null) {
			unsets = new Document();
			q.rowObj.append("$unset", unsets);
		}

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
	}
	

	public boolean exists(MetaInformation meta, String table, String row)
		throws DatabaseNotReachedException
	{
		checkIsStarted();

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(row);

		Document found = null;
		
		try {
			found = mongoDB.getCollection(sanitizedTableName)
					.find(new Document(MongoRow.ROW_ENTRY_NAME, sanitizedRowName))
					.projection(new Document(MongoRow.ROW_ENTRY_NAME, 1))
					.limit(1).first();
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}

		return found != null;
	}


	public boolean exists(
		MetaInformation meta, String table, String row, String family
	) throws DatabaseNotReachedException
	{
		checkIsStarted();

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(row);
		String sanitizedFamilyName = sanitizeName(family);
		
		Document found = null;
		Document query = new Document();
		Document keys = new Document();

		query.append(MongoRow.ROW_ENTRY_NAME, sanitizedRowName);
		query.append(sanitizedFamilyName, new Document("$type", 3));
		query.append("$where", "Object.keys(this['" + sanitizedFamilyName + "']).length > 0");
		
		keys.put(MongoRow.ROW_ENTRY_NAME, 1);

		try {
			found = mongoDB.getCollection(sanitizedTableName).find(query).projection(keys).first();
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
		checkIsStarted();

        String sanitizedTableName = sanitizeTableName(table);

		Document query = buildQueryWithRowConstraint(c);

		Document keys = new Document();
		if (families != null) {
			for (String family : families) {
				String sanitizedFamilyName = sanitizeName(family);
				keys.put(sanitizedFamilyName, 1);
			}
		}
		keys.put(MongoRow.ROW_ENTRY_NAME, 1);

		MongoCursor<Document> cur;
		try {
			cur = mongoDB.getCollection(sanitizedTableName)
					.find(query).projection(keys)
					.limit(limit).sort(new Document(MongoRow.ROW_ENTRY_NAME, 1))
					.iterator();
		} catch (MongoException e) {
			throw new DatabaseNotReachedException(e);
		}

		return new CloseableIterator(cur);
	}

	private Document buildQueryWithRowConstraint(Constraint c) {
		Document query = new Document();
		if (c != null) {
			Document conds = new Document();
			if (c.getStartKey() != null) conds.append("$gte", sanitizeName(c.getStartKey()));
			if (c.getEndKey() != null) conds.append("$lte", sanitizeName(c.getEndKey()));
			query.append(MongoRow.ROW_ENTRY_NAME, conds);
		}
		return query;
	}


	public byte[] get(
			MetaInformation meta, String table, String row,
			String family, String key
	) throws DatabaseNotReachedException
	{
		checkIsStarted();

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(row);
		String sanitizedFamilyName = sanitizeName(family);
		String sanitizedKeyName = sanitizeName(key);

		byte[] ret = null;
		Document limitedRow;
		Document columns;

		try {
			limitedRow = findLimitedRow(
				sanitizedTableName,
				sanitizedRowName,
				sanitizedFamilyName,
				sanitizedKeyName
			);
			if (limitedRow == null) return null;

			columns = getColumns(
				limitedRow,
				sanitizedFamilyName
			);

			if (columns.containsKey(sanitizedKeyName)) {
				Object val = columns.get(sanitizedKeyName);
				if (val instanceof Binary) val = ((Binary)val).getData();
				ret = ConversionTools.convert(val);
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

		checkIsStarted();

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(id);
		String sanitizedFamilyName = sanitizeName(family);
		
		Document limitedRow;
		Document columns;

		try {
			limitedRow = findLimitedRow(sanitizedTableName, sanitizedRowName, sanitizedFamilyName);
			if (limitedRow == null) return null;

			columns = getColumns(
				limitedRow,
				sanitizedFamilyName
			);

			for (Entry<String, Object> cv : columns.entrySet()) {
				Object val = cv.getValue();
				if (val instanceof Binary) val = ((Binary)val).getData();
				map.put(
					dirtyName(cv.getKey()),
					ConversionTools.convert(val)
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
		checkIsStarted();

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(id);
		String sanitizedFamilyName = sanitizeName(family);

		Document limitedRow;
		Document columns;
		Map<String, byte[]> map = new HashMap<String, byte[]>();

		try {
			limitedRow = findLimitedRow(sanitizedTableName, sanitizedRowName, sanitizedFamilyName);
			if (limitedRow == null) return null;

			columns = getColumns(
				limitedRow,
				sanitizedFamilyName
			);

			if (c != null) {
				for (Entry<String, Object> kv : columns.entrySet()) {
					String originalKey = dirtyName(kv.getKey());
					Object val = kv.getValue();
					if (val instanceof Binary) val = ((Binary)val).getData();
					boolean ok1 = (c.getStartKey() != null)
						? originalKey.compareTo(c.getStartKey()) >= 0
						: true;
					boolean ok2 = (c.getEndKey() != null)
						? originalKey.compareTo(c.getEndKey()) <= 0
						: true;
					if (ok1 && ok2) {
						map.put(
							originalKey,
							ConversionTools.convert(val)
						);
					}
				}
			}
			
			else {
				for (Entry<String, Object> kv : columns.entrySet()) {
					String originalKey = dirtyName(kv.getKey());
					Object val = kv.getValue();
					if (val instanceof Binary) val = ((Binary)val).getData();
					map.put(
						dirtyName(originalKey),
						ConversionTools.convert(val)
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

		checkIsStarted();

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(id);
		Set<String> sanitizedFamiliesNames = new TreeSet<String>();
		for (String family : families) {
			sanitizedFamiliesNames.add(this.sanitizeName(family));
		}

		Document limitedRow;
		Document columns;

		try {
			limitedRow = findLimitedRow(sanitizedTableName, sanitizedRowName, sanitizedFamiliesNames);
			if (limitedRow == null) return null;

			for (String family : families) {
				Map<String, byte[]> map = new HashMap<String, byte[]>();
				String sanitizedFamilyName = sanitizeName(family);

				columns = getColumns(limitedRow, sanitizedFamilyName);
				for (Entry<String, Object> kv : columns.entrySet()) {
					Object val = kv.getValue();
					if (val instanceof Binary) val = ((Binary)val).getData();
					map.put(
						dirtyName(kv.getKey()),
						ConversionTools.convert(val)
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
		checkIsStarted();

        String sanitizedTableName = sanitizeTableName(table);
		String sanitizedRowName = sanitizeName(id);
        MSQuery q = createQuery(sanitizedRowName);

		enrichQueryWithInsert(meta, q, sanitizedTableName, sanitizedRowName, changed);
		enrichQueryWithIncrement(q, sanitizedTableName, sanitizedRowName, increments);
		enrichQueryWithRemove(q, sanitizedTableName, sanitizedRowName, removed);

		runQuery(q, sanitizedTableName);

	}


	public long count(MetaInformation meta, String table, Constraint c)
		throws DatabaseNotReachedException
	{
		checkIsStarted();

        String sanitizedTableName = sanitizeTableName(table);

		long cnt = 0;

		Bson query = buildQueryWithRowConstraint(c);

		try {
			cnt = mongoDB.getCollection(sanitizedTableName).countDocuments(query == null ? new BasicDBObject() : query);
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}

		return cnt;
	}

	public void dropTable(String table)
	{
		checkIsStarted();

        String sanitizedTableName = sanitizeTableName(table);

		try {
			mongoDB.getCollection(sanitizedTableName).drop();
			hasTableCache.put(sanitizedTableName, new TableAvailablility(false));
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
		this.setDB(dbname);
	}

	public void setDB(String dbname)
	{
		this.db = dbname;

		if (started) {
			mongoDB = mongoClient.getDatabase(db);
		}
	}

	public synchronized void close()
		throws DatabaseNotReachedException
	{
		checkIsStarted();

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
