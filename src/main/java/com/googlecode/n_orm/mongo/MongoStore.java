package com.googlecode.n_orm.mongo;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.InetAddress;

import com.googlecode.n_orm.DatabaseNotReachedException;

import com.googlecode.n_orm.storeapi.Store;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.GenericStore;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;


public class MongoStore implements Store, GenericStore
{
	private static String DB_NAME    = "n_orm";
	private static short  MONGO_PORT =  27017;
	private static String MONGO_HOST = "localhost";

	private static String hostname;

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
	}

	private DB mongoDB;
	private MongoClient mongoClient;

	private int    port = MONGO_PORT;
	private String host = hostname;
	private String db   = DB_NAME;

	private boolean started = false;


	public void start()
		throws DatabaseNotReachedException
	{
		synchronized (MongoStore.class) {
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
	}

	public boolean hasTable(String tableName)
		throws DatabaseNotReachedException
	{
		boolean ret;

		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

		try {
			//TODO: cache results
			ret = mongoDB.collectionExists(tableName);
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}

		return ret;
	}

	protected DBObject findRow(String table, String row)
		throws DatabaseNotReachedException
	{
		if (!hasTable(table)) {
			return null;
		}

		DBObject o;

		try {
			o = mongoDB.getCollection(table).findOne(
				new BasicDBObject(MongoRow.ROW_ENTRY_NAME, row)
			);
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}

		return o;
	}


	protected DBObject findLimitedRow(String table, String row)
		throws DatabaseNotReachedException
	{
		return findLimitedRow(table, row, null, null);
	}


	protected DBObject findLimitedRow(String table, String row, String family)
		throws DatabaseNotReachedException
	{
		return findLimitedRow(table, row, family, null);
	}

	
	protected DBObject findLimitedRow(
		String table, String row, String family, String column
	) throws DatabaseNotReachedException
	{
		if (!hasTable(table)) {
			return null;
		}

		DBObject limitedRow;
		DBObject query = new BasicDBObject();
		DBObject keys = new BasicDBObject();

		query.put(MongoRow.ROW_ENTRY_NAME, row);
		if (family != null) {
			if (column != null) {
				keys.put(MongoRow.FAM_ENTRIES_NAME + "." + family + "." + column, 1);
			} else {
				keys.put(MongoRow.FAM_ENTRIES_NAME + "." + family, 1);
			}
		}
		keys.put("_id", 0);

		try {
			limitedRow = mongoDB.getCollection(table).findOne(query, keys);
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}

		return limitedRow;
	}


	protected DBObject getFamilies(DBObject row)
		throws DatabaseNotReachedException
	{
		return (DBObject) row.get(MongoRow.FAM_ENTRIES_NAME);
	}

	
	protected DBObject getColumns(DBObject families, String familyName)
		throws DatabaseNotReachedException
	{
		return (DBObject) families.get(familyName);
	}

	
	public void delete(MetaInformation meta, String table, String id)
		throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

		try {
			// TODO: handle return value of the following operation
			mongoDB.getCollection(table).remove(
				new BasicDBObject(MongoRow.ROW_ENTRY_NAME, id)
			);
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}
	}

	public void insert(
			MetaInformation meta, String table, String row, ColumnFamilyData data
	) throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

		DBObject familiesList = new BasicDBObject();
		for (Map.Entry<String, Map<String, byte[]>> family : data.entrySet()) {

			DBObject columnsObj = new BasicDBObject();
			for (Map.Entry<String, byte[]> column : family.getValue().entrySet()) {
				columnsObj.put(column.getKey(), column.getValue());
			}

			familiesList.put(family.getKey(), columnsObj);
		}

		DBObject rowObj = new BasicDBObject();
		rowObj.put(MongoRow.ROW_ENTRY_NAME, row);
		rowObj.put(MongoRow.FAM_ENTRIES_NAME, familiesList);

		DBObject query = new BasicDBObject(MongoRow.ROW_ENTRY_NAME, row);
		DBCollection col = mongoDB.getCollection(table);

		col.update(query, rowObj, true, false);
	}
	

	public boolean exists(MetaInformation meta, String table, String row)
		throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

		long count = 0;
		
		try {
			count = mongoDB.getCollection(table).getCount(
				new BasicDBObject(MongoRow.ROW_ENTRY_NAME, row)
			);
		} catch (Exception e) {
			//throw new DatabaseNotReachedException(e);
		}

		return count > 0;
	}


	public boolean exists(
		MetaInformation meta, String table, String row, String family
	) throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

		DBObject families;
		boolean ret = false;

		try {
			families = getFamilies(findLimitedRow(table, row));
			ret = families.containsField(family);
		} catch (Exception e) {
			//throw new DatabaseNotReachedException(e);
		}

		return ret;
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

		DBObject query = new BasicDBObject();
		query.put(
			"$query",
			new BasicDBObject() // don'task me why, this empty object must be there
		);
		if (c != null) {
			query.put(
				"$where",
				"this." + MongoRow.ROW_ENTRY_NAME + " >= '" + c.getStartKey() + "'"
				+ " && " +
				"this." + MongoRow.ROW_ENTRY_NAME + " <= '" + c.getEndKey()   + "'"
			);
		}
		query.put(
			"$orderby",
			new BasicDBObject(
				MongoRow.ROW_ENTRY_NAME,
				1
			)
		);
		query.put(
			"$limit",
			Integer.toString(limit)
		);

		DBObject keys = new BasicDBObject();
		if (families != null) {
			for (String family : families) {
				keys.put(MongoRow.FAM_ENTRIES_NAME + "." + family, 1);
			}
		}
		keys.put("_id", 0);

		DBCursor cur = mongoDB.getCollection(table).find(query, keys);

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

		byte[] ret;
		DBObject limitedRow, columns;

		try {
			limitedRow = findLimitedRow(table, row, family, key);

			columns = getColumns(
				getFamilies(limitedRow),
				family
			);

			ret = (byte[])(columns.get(key));
		} catch (Exception e) {
			return null;
			//throw new DatabaseNotReachedException(e);
		}

		return ret;
	}


	public Map<String, byte[]> get(
		MetaInformation meta, String table, String id, String family
	) throws DatabaseNotReachedException
	{
		Map<String, byte[]> map = new HashMap();

		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}
		
		DBObject limitedRow, columns;

		try {
			limitedRow = findLimitedRow(table, id, family);

			columns = getColumns(
				getFamilies(limitedRow),
				family
			);

			for (String key : columns.keySet()) {
				map.put(key, (byte[])(columns.get(key)));
			}

		} catch (Exception e) {
			return null;
			//throw new DatabaseNotReachedException(e);
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

		DBObject limitedRow, columns;
		Map<String, byte[]> map = new HashMap();

		try {
			limitedRow = findLimitedRow(table, id, family);

			columns = getColumns(
				getFamilies(limitedRow),
				family
			);

			if (c != null) {
				for (String key : columns.keySet()) {
                    boolean ok1 = true;
                    boolean ok2 = true;
                    if (c.getStartKey() != null) {
                        ok1 = key.compareTo(c.getStartKey()) >= 0;
                    }
                    if (c.getEndKey() != null) {
                        ok2 = key.compareTo(c.getEndKey()) <= 0;
                    }
					if (ok1 && ok2) {
						map.put(key, (byte[])(columns.get(key)));
					}
				}
			}
			
			else {
				for (String key : columns.keySet()) {
					map.put(key, (byte[])(columns.get(key)));
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

		DBObject limitedRow, fam, columns;

		try {
			limitedRow = findLimitedRow(table, id);
			fam = getFamilies(limitedRow);

			for (String family : families) {
				Map map = new HashMap();
				columns = getColumns(fam, family);

				for (String key : columns.keySet()) {
					map.put(key, (byte[])(columns.get(key)));
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

		// update
		if (changed != null) {
			insert(null, table, id, changed);
		}


		DBObject query = new BasicDBObject(MongoRow.ROW_ENTRY_NAME, id);

		// remove columns
		if (removed != null) {
			for (String family : removed.keySet()) {
				Set<String> columns = removed.get(family);

				DBObject unsets = new BasicDBObject();
				for (String col: columns) {
					unsets.put(
						MongoRow.FAM_ENTRIES_NAME + "." + family + "." + col,
						"''"
					);
				}

				mongoDB.getCollection(table).update(
					query,
					new BasicDBObject("$unset", unsets)
				);
			}
		}

		// increments
		if (increments != null) {
			for (String family : increments.keySet()) {
				Map<String, Number> columns = increments.get(family);

				DBObject incs = new BasicDBObject();
				for (String col: columns.keySet()) {
					incs.put(
						MongoRow.FAM_ENTRIES_NAME + "." + family + "." + col,
						columns.get(col)
					);
				}

				mongoDB.getCollection(table).update(
					query,
					new BasicDBObject("$inc", incs)
				);
			}
		}
	}


	public long count(MetaInformation meta, String table, Constraint c)
		throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

		long cnt = 0;

		DBObject query = new BasicDBObject();

		if (c != null) {
			query.put(
				"$where",
				"this." + MongoRow.ROW_ENTRY_NAME + " >= '" + c.getStartKey() + "'"
				+ " && " +
				"this." + MongoRow.ROW_ENTRY_NAME + " <= '" + c.getEndKey()   + "'"
			);
		}

		try {
			cnt = mongoDB.getCollection(table).count(query);
		} catch (Exception e) {
		}

		return cnt;
	}

	public void dropTable(String table)
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

		try {
			mongoDB.getCollection(table).drop();
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

	public void setDB(String dbname)
	{
		this.db = dbname;

		if (started) {
			mongoDB = mongoClient.getDB(db);
		}
	}

	public void close()
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
