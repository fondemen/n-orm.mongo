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


/* Implementation notes:
 *
 * A Table ~~ A Collection               ~~ A @persisting class
 * A Row   ~~ A Document (1st lvl entry)
**/

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

	public boolean hasTable(String tableName)
		throws DatabaseNotReachedException
	{
		boolean ret;

		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

		try {
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


	protected DBObject getFamilies(DBObject row)
		throws DatabaseNotReachedException
	{
		return (DBObject) row.get(MongoRow.FAM_ENTRY_NAME);
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

		DBObject o = findRow(table, id);
		
		if (o != null) {
			try {
				// TODO: handle return value of the following operation
				mongoDB.getCollection(table).remove(o);
			} catch (Exception e) {
				throw new DatabaseNotReachedException(e);
			}
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

		DBObject familyObj = new BasicDBObject();
		for (Map.Entry<String, Map<String, byte[]>> family : data.entrySet()) {

			DBObject columnObj = new BasicDBObject();
			for (Map.Entry<String, byte[]> column : family.getValue().entrySet()) {
				columnObj.put(column.getKey(), column.getValue());
			}

			familyObj.put(family.getKey(), columnObj);
		}

		DBObject rowObj = new BasicDBObject();
		rowObj.put(MongoRow.ROW_ENTRY_NAME, row);
		rowObj.put(MongoRow.FAM_ENTRY_NAME, familyObj);

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

		return (findRow(table, row) == null) ? false : true;
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

		try {
			families = getFamilies(findRow(table, row));
		} catch (DatabaseNotReachedException e) {
			throw e;
		} catch (Exception e) {
			throw new DatabaseNotReachedException("Malformed row");
		}

		return families.containsField(family);
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

		return null;
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

		DBObject columns;

		try {
			columns = getColumns(
				getFamilies(findRow(table, row)),
				family
			);
		} catch (DatabaseNotReachedException e) {
			throw e;
		} catch (Exception e) {
			throw new DatabaseNotReachedException("Malformed row");
		}

		return (byte[])(columns.get(key));
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
		
		DBObject columns;

		try {
			columns = getColumns(
				getFamilies(findRow(table, id)),
				family
			);

			for (String key : columns.keySet()) {
				map.put(key, (byte[])(columns.get(key)));
			}

		} catch (DatabaseNotReachedException e) {
			throw e;
		} catch (Exception e) {
			throw new DatabaseNotReachedException("Malformed row");
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

		DBObject columns;
		Map<String, byte[]> map = new HashMap();

		try {
			columns = getColumns(
				getFamilies(findRow(table, id)),
				family
			);

			for (String key : columns.keySet()) {
				if (c.sastisfies(key)) {
					map.put(key, (byte[])(columns.get(key)));
				}
			}

		} catch (DatabaseNotReachedException e) {
			throw e;
		} catch (Exception e) {
			throw new DatabaseNotReachedException("Malformed row");
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

		DBObject fam, columns;

		try {
			fam = getFamilies(findRow(table, id));

			for (String family : families) {
				Map map = new HashMap();
				columns = getColumns(fam, family);

				for (String key : columns.keySet()) {
					map.put(key, (byte[])(columns.get(key)));
				}

				mapOfMaps.put(family, map);
			}
		} catch (DatabaseNotReachedException e) {
			throw e;
		} catch (Exception e) {
			throw new DatabaseNotReachedException("Malformed row");
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
	}


	public long count(MetaInformation meta, String table, Constraint c)
		throws DatabaseNotReachedException
	{
		if (!started) {
			Mongo.mongoLog.log(Level.SEVERE, "Store not started");
			throw new DatabaseNotReachedException("Store not started");
		}

		long cnt = 0;
		DBObject columns, families;

		try {
			DBCursor cursor = mongoDB.getCollection(table).find();

			for (DBObject row : cursor.toArray()) {
				families = getFamilies(row);

				for (String fam : families.keySet()) {
					columns = getColumns(families, fam);

					for (String key : columns.keySet()) {
						if (c.sastisfies(key)) {
							cnt++;
						}
					}
				}
			}

		} catch (DatabaseNotReachedException e) {
			throw e;
		} catch (Exception e) {
			throw new DatabaseNotReachedException("Malformed row");
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
