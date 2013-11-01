package com.googlecode.n_orm.mongo;

import java.util.Set;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.InetAddress;

import com.googlecode.n_orm.DatabaseNotReachedException;

import com.googlecode.n_orm.storeapi.Store;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.GenericStore;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class MongoStore implements Store, GenericStore
{
	private int    port = 0;
	private String host = null;

	private static String DB_NAME = "n_orm";

	private MongoClient mongoClient;


	public void start()
		throws DatabaseNotReachedException
	{
		if (host == null) {
			try {
				host = InetAddress.getLocalHost().getHostAddress();
			} catch (Exception e) {
				Mongo.mongoLog.log(Level.WARNING, "Could not get localhost.");
				host = "localhost";
			}
		}

		try {
			Mongo.mongoLog.log(
				Level.FINE,
				"Trying to connect to the mongo database "+host+":"+port
			);
			mongoClient = (port == 0)
				? new MongoClient(host)
				: new MongoClient(host, port);

			// MongoClient won't complain that it couldn't connect to the
			// database until the first access attempt. Force access to the DB
			mongoClient.getDB(DB_NAME);

		} catch(Exception e) {
			Mongo.mongoLog.log(
				Level.SEVERE,
				"Could not find "+host+":"+port
			);
			throw new DatabaseNotReachedException(e);
		}
	}

	public boolean hasTable(String tableName)
		throws DatabaseNotReachedException
	{
		return false;
	}

	public void delete(MetaInformation meta, String table, String id)
		throws DatabaseNotReachedException
	{
	}

	public boolean exists(MetaInformation meta, String table, String row)
		throws DatabaseNotReachedException
	{
		return false;
	}

	public boolean exists(
		MetaInformation meta, String table, String row,
		String family
	) throws DatabaseNotReachedException
	{
		return false;
	}


	public CloseableKeyIterator get(
		MetaInformation meta, String table,
		Constraint c, int limit, Set<String> families
	) throws DatabaseNotReachedException
	{
		return null;
	}

	public byte[] get(
			MetaInformation meta, String table, String row,
			String family, String key
	) throws DatabaseNotReachedException
	{
		return null;
	}

	public Map<String, byte[]> get(
		MetaInformation meta, String table, String id, String family
	) throws DatabaseNotReachedException
	{
		return null;
	}

	public Map<String, byte[]> get(
		MetaInformation meta, String table,
		String id, String family, Constraint c
	) throws DatabaseNotReachedException
	{
		return null;
	}

	public ColumnFamilyData get(
		MetaInformation meta, String table, String id, Set<String> families
	) throws DatabaseNotReachedException
	{
		return null;
	}

	public void storeChanges(
		MetaInformation meta, String table, String id,
		ColumnFamilyData changed, Map<String, Set<String>> removed,
		Map<String, Map<String, Number>> increments
	) throws DatabaseNotReachedException
	{
	}

	public long count(MetaInformation meta, String table, Constraint c)
		throws DatabaseNotReachedException
	{
		return 0;
	}

	public void setHost(String host)
	{
		this.host = host;
	}

	public void setPort(int port)
	{
		this.port = port;
	}
}
