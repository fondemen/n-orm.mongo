package com.googlecode.n_orm.mongo;

import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.SimpleStore;


public class SimpleMongoStore implements SimpleStore
{
	private static MongoStore store = new MongoStore();

	public void start() {
		store.start();
	}

	public void setDB(String dbname)
	{
		store.setDB(dbname);
	}

	public void dropTable(String table)
	{
		store.dropTable(table);
	}


	public boolean hasTable(String tableName)
		throws DatabaseNotReachedException
	{
		return store.hasTable(tableName);
	}


	public void delete(String table, String id)
		throws DatabaseNotReachedException
	{
		store.delete(null, table, id);
	}


	public boolean exists(String table, String row)
		throws DatabaseNotReachedException
	{
		return store.exists(null, table, row);
	}


	public boolean exists(String table, String row, String family)
		throws DatabaseNotReachedException
	{
		return store.exists(null, table, row, family);
	}


	public CloseableKeyIterator get(
			 String table,
			 Constraint c, int limit, Set<String> families
	) throws DatabaseNotReachedException
	{
		return store.get(null, table, c, limit, families);
	}


	public byte [] get(String table, String row, String family, String key)
		throws DatabaseNotReachedException
	{
		return store.get(null, table, row, family, key);
	}


	public Map<String, byte[]> get(String table, String id, String family)
		throws DatabaseNotReachedException
	{
		return store.get(null, table, id, family);
	}


	public Map<String, byte[]> get(String table, String id, String family, Constraint c)
		throws DatabaseNotReachedException
	{
		return store.get(null, table, id, family, c);
	}


	public ColumnFamilyData get(String table, String id, Set<String> families)
		throws DatabaseNotReachedException
	{
		return store.get(null, table, id, families);
	}


	public void storeChanges(
		String table, String id,
		ColumnFamilyData changed,
		Map<String, Set<String>> removed,
		Map<String, Map<String, Number>> increments
	) throws DatabaseNotReachedException
	{
		store.storeChanges(null, table, id, changed, removed, increments);
	}


	public long count(String table, Constraint c)
		throws DatabaseNotReachedException
	{
		return store.count(null, table, c);
	}


}
