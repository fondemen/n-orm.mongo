package com.googlecode.n_orm.mongo;

import java.util.Set;
import java.util.Map;

import com.googlecode.n_orm.DatabaseNotReachedException;

import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;


public class Store implements com.googlecode.n_orm.storeapi.Store
{
	public void start()
		throws DatabaseNotReachedException
	{
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

	public long count( MetaInformation meta, String table, Constraint c)
		throws DatabaseNotReachedException
	{
		return 0;
	}
}
