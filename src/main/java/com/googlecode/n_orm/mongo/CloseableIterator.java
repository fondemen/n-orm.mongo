package com.googlecode.n_orm.mongo;

import com.googlecode.n_orm.storeapi.Row;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.BasicDBObject;

final class CloseableIterator implements CloseableKeyIterator
{
	private DBCursor cursor;

	public CloseableIterator(DBCursor cur) {
		cursor = cur;
	}

	@Override
	public boolean hasNext() {
		return cursor.hasNext();
	}

	@Override
	public Row next() {
		return new MongoRow(cursor.next());
	}

	public Row curr() {
		return new MongoRow(cursor.curr());
	}

	@Override
	public void remove() {
		throw new IllegalStateException("Cannot remove key from a result set.");
	}

	@Override
	public void close() {
	}
}
