package com.googlecode.n_orm.mongo;

import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Row;
import com.mongodb.DBCursor;

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
		MongoRow row = null;

		if (cursor.hasNext()) {
			row = new MongoRow(cursor.next());
		}

		return row;
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
		cursor = null;
	}
}
