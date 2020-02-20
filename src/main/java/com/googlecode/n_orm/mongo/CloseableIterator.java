package com.googlecode.n_orm.mongo;

import org.bson.Document;

import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Row;
import com.mongodb.client.MongoCursor;

final class CloseableIterator implements CloseableKeyIterator
{
	private MongoCursor<Document> cursor;

	public CloseableIterator(MongoCursor<Document> cur) {
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

	@Override
	public void remove() {
		throw new IllegalStateException("Cannot remove key from a result set.");
	}

	@Override
	public void close() {
		cursor = null;
	}
}
