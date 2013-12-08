import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Arrays;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import static org.junit.Assert.*;

import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.mongo.MongoStore;
import com.googlecode.n_orm.DatabaseNotReachedException;

import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;


public class BaseTest
{
	private static String DBNAME     = "n_orm_test";
	private static String COLLECTION = "defaultcol";

	@Before
	public void prepareTests()
	{
		MongoStore mongoStore = new MongoStore();

		mongoStore.setDB(DBNAME);
		mongoStore.start();
		mongoStore.dropTable(COLLECTION);
		mongoStore.close();
	}

	@Test
	public void notStartedTest()
	{
		MongoStore mongoStore = new MongoStore();

		try {
			mongoStore.count(null, null, null);
		} catch (DatabaseNotReachedException e) {
			; // ok
		} catch (Exception e) {
			assert false;
		}

		try {
			mongoStore.close();
		} catch (DatabaseNotReachedException e) {
			; // ok
		} catch (Exception e) {
			assert false;
		}
	}

	@Test
	public void collectionTest()
	{
		String TEST_ROW = "my_super_test_row";
		String TEST_FAMILY = "my_super_test_family";
		String TEST_COLLECTION = "my_super_test_collection";

		MongoStore mongoStore = new MongoStore();
		mongoStore.setDB(DBNAME);

		ColumnFamilyData data = new DefaultColumnFamilyData();
		Map<String, byte[]> col = new HashMap<String, byte[]>();

		col.put("toto", (new String("haha")).getBytes());
		col.put("tutu", (new String("bebe")).getBytes());
		data.put(TEST_FAMILY, col);

		mongoStore.start();

		// add a collection by inserting something in it
		mongoStore.insert(null, TEST_COLLECTION, TEST_ROW, data);

		// test for the existance of the inserted elements
		assertTrue(mongoStore.hasTable(TEST_COLLECTION));
		assertTrue(mongoStore.exists(null, TEST_COLLECTION, TEST_ROW));
		assertTrue(mongoStore.exists(null, TEST_COLLECTION, TEST_ROW, TEST_FAMILY));

		// remove the table
		mongoStore.dropTable(TEST_COLLECTION);
		assertFalse(mongoStore.hasTable(TEST_COLLECTION));

		mongoStore.close();
	}

	@Test
	public void addAndRetrieve()
	{
		MongoStore mongoStore = new MongoStore();
		mongoStore.setDB(DBNAME);

		mongoStore.start();
		ColumnFamilyData data = new DefaultColumnFamilyData();

		Map<String, byte[]> col_ret;
		Map<String, byte[]> col1 = new HashMap<String, byte[]>();
		Map<String, byte[]> col2 = new HashMap<String, byte[]>();

		col1.put("toto", (new String("haha")).getBytes());
		col1.put("tutu", (new String("bebe")).getBytes());

		col2.put("machin", (new String("truc"    )).getBytes());
		col2.put("bidule", (new String("chouette")).getBytes());

		data.put("1", col1);
		data.put("2", col2);

		mongoStore.insert(null, COLLECTION, "truc", data);
		
		col_ret = mongoStore.get(null, COLLECTION, "truc", "1");
		assertEquals(col_ret.size(), col1.size());
		for (String key : col_ret.keySet()) {
			assertArrayEquals(col_ret.get(key), col1.get(key));
		}

		col_ret = mongoStore.get(null, COLLECTION, "truc", "2");
		assertEquals(col_ret.size(), col2.size());
		for (String key : col_ret.keySet()) {
			assertArrayEquals(col_ret.get(key), col2.get(key));
		}

		for (String key : col1.keySet()) {
			assertArrayEquals(
				col1.get(key),
				mongoStore.get(null, COLLECTION, "truc", "1", key)
			);
		}

		for (String key : col2.keySet()) {
			assertArrayEquals(
				col2.get(key),
				mongoStore.get(null, COLLECTION, "truc", "2", key)
			);
		}

		mongoStore.delete(null, COLLECTION, "truc");
		mongoStore.close();
	}
}
