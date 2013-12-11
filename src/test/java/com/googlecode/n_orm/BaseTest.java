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

import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;


public class BaseTest
{
	private static String DBNAME     = "n_orm_test";
	private static String COLLECTION = "defaultcol";

	private String TEST_ROW = "my_super_test_row";
	private String TEST_FAMILY = "my_super_test_family";


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
		MongoStore mongoStore = new MongoStore();
		mongoStore.setDB(DBNAME);

		ColumnFamilyData data = new DefaultColumnFamilyData();
		Map<String, byte[]> col = new HashMap<String, byte[]>();

		col.put("toto", (new String("haha")).getBytes());
		col.put("tutu", (new String("bebe")).getBytes());
		data.put(TEST_FAMILY, col);

		mongoStore.start();

		// add a collection by inserting something in it
		mongoStore.insert(null, COLLECTION, TEST_ROW, data);

		// test for the existance of the inserted elements
		assertTrue(mongoStore.hasTable(COLLECTION));
		assertTrue(mongoStore.exists(null, COLLECTION, TEST_ROW));
		assertTrue(mongoStore.exists(null, COLLECTION, TEST_ROW, TEST_FAMILY));

		// remove an element
		mongoStore.delete(null, COLLECTION, TEST_ROW);
		assertFalse(mongoStore.exists(null, COLLECTION, TEST_ROW));

		// remove the table
		mongoStore.dropTable(COLLECTION);
		assertFalse(mongoStore.hasTable(COLLECTION));

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
		
		// test columns retrieval
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

		// test individual entries retrieval
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


	@Test
	public void constraintTest()
	{
		MongoStore mongoStore = new MongoStore();
		mongoStore.setDB(DBNAME);

		mongoStore.start();
		ColumnFamilyData data1 = new DefaultColumnFamilyData();
		ColumnFamilyData data2 = new DefaultColumnFamilyData();

		Map<String, byte[]> col_ret;
		Map<String, byte[]> col1 = new HashMap<String, byte[]>();
		Map<String, byte[]> col2 = new HashMap<String, byte[]>();

		col1.put("toto", (new String("haha")).getBytes());
		col1.put("tutu", (new String("bebe")).getBytes());

		col2.put("toto", (new String("truc"    )).getBytes());
		col2.put("tutu", (new String("chouette")).getBytes());

		data1.put(TEST_FAMILY, col1);
		data2.put(TEST_FAMILY, col2);

		mongoStore.insert(null, COLLECTION, "truc",  data1);
		mongoStore.insert(null, COLLECTION, "chose", data2);

		Constraint c = new Constraint("toto", "toto");
		col_ret = mongoStore.get(null, COLLECTION, "truc", TEST_FAMILY, c);
		assertTrue(col_ret.size() == 1);
	}
}
