import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
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
		mongoStore.dropTable(COLLECTION);

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
		mongoStore.dropTable(COLLECTION);

		ColumnFamilyData data = new DefaultColumnFamilyData();

		Map<String, byte[]> col_ret;
		Map<String, byte[]> col1 = new HashMap<String, byte[]>();
		Map<String, byte[]> col2 = new HashMap<String, byte[]>();

		col1.put("toto", (new String("haha")).getBytes());
		col1.put("tutu", (new String("bebe")).getBytes());

		col2.put("machin", (new String("truc"    )).getBytes());
		col2.put("bidule", (new String("chouette")).getBytes());

		data.put("fam1", col1);
		data.put("fam2", col2);

		mongoStore.insert(null, COLLECTION, "truc", data);

		// test full ColumnFamilyData retrieval
		ColumnFamilyData data_ret
			= mongoStore.get(null, COLLECTION, "truc", data.keySet());
		assertEquals(data.size(), data_ret.size());
		for (String fam : data.keySet()) {
			for (String col : data.get(fam).keySet()) {
				assertArrayEquals(
					data.get(fam).get(col),
					data_ret.get(fam).get(col)
				);
			}
		}
		
		// test columns retrieval
		col_ret = mongoStore.get(null, COLLECTION, "truc", "fam1");
		assertEquals(col_ret.size(), col1.size());
		for (String key : col_ret.keySet()) {
			assertArrayEquals(col_ret.get(key), col1.get(key));
		}

		col_ret = mongoStore.get(null, COLLECTION, "truc", "fam2");
		assertEquals(col_ret.size(), col2.size());
		for (String key : col_ret.keySet()) {
			assertArrayEquals(col_ret.get(key), col2.get(key));
		}

		// test individual entries retrieval
		for (String key : col1.keySet()) {
			assertArrayEquals(
				col1.get(key),
				mongoStore.get(null, COLLECTION, "truc", "fam1", key)
			);
		}

		for (String key : col2.keySet()) {
			assertArrayEquals(
				col2.get(key),
				mongoStore.get(null, COLLECTION, "truc", "fam2", key)
			);
		}

		mongoStore.close();
	}


	@Test
	public void constraintTest()
	{
		MongoStore mongoStore = new MongoStore();
		mongoStore.setDB(DBNAME);

		mongoStore.start();
		mongoStore.dropTable(COLLECTION);

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

		Constraint c1 = new Constraint("truc", "truc");
		assertTrue(mongoStore.count(null, COLLECTION, c1) == 1);

		Constraint c2 = new Constraint("chose", "truc");
		assertTrue(mongoStore.count(null, COLLECTION, c2) == 2);

		Constraint c3 = new Constraint("a", "zzzzzzzzzzz");
		assertTrue(mongoStore.count(null, COLLECTION, c3) == 2);

		Constraint c4 = new Constraint("zzzzzzzzzz", "zzzzzzzzzzzzzzzz");
		assertTrue(mongoStore.count(null, COLLECTION, c4) == 0);

		mongoStore.close();
	}

	@Test
	public void storeChangesTest()
	{
		MongoStore mongoStore = new MongoStore();
		mongoStore.setDB(DBNAME);

		mongoStore.start();
		mongoStore.dropTable(COLLECTION);

		ColumnFamilyData data1 = new DefaultColumnFamilyData();

		Map<String, byte[]> col_ret;
		Map<String, byte[]> col1 = new HashMap<String, byte[]>();
		Map<String, byte[]> col2 = new HashMap<String, byte[]>();

		col1.put("toto", (new String("2")).getBytes());
		col1.put("tutu", (new String("5")).getBytes());

		col2.put("tete", (new String("42")).getBytes());
		col2.put("titi", (new String("33")).getBytes());

		data1.put("fam1", col1);
		data1.put("fam2", col2);

		mongoStore.insert(null, COLLECTION, "truc",  data1);

		Map<String, Set<String>> removed = new HashMap<String, Set<String>>();
		Set s1 = new TreeSet<String>();
		Set s2 = new TreeSet<String>();
		s1.add("toto");
		s2.add("titi");
		removed.put("fam1", s1);
		removed.put("fam2", s2);

		mongoStore.storeChanges(null, COLLECTION, "truc", null, removed, null);

		col_ret = mongoStore.get(null, COLLECTION, "truc", "fam1");
		assertEquals(col_ret.size(), col1.size() - 1);
		assertArrayEquals(col_ret.get("tutu"), col1.get("tutu"));

		col_ret = mongoStore.get(null, COLLECTION, "truc", "fam2");
		assertEquals(col_ret.size(), col2.size() - 1);
		assertArrayEquals(col_ret.get("tete"), col2.get("tete"));

		mongoStore.close();
	}
}
