package com.googlecode.n_orm.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.Row;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;


public class BasicTest {
	private static SimpleMongoStore store;
	private static String DBNAME          = "n_orm_test";
	private static final String testTable = "testtable";
	
	@BeforeClass
	public static void prepareStore() {
		store = new SimpleMongoStore();
		store.setDB(DBNAME);
		store.start();
		store.dropTable(testTable);

		ColumnFamilyData data = new DefaultColumnFamilyData();
		Map<String, byte[]> dataFamily1 = new HashMap<String, byte[]>();
		Map<String, byte[]> dataFamily2 = new HashMap<String, byte[]>();
		dataFamily1.put("cle", new String("valeur").getBytes());
		dataFamily1.put("cle2", new String("valeur2").getBytes());
		dataFamily1.put("cle21", new String("valeur21").getBytes());
		dataFamily1.put("cle3", new String("valeur3").getBytes());
		dataFamily2.put("cle-fam2", new String("1").getBytes());
		dataFamily2.put("cle-incr", new String("42").getBytes());
		
		data.put("family1", dataFamily1);
		data.put("family2", dataFamily2);
		
		store.storeChanges(testTable, "123456", data, null, null);
		store.storeChanges(testTable, "123457", data, null, null);
		store.storeChanges(testTable, "123458", data, null, null);
		store.storeChanges(testTable, "300000", data, null, null);
		store.storeChanges(testTable, "300010", data, null, null);
		store.storeChanges(testTable, "400000", data, null, null);
	}

	protected Map<String, Field> toNaiveMap(String field) {
		Map<String, Field> ret = new TreeMap<String, Field>();
		ret.put(field, null);
		return ret;
	}

	@Test
	public void test02GetTableWithFamily() {
		Map<String, byte[]> result = store.get(testTable, "123456", "family1");
		assertEquals("valeur", new String(result.get("cle")));
	}
	
	@Test
	public void test03GetTableWithFamilyAndKey() {
		assertEquals("valeur", new String(store.get( testTable, "123456", "family1", "cle")));
		assertSame(null, store.get( testTable, "123456", "family1", "not a key"));
		assertSame(null, store.get( testTable, "123456", "not a real family", "not a key"));
	}
	
	@Test
	public void test03Count() {
		assertEquals(6,store.count(testTable, null));
		assertEquals(1,store.count(testTable, new Constraint("400000", "400001")));
		assertEquals(0,store.count(testTable, new Constraint("a", "b")));
	}

	@Test
	public void test05CountWithConstraint() {
		assertEquals(3,store.count(testTable, new Constraint("123450", "123460")));
		assertEquals(3,store.count(testTable, new Constraint("1", "2")));
		assertEquals(0,store.count(testTable, new Constraint("a", "z")));
		assertEquals(6,store.count(testTable, null));
	}
	
	@Test
	public void test07getTableWithSetFamilies() {
		Set<String> families = new TreeSet<String>();
		families.add("family1");
		families.add("family2");
		ColumnFamilyData result = store.get(testTable, "123456", families);
		
		assertEquals("1", new String(result.get("family2").get("cle-fam2")));
		assertFalse("not a valid value".equals(new String(result.get("family2").get("cle-fam2"))));
	}
	
	@Test
	public void test08existsId() {
		assertTrue(store.exists(testTable, "123456"));
		assertFalse(store.exists(testTable, "no valid id"));
	}
	
	@Test
	public void test09existsIdWithFamily() {
		assertTrue(store.exists( testTable, "123456", "family1"));
		assertTrue(store.exists( testTable, "123456", "family2"));
		assertFalse(store.exists(testTable, "not a valid id","family1"));
		assertFalse(store.exists(testTable, "123456","not a valid family"));
	}

	@Ignore
	@Test
	public void test10delete() {
		assertTrue(store.exists(testTable, "300000"));
		assertTrue(store.exists(testTable, "300000", "family1"));
		store.delete(testTable, "300000");
		assertFalse(store.exists(testTable, "300000"));
		assertFalse(store.exists(testTable, "300000", "family1"));
	}
	
	@Test
	public void test15getIterator() {
		Set<String> families = new TreeSet<String>();
		families.add("family1");
		families.add("family2");
		
		CloseableKeyIterator it = store.get(testTable, new Constraint("400000", "400001"), 50, families);
		assertEquals("valeur", new String(it.next().getValues().get("family1").get("cle")));
		
		it = store.get(testTable, null, 50, families);
		Row row = it.next();
		assertEquals("valeur", new String(row.getValues().get("family1").get("cle")));
		assertEquals("123456", row.getKey());
		while(it.hasNext())
			it.next();
		
		assertNull(it.next());
		
		try {
			it.remove();
		} catch(IllegalStateException e) {
			
		}
		it.close();

	}
	
	@Test
	@Ignore
	public void test20removedKeys() {
		assertEquals("valeur2", new String(store.get(testTable, "123456", "family1", "cle2")));
		Map<String, Set<String>> toBeDeletedKeysFamilies = new HashMap<String, Set<String>>();
		
		Set<String> toBeDeletedKeys = new TreeSet<String>();
		toBeDeletedKeys.add("cle2");
		toBeDeletedKeysFamilies.put("family1", toBeDeletedKeys);
		store.storeChanges(testTable, "123456", null, toBeDeletedKeysFamilies, null);
		assertSame(null, store.get(testTable, "123456", "family1", "cle2"));

	}
	
	
	@Ignore
	public void test35increment() {
		assertEquals("42", new String(store.get(testTable, "123456", "family2", "cle-incr")));
		Map<String, Map<String, Number>> toBeIncrementedKeysFamilies = new HashMap<String, Map<String, Number>>();
		
		Map<String, Number> toBeIncrementedKeys = new HashMap<String, Number>();
		
		toBeIncrementedKeys.put("cle-incr", 10);
		toBeIncrementedKeysFamilies.put("family2", toBeIncrementedKeys);
		store.storeChanges(testTable, "123456", null, null, toBeIncrementedKeysFamilies);
		assertEquals("52", new String(store.get(testTable, "123456", "family2", "cle-incr")));

	}
	
	@Test
	public void test40getWithConstraintOnKeys() {
		assertEquals(4, store.get(testTable, "400000", "family1", new Constraint("a", "d")).size());
		Constraint c = null;
		assertEquals(4, store.get(testTable, "400000", "family1", c).size());

		assertEquals(0, store.get(testTable, "400000", "family1", new Constraint("zz", "zzz")).size());
		assertEquals("valeur21", new String(store.get(testTable, "400000", "family1", new Constraint("cle21", "d")).get("cle21")));
	}
	
}














