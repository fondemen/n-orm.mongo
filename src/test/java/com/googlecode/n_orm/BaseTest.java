import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.io.IOException;

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
	public void addAndDeleteTest()
	{
		MongoStore mongoStore = new MongoStore();
		mongoStore.setDB(DBNAME);

		mongoStore.start();
		ColumnFamilyData data = new DefaultColumnFamilyData();
		
		HashMap<String, byte[]> col1 = new HashMap<String, byte[]>();
		HashMap<String, byte[]> col2 = new HashMap<String, byte[]>();

		col1.put("toto", new String("haha").getBytes());
		col1.put("tutu", new String("bebe").getBytes());

		col2.put("machin", new String("truc").getBytes());
		col2.put("bidule", new String("chouette").getBytes());

		data.put("1", col1);
		data.put("2", col2);

		mongoStore.insert(null, COLLECTION, "truc", data);
		//mongoStore.delete(null, COLLECTION, "truc");
		mongoStore.close();
	}
}
