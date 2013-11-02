import java.util.Properties;
import java.io.IOException;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import static org.junit.Assert.*;

import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.mongo.MongoStore;
import com.googlecode.n_orm.DatabaseNotReachedException;


public class BaseTest {

	private Properties props;

	@Before
	public void loadProps()
	{
		try {
			props = StoreSelector.getInstance().findProperties(
				MongoStore.class
			);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		assertEquals(props.getProperty("class"), MongoStore.class.getName());
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
	public void connectTest()
	{
		MongoStore mongoStore = new MongoStore();

		assert props.getProperty("db")   != null;
		assert props.getProperty("host") != null;
		assert props.getProperty("port") != null;

		mongoStore.setDB(props.getProperty("db"));
		mongoStore.setHost(props.getProperty("host"));
		mongoStore.setPort(Integer.parseInt(props.getProperty("port")));

		mongoStore.start();
		mongoStore.close();
	}
}
