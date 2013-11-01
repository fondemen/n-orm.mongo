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
	private MongoStore mongoStore;

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
	public void connectTest()
		throws DatabaseNotReachedException
	{
		mongoStore = new MongoStore();
		assert props.getProperty("host") != null;
		assert props.getProperty("port") != null;
		mongoStore.setHost(props.getProperty("host"));
		mongoStore.setPort(Integer.parseInt(props.getProperty("port")));
		mongoStore.start();
	}
}
