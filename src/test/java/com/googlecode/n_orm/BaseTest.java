import java.util.Properties;
import java.io.IOException;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import static org.junit.Assert.*;

import com.googlecode.n_orm.mongo.Store;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.DatabaseNotReachedException;

public class BaseTest {

	private Store mongoStore;
	private Properties props;

	@Before
	public void loadProps()
	{
		try {
			props = StoreSelector.getInstance().findProperties(Store.class);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		assertEquals(props.getProperty("class"), Store.class.getName());
	}


	@Test
	public void connectTest()
		throws DatabaseNotReachedException
	{
		mongoStore = new Store();
		assert props.getProperty("host") != null;
		assert props.getProperty("port") != null;
		mongoStore.setHost(props.getProperty("host"));
		mongoStore.setPort(Integer.parseInt(props.getProperty("port")));
		mongoStore.start();
	}
}
