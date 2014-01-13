package com.googlecode.n_orm.mongo;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.googlecode.n_orm.GenericTests;
import com.googlecode.n_orm.StoreTestLauncher;

@RunWith(Suite.class)
@SuiteClasses(GenericTests.class)
public class GenericTest {
	private static SimpleMongoStore store;
	private static String DBNAME          = "n_orm_test";
	private static final String testTable = "testtable";

	@BeforeClass public static void setupStore() {

		MongoStore ms = new MongoStore();
		ms.setDB(DBNAME);
		ms.start();
		ms.dropTable(testTable);

		StoreTestLauncher.INSTANCE = new MongoLauncher();
	}

}

