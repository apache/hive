package org.apache.hadoop.hive.hwi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hwi.HWIAuth;
import org.apache.hadoop.hive.hwi.HWISessionItem;
import org.apache.hadoop.hive.hwi.HWISessionManager;
import org.apache.hadoop.hive.ql.history.HiveHistoryViewer;

public class TestHWISessionManager extends TestCase {

	private static String tableName = "test_hwi_table";

	private HiveConf conf;
	private Path dataFilePath;
	private HWISessionManager hsm;

	public TestHWISessionManager(String name) {
		super(name);
		conf = new HiveConf(TestHWISessionManager.class);
		String dataFileDir = conf.get("test.data.files").replace('\\', '/')
		.replace("c:", "");
		dataFilePath = new Path(dataFileDir, "kv1.txt");
	}

	protected void setUp() throws Exception {
		super.setUp();
		hsm = new HWISessionManager();
		Thread t = new Thread(hsm);
		t.start();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		hsm.setGoOn(false);
	}

	public final void testHiveDriver() throws Exception {
		// create a user
		HWIAuth user1 = new HWIAuth();
		user1.setUser("hadoop");
		user1.setGroups(new String[] { "hadoop" });

		// create two sessions for user
		HWISessionItem user1_item1 = hsm.createSession(user1, "session1");
		HWISessionItem user1_item2 = hsm.createSession(user1, "session2");

		// create second user
		HWIAuth user2 = new HWIAuth();
		user2.setUser("user2");
		user2.setGroups(new String[] { "user2" });

		// create one session for this user
		HWISessionItem user2_item1 = hsm.createSession(user2, "session1");

		// testing storage of sessions in HWISessionManager
		assertEquals(hsm.findAllSessionsForUser(user1).size(), 2);
		assertEquals(hsm.findAllSessionsForUser(user2).size(), 1);
		assertEquals(hsm.findAllSessionItems().size(), 3);

		HWISessionItem searchItem = hsm.findSessionItemByName(user1, "session1");
		assertEquals(searchItem, user1_item1);

		searchItem.setQuery("create table " + tableName
				+ " (key int, value string)");
		searchItem.clientStart();

		// wait for the session manager to make the table. It is non blocking API.
		while (searchItem.getStatus() != HWISessionItem.WebSessionItemStatus.QUERY_COMPLETE) {
			Thread.sleep(1);
		}
		assertEquals(searchItem.getQueryRet(), 0);

		// load data into table
		searchItem.clientRenew();
		searchItem.setQuery(("load data local inpath '" + dataFilePath.toString()
				+ "' into table " + tableName));
		searchItem.clientStart();
		while (searchItem.getStatus() != HWISessionItem.WebSessionItemStatus.QUERY_COMPLETE) {
			Thread.sleep(1);
		}
		assertEquals(searchItem.getQueryRet(), 0);

		// start two queries simultaniously
		user1_item2.setQuery("select distinct(test_hwi_table.key) from "
				+ tableName);
		user2_item1.setQuery("select distinct(test_hwi_table.key) from "
				+ tableName);

		// set result files to compare results
		File tmpdir = new File("/tmp/" + System.getProperty("user.name") + "/");
		if (tmpdir.exists() && !tmpdir.isDirectory()) {
			throw new RuntimeException(tmpdir + " exists but is not a directory");
		}

		if (!tmpdir.exists()) {
			if (!tmpdir.mkdirs()) {
				throw new RuntimeException("Could not make scratch directory " + tmpdir);
			}
		}

		File result1 = new File(tmpdir, "user1_item2");
		File result2 = new File(tmpdir, "user2_item1");
		user1_item2.setResultFile(result1.toString());
		user2_item1.setResultFile(result2.toString());
		user1_item2.setSSIsSilent(true);
		user2_item1.setSSIsSilent(true);

		user1_item2.clientStart();
		user2_item1.clientStart();

		while (user1_item2.getStatus() != HWISessionItem.WebSessionItemStatus.QUERY_COMPLETE) {
			Thread.sleep(1);
		}
		while (user2_item1.getStatus() != HWISessionItem.WebSessionItemStatus.QUERY_COMPLETE) {
			Thread.sleep(1);
		}

		assertEquals(user1_item2.getQueryRet(), 0);
		assertEquals(user2_item1.getQueryRet(), 0);
		assertEquals(isFileContentEqual(result1, result2), true);

		// clean up the files
		result1.delete();
		result2.delete();

		// test a session renew/refresh
		user2_item1.clientRenew();
		user2_item1.setQuery("select distinct(test_hwi_table.key) from "
				+ tableName);
		user2_item1.clientStart();
		while (user2_item1.getStatus() != HWISessionItem.WebSessionItemStatus.QUERY_COMPLETE) {
			Thread.sleep(1);
		}

		// cleanup
		HWISessionItem cleanup = hsm.createSession(user1, "cleanup");
		cleanup.setQuery("drop table " + tableName);
		cleanup.clientStart();

		while (cleanup.getStatus() != HWISessionItem.WebSessionItemStatus.QUERY_COMPLETE) {
			Thread.sleep(1);
		}

		// test the history is non null object.
		HiveHistoryViewer hhv = cleanup.getHistoryViewer();
		assertNotNull(hhv);
		assertEquals(cleanup.getQueryRet(), 0);
	}

	public boolean isFileContentEqual(File one, File two) throws Exception {
		if (one.exists() && two.exists()) {
			if (one.isFile() && two.isFile()) {
				if (one.length() == two.length()) {
					BufferedReader br1 = new BufferedReader(new FileReader(one));
					BufferedReader br2 = new BufferedReader(new FileReader(one));
					String line1 = null;
					String line2 = null;
					while ((line1 = br1.readLine()) != null) {
						line2 = br2.readLine();
						if (!line1.equals(line2)) {
							br1.close();
							br2.close();
							return false;
						}
					}
					br1.close();
					br2.close();
					return true;
				}
			}
		}
		return false;
	}
}
