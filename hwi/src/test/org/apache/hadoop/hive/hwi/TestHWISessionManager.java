package org.apache.hadoop.hive.hwi;
import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hwi.HWIAuth;
import org.apache.hadoop.hive.hwi.HWISessionItem;
import org.apache.hadoop.hive.hwi.HWISessionManager;

public class TestHWISessionManager extends TestCase {

  private static String tableName="test_hwi_table";
  
  private HiveConf conf;
  private Path dataFilePath;
  private HWISessionManager hsm; 

  public TestHWISessionManager(String name){
    super(name);
    conf = new HiveConf(TestHWISessionManager.class);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
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
	  //create a user
	  HWIAuth user1= new HWIAuth();
	  user1.setUser("hadoop");
	  user1.setGroups(new String[]{ "hadoop"});
	  
	  //create two sessions for user
	  HWISessionItem user1_item1 = hsm.createSession(user1, "session1");
	  HWISessionItem user1_item2 = hsm.createSession(user1, "session2");
	  
	  //create second user
	  HWIAuth user2= new HWIAuth();
	  user2.setUser("user2");
	  user2.setGroups(new String[]{ "user2"});
	  
	  //create one session for this user
	  HWISessionItem user2_item1 =hsm.createSession(user2, "session1");
	  
	  assertEquals( hsm.findAllSessionsForUser(user1).size(),2 );
	  assertEquals( hsm.findAllSessionsForUser(user2).size(),1 );
	  assertEquals( hsm.findAllSessionItems().size(),3);
	
	  HWISessionItem searchItem =hsm.findSessionItemByName(user1, "session1");
	  assertEquals( searchItem, user1_item1);
	  
	  searchItem.setQuery("create table " + tableName + " (key int, value string)");
	  searchItem.clientStart();
	  
	  //wait for the session manager to make the table it is non blocking api
	  while (searchItem.getStatus() != HWISessionItem.WebSessionItemStatus.QUERY_COMPLETE){
		  Thread.sleep(1);
	  }
	  assertEquals(searchItem.getQueryRet(),0 );
	  
	  //start two queries simultaniously
	  user1_item2.setQuery("select distinct(test_hwi_table.key) from "+tableName);
	  user2_item1.setQuery("select distinct(test_hwi_table.key) from "+tableName);
	  
	  user1_item2.clientStart();
	  user2_item1.clientStart();
	  
	  
	  while (user1_item2.getStatus() != HWISessionItem.WebSessionItemStatus.QUERY_COMPLETE ){ 
		  Thread.sleep(1);
	  }
	  while (user2_item1.getStatus() != HWISessionItem.WebSessionItemStatus.QUERY_COMPLETE ) {
		  Thread.sleep(1);
	  }
	  
	  assertEquals(user1_item2.getQueryRet(),0);
	  assertEquals(user2_item1.getQueryRet(),0);

          HWISessionItem cleanup = hsm.createSession(user1, "cleanup");
          cleanup.setQuery("drop table " + tableName);
          cleanup.clientStart();

          while (cleanup.getStatus() != HWISessionItem.WebSessionItemStatus.QUERY_COMPLETE ){
                  Thread.sleep(1);
          }

	  assertEquals(cleanup.getQueryRet(),0);
  }
}
