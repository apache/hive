package org.apache.hadoop.hive.service;

import java.util.*;

import org.apache.hadoop.fs.Path;
import junit.framework.TestCase;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveServer;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TTransport;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;

public class TestHiveServer extends TestCase {

  private HiveInterface client;
  private final static String host = "localhost";
  private final static int port = 10000;
  private Path dataFilePath;

  private static String tableName = "testhivedrivertable";
  private HiveConf conf;
  private boolean standAloneServer = false;
  private TTransport transport;

  public TestHiveServer(String name) {
    super(name);
    conf = new HiveConf(TestHiveServer.class);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");
    // See data/conf/hive-site.xml
    String paramStr = System.getProperty("test.service.standalone.server");
    if (paramStr != null && paramStr.equals("true"))
      standAloneServer = true;
  }

  protected void setUp() throws Exception {
    super.setUp();
    if (standAloneServer) {
      try {
        transport = new TSocket(host, port);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new HiveClient(protocol);
        transport.open();
      }
      catch (Throwable e) {
        e.printStackTrace();
      }
    }
    else {
      client = new HiveServer.HiveServerHandler();
    }
  }

  protected void tearDown() throws Exception {
    super.tearDown();
    if (standAloneServer) {
      transport.close();
    }
  }

  public void testExecute() throws Exception {
    try {
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }

    try {
      client.execute("create table " + tableName + " (num int)");
      client.execute("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);
      client.execute("select count(1) as cnt from " + tableName);
      String row = client.fetchOne();
      assertEquals(row, "500");
      assertEquals("struct result { string cnt}#cnt#string", client.getSchema());
      client.execute("drop table " + tableName);
    }
    catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public void notestExecute() throws Exception {
    try {
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }

    client.execute("create table " + tableName + " (num int)");
    client.execute("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);
    client.execute("select count(1) from " + tableName);
    String row = client.fetchOne();
    assertEquals(row, "500");
    client.execute("drop table " + tableName);
    transport.close();
  }

  /**
   * Test metastore call
   */
  public void testMetastore() throws Exception {
    try {
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }

    client.execute("create table " + tableName + " (num int)");
    List<String> tabs = client.get_tables("default", tableName);
    assertEquals(tabs.get(0), tableName);
    client.execute("drop table " + tableName);
  }

  /** 
   *
   */
  public void testFetch() throws Exception {
    // create and populate a table with 500 rows.
    try {
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }
    client.execute("create table " + tableName + " (key int, value string)");
    client.execute("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);

    try {
    // fetchAll test
    client.execute("select key, value from " + tableName);
    assertEquals(client.fetchAll().size(), 500);
    assertEquals(client.fetchAll().size(), 0);

    // fetchOne test
    client.execute("select key, value from " + tableName);
    for (int i = 0; i < 500; i++) {
      String str = client.fetchOne();
      if (str.equals("")) {
        assertTrue(false);
      }
    }
    assertEquals(client.fetchOne(), "");

    // fetchN test
    client.execute("select key, value from " + tableName);
    assertEquals(client.fetchN(499).size(), 499);
    assertEquals(client.fetchN(499).size(), 1);
    assertEquals(client.fetchN(499).size(), 0);
    }
    catch (Throwable e) {
      e.printStackTrace();
    }
  }

  public void testDynamicSerde() throws Exception {
    try {
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }

    client.execute("create table " + tableName + " (key int, value string)");
    client.execute("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);
    //client.execute("select key, count(1) from " + tableName + " where key > 10 group by key");
    String sql = "select key, value from " + tableName + " where key > 10";
    client.execute(sql);

    // Instantiate DynamicSerDe
    DynamicSerDe ds = new DynamicSerDe();
    Properties dsp = new Properties();
    dsp.setProperty(Constants.SERIALIZATION_FORMAT, org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol.class.getName());
    dsp.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME, "result");
    dsp.setProperty(Constants.SERIALIZATION_DDL, client.getSchema().split("#")[0]);
    dsp.setProperty(Constants.SERIALIZATION_LIB, ds.getClass().toString());
    dsp.setProperty(Constants.FIELD_DELIM, "9");
    ds.initialize(new Configuration(), dsp);

    String row = client.fetchOne();
    Object o = ds.deserialize(new BytesWritable(row.getBytes()));

    assertEquals(o.getClass().toString(), "class java.util.ArrayList");
    List<?> lst = (List<?>)o;
    assertEquals(lst.get(0), "238");

    // TODO: serde doesn't like underscore  -- struct result { string _c0}
    sql = "select count(1) as c from " + tableName;
    client.execute(sql);
    row = client.fetchOne();
    dsp.setProperty(Constants.SERIALIZATION_DDL, client.getSchema().split("#")[0]);
    // Need a new DynamicSerDe instance - re-initialization is not supported.
    ds = new DynamicSerDe();
    ds.initialize(new Configuration(), dsp);
    o = ds.deserialize(new BytesWritable(row.getBytes()));
  }

}
