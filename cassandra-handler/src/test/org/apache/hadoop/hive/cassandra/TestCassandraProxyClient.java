package org.apache.hadoop.hive.cassandra;

import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class TestCassandraProxyClient extends TestCase {
  Cassandra.Iface client;
  private EmbeddedCassandraService cassandra;
  private String ksName;
  private String cfName;

  /**
   * Make sure that when the server is down, proxy client will only try a certain amount of times and fails the request.
   * Please make sure to run this as the first test.
   *
   * @throws Exception
   */
  public void testServerDown() throws Exception {

    try {
      Cassandra.Iface client = (Cassandra.Iface) CassandraProxyClient.newProxyConnection(
          "127.0.0.1", 9170, true, true);
      client.describe_keyspaces();
      fail("Fail this test.");
    } catch (CassandraException e) {
      //As expected.
    }
  }

  /**
   * Start the embedded cassandra server if it is not up.
   *
   * @throws IOException
   * @throws TTransportException
   * @throws TException
   * @throws CassandraException
   */
  protected void startServer() throws IOException, TTransportException, TException, CassandraException {
    if (cassandra==null){
      CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
      cleaner.prepare();
      cassandra = new EmbeddedCassandraService();
      cassandra.start();
    }

    //Make sure that this server is connectable.
    client = (Cassandra.Iface) CassandraProxyClient.newProxyConnection(
        "127.0.0.1", 9170, true, true);
  }

  @Override
  protected void tearDown() throws Exception {
    //do we need this?
    CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
    cleaner.prepare();
  }

  public void testInsertionQuery() throws Exception {
    startServer();

    List<KsDef> keyspaces = client.describe_keyspaces();
    assertTrue(keyspaces.size() > 1);


    for (KsDef thisKs : keyspaces) {
      if (!thisKs.getName().equals("system")) {
        ksName = thisKs.getName();
        break;
      }
    }

    CfDef columnFamily = new CfDef();
    columnFamily.setKeyspace(ksName);
    cfName = "TestCassandra";
    columnFamily.setName(cfName);
    client.system_add_column_family(columnFamily);

    //add some data
    Column column = new Column(ByteBufferUtil.bytes("name"), ByteBufferUtil.bytes("value"), System.currentTimeMillis());
    client.insert(ByteBufferUtil.bytes("key1"), new ColumnParent(cfName), column, ConsistencyLevel.ALL);

    //query for the data
    ColumnPath path = new ColumnPath();
    path.setColumn_family(cfName);
    path.setColumn(ByteBufferUtil.bytes("name"));
    ColumnOrSuperColumn result = client.get(ByteBufferUtil.bytes("key1"), path, ConsistencyLevel.ALL);
    assertNotNull(result);
    assertEquals("name", new String(result.getColumn().getName()));
    assertEquals("value", new String(result.getColumn().getValue()));
  }

}
