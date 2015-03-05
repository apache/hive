package org.apache.hadoop.hive.metastore.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A pass through to a simple HBase connection.  This has no transactions.
 */
public class VanillaHBaseConnection implements HBaseConnection {
  static final private Log LOG = LogFactory.getLog(VanillaHBaseConnection.class.getName());


  private HConnection conn;
  private Map<String, HTableInterface> tables;
  Configuration conf;

  VanillaHBaseConnection() {
  }

  @Override
  public void connect() throws IOException {
    tables = new HashMap<String, HTableInterface>();
    if (conf == null) throw new RuntimeException("Must call getConf before connect");
    conn = HConnectionManager.createConnection(conf);
  }

  @Override
  public void close() throws IOException {
    for (HTableInterface htab : tables.values()) htab.close();
  }

  @Override
  public void beginTransaction() throws IOException {

  }

  @Override
  public void commitTransaction() throws IOException {

  }

  @Override
  public void rollbackTransaction() throws IOException {

  }

  @Override
  public void createHBaseTable(String tableName, List<byte[]> columnFamilies) throws
      IOException {
    HBaseAdmin admin = new HBaseAdmin(conn);
    LOG.info("Creating HBase table " + tableName);
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
    for (byte[] cf : columnFamilies) {
      tableDesc.addFamily(new HColumnDescriptor(cf));
    }
    admin.createTable(tableDesc);
    admin.close();
  }

  @Override
  public HTableInterface getHBaseTable(String tableName) throws IOException {
    return getHBaseTable(tableName, false);
  }

  @Override
  public HTableInterface getHBaseTable(String tableName, boolean force) throws IOException {
    HTableInterface htab = tables.get(tableName);
    if (htab == null) {
      LOG.debug("Trying to connect to table " + tableName);
      try {
        htab = conn.getTable(tableName);
        // Calling gettable doesn't actually connect to the region server, it's very light
        // weight, so call something else so we actually reach out and touch the region server
        // and see if the table is there.
        if (force) htab.get(new Get("nosuchkey".getBytes(HBaseUtils.ENCODING)));
      } catch (IOException e) {
        LOG.info("Caught exception when table was missing");
        return null;
      }
      htab.setAutoFlushTo(false);
      tables.put(tableName, htab);
    }
    return htab;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
