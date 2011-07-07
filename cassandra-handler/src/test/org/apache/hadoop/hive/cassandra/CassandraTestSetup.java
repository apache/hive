package org.apache.hadoop.hive.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.extensions.TestSetup;
import junit.framework.Test;

import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.cassandra.serde.StandardColumnSerDe;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

public class CassandraTestSetup extends TestSetup {

  static final Log LOG = LogFactory.getLog(CassandraTestSetup.class);
  private EmbeddedCassandraService cassandra;

  public CassandraTestSetup(Test test) {
    super(test);
  }

  @SuppressWarnings("deprecation")
  void preTest(HiveConf conf) throws Exception {
    if (cassandra == null) {
      CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
      cleaner.prepare();
      cassandra = new EmbeddedCassandraService();
      cassandra.start();

      // Make sure that this server is connectable.
      CassandraProxyClient client = new CassandraProxyClient(
          "127.0.0.1", 9170, true, true);

      client.getProxyConnection().describe_cluster_name();

      createTestCFWithData(client);
    }

    String auxJars = conf.getAuxJars();
    auxJars = ((auxJars == null) ? "" : (auxJars + ",")) + "file://"
        + new JobConf(conf, Cassandra.Client.class).getJar();
    auxJars += ",file://" + new JobConf(conf, ColumnFamilyInputFormat.class).getJar();
    auxJars += ",file://" + new JobConf(conf, StandardColumnSerDe.class).getJar();
    auxJars += ",file://" + new JobConf(conf, org.apache.thrift.transport.TSocket.class).getJar();
    auxJars += ",file://"
        + new JobConf(conf, com.google.common.collect.AbstractIterator.class).getJar();
    auxJars += ",file://" + new JobConf(conf, org.apache.commons.lang.ArrayUtils.class).getJar();
    auxJars += ",file://"
        + new JobConf(conf, org.apache.thrift.meta_data.FieldValueMetaData.class).getJar();
    conf.setAuxJars(auxJars);

    System.err.println(auxJars);

  }

  /**
   * Insert some test data for cassandra external table mapping.
   *
   * @throws Exception
   */
  private void createTestCFWithData(CassandraProxyClient client) throws Exception {
    KsDef ks = new KsDef();
    String keyspace = "ks_demo";
    String cfName = "cf_demo";
    ks.setName(keyspace);
    ks.setStrategy_class("org.apache.cassandra.locator.SimpleStrategy");
    Map<String, String> strategy_options = new HashMap<String, String> ();
    strategy_options.put("replication_factor", "1");
    ks.setStrategy_options(strategy_options);

    CfDef cf = new CfDef();
    cf.setKeyspace(keyspace);
    cf.setName(cfName);

    String utfType = "UTF8Type";
    cf.setDefault_validation_class(utfType);
    cf.setKey_validation_class(utfType);

    List<ColumnDef> columns = new ArrayList<ColumnDef>();

    ColumnDef uniqueid = new ColumnDef();
    String key1 = "uniqueid";
    uniqueid.setName(ByteBufferUtil.bytes(key1));
    uniqueid.setValidation_class("LexicalUUIDType");
    uniqueid.setIndex_type(IndexType.KEYS);
    columns.add(uniqueid);

    ColumnDef count = new ColumnDef();
    String key2 = "countLong";
    count.setName(ByteBufferUtil.bytes(key2));
    count.setValidation_class("LongType");
    count.setIndex_type(IndexType.KEYS);
    columns.add(count);

    ColumnDef intCol = new ColumnDef();
    String key3 = "countInt";
    intCol.setName(ByteBufferUtil.bytes(key3));
    intCol.setValidation_class("IntegerType");
    intCol.setIndex_type(IndexType.KEYS);
    columns.add(intCol);

    cf.setColumn_metadata(columns);

    ks.addToCf_defs(cf);
    client.getProxyConnection().system_add_keyspace(ks);
    client.getProxyConnection().set_keyspace(keyspace);

    //add data into this column family
    Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map = new HashMap<ByteBuffer,Map<String,List<Mutation>>>();

    long timestamp = System.currentTimeMillis();
    Map<String, List<Mutation>> map1 = new HashMap<String, List<Mutation>>();
    List<Mutation> mutationList = new ArrayList<Mutation>();
    Column cassCol = new Column();
    cassCol.setName(key1.getBytes());
    cassCol.setValue(LexicalUUIDType.instance.fromString("4fd1d3a0-a76d-11e0-0000-c6fa7f155dfe"));
    cassCol.setTimestamp(timestamp);
    ColumnOrSuperColumn thisCol = new ColumnOrSuperColumn();
    thisCol.setColumn(cassCol);
    Mutation mutation = new Mutation();
    mutation.setColumn_or_supercolumn(thisCol);

    mutationList.add(mutation);

    Column cassCol2 = new Column();
    cassCol2.setName(key2.getBytes());
    cassCol2.setValue(ByteBufferUtil.bytes((long)1223456));
    cassCol2.setTimestamp(timestamp);
    ColumnOrSuperColumn thisCol2 = new ColumnOrSuperColumn();
    thisCol2.setColumn(cassCol2);
    Mutation mutation2 = new Mutation();
    mutation2.setColumn_or_supercolumn(thisCol2);

    mutationList.add(mutation2);

    Column cassCol3 = new Column();
    cassCol3.setName(key3.getBytes());
    cassCol3.setValue(ByteBufferUtil.bytes((int)234));
    cassCol3.setTimestamp(timestamp);
    ColumnOrSuperColumn thisCol3 = new ColumnOrSuperColumn();
    thisCol3.setColumn(cassCol3);
    Mutation mutation3 = new Mutation();
    mutation3.setColumn_or_supercolumn(thisCol3);

    mutationList.add(mutation3);

    map1.put(cfName, mutationList);

    mutation_map.put(ByteBufferUtil.bytes("rowKey1"), map1);

    client.getProxyConnection().batch_mutate(mutation_map, ConsistencyLevel.ONE);
  }

  @Override
  protected void tearDown() throws Exception {
    // do we need this?
    CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
    cleaner.prepare();
  }

}
