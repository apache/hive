package org.apache.cassandra.contrib.utils.service;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.hive.cassandra.CassandraProxyClient;

public class CassandraEmbeddedTestSetup {
  private final String KS = "ks_demo";
  private final String CF = "cf_demo";
  private final String SUPERKS = "super_ks_demo";
  private final String SUPERCF = "super_cf_demo";
  private final String COUNTERKS = "counter_ks_demo";
  private final String COUNTERCF = "counter_cf_demo";

  private final String UUID_STR = "uniqueid";
  private final ByteBuffer UUID_KEY = ByteBufferUtil.bytes(UUID_STR);
  private final String LONG_STR = "countLong";
  private final ByteBuffer LONG_KEY = ByteBufferUtil.bytes(LONG_STR);
  private final String INT_STR = "countInt";
  private final ByteBuffer INT_KEY = ByteBufferUtil.bytes(INT_STR);
  private final String UTF8_STR = "utf8";
  private final ByteBuffer UTF8_KEY = ByteBufferUtil.bytes(UTF8_STR);
  private final String COUNTER_LONG_STR = "counterColumnLong";
  private final ByteBuffer COUNTER_LONG_KEY = ByteBufferUtil.bytes(COUNTER_LONG_STR);

  CassandraDaemon cassandra;


  public CassandraEmbeddedTestSetup() throws Exception
  {

    CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
    cleaner.prepare();

    if (cassandra == null) {
      cassandra = new CassandraDaemon();
      cassandra.init(null);
      cassandra.start();

      // Make sure that this server is connectable.
      CassandraProxyClient client = new CassandraProxyClient(
          "127.0.0.1", 9170, true, true);

      client.getProxyConnection().describe_cluster_name();

      //create schema for a column family and insert some data into it
      createCFSchema(client);
      addCFData(client);

      //create schema for a super column family and insert some data into it
      createSuperCFSchema(client);
      addSuperCFData(client);

      // create for counter cf and add some data
      createCounterCFSchema(client);
      addCounterCFData(client);
    }
  }

  /**
   * Insert some test data for cassandra external table mapping.
   *
   * @throws Exception
   */
  private void createCFSchema(CassandraProxyClient client) throws Exception {
    KsDef ks = new KsDef();

    ks.setName(KS);
    ks.setStrategy_class("org.apache.cassandra.locator.SimpleStrategy");
    Map<String, String> strategy_options = new HashMap<String, String> ();
    strategy_options.put("replication_factor", "1");
    ks.setStrategy_options(strategy_options);

    CfDef cf = new CfDef();
    cf.setKeyspace(KS);
    cf.setName(CF);

    String utfType = "UTF8Type";
    cf.setDefault_validation_class(utfType);
    cf.setKey_validation_class(utfType);

    cf.setColumn_metadata(
        Arrays.asList(new ColumnDef(UUID_KEY, "LexicalUUIDType").
                          setIndex_type(IndexType.KEYS).
                          setIndex_name(UUID_STR),
                      new ColumnDef(LONG_KEY, "LongType").
                          setIndex_type(IndexType.KEYS).
                          setIndex_name(LONG_STR),
                      new ColumnDef(INT_KEY, "IntegerType").
                          setIndex_type(IndexType.KEYS).
                          setIndex_name(INT_STR)));

    ks.addToCf_defs(cf);
    client.getProxyConnection().system_add_keyspace(ks);
    client.getProxyConnection().set_keyspace(KS);
  }

  private void addCFData(CassandraProxyClient client) throws Exception {
    //add data into this column family
    Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map = new HashMap<ByteBuffer,Map<String,List<Mutation>>>();

    long timestamp = System.currentTimeMillis();
    Map<String, List<Mutation>> map1 = new HashMap<String, List<Mutation>>();
    List<Mutation> mutationList = new ArrayList<Mutation>();
    addColumnToMutation(mutationList,
      UUID_STR.getBytes(),
      LexicalUUIDType.instance.fromString("4fd1d3a0-a76d-11e0-0000-c6fa7f155dfe"),
      timestamp);

    addColumnToMutation(mutationList,
      LONG_STR.getBytes(),
      ByteBufferUtil.bytes((long)1223456),
      timestamp);

    addColumnToMutation(mutationList,
      INT_STR.getBytes(),
      ByteBufferUtil.bytes((int)234),
      timestamp);

    map1.put(CF, mutationList);

    mutation_map.put(ByteBufferUtil.bytes("rowKey1"), map1);

    client.getProxyConnection().batch_mutate(mutation_map, ConsistencyLevel.ONE);
  }

  private void addColumnToMutation(List<Mutation> mutationList, byte[] key, ByteBuffer value, long timestamp) {
    Column cassCol = new Column();
    cassCol.setName(key);
    cassCol.setValue(value);
    cassCol.setTimestamp(timestamp);
    ColumnOrSuperColumn thisCol = new ColumnOrSuperColumn();
    thisCol.setColumn(cassCol);
    Mutation mutation = new Mutation();
    mutation.setColumn_or_supercolumn(thisCol);

    mutationList.add(mutation);
  }

  /**
   * Insert some test data for cassandra external table mapping.
   *
   * @throws Exception
   */
  private void createSuperCFSchema(CassandraProxyClient client) throws Exception {
    KsDef ks = new KsDef();

    ks.setName(SUPERKS);
    ks.setStrategy_class("org.apache.cassandra.locator.SimpleStrategy");
    Map<String, String> strategy_options = new HashMap<String, String> ();
    strategy_options.put("replication_factor", "1");
    ks.setStrategy_options(strategy_options);

    CfDef cf = new CfDef();
    cf.setKeyspace(SUPERKS);
    cf.setName(SUPERCF);
    cf.setColumn_type("Super");

    ks.addToCf_defs(cf);
    client.getProxyConnection().system_add_keyspace(ks);
    client.getProxyConnection().set_keyspace(SUPERKS);
  }

  private void addSuperCFData(CassandraProxyClient client) throws Exception {
    //add data into this column family
    Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map = new HashMap<ByteBuffer,Map<String,List<Mutation>>>();

    long timestamp = System.currentTimeMillis();
    Map<String, List<Mutation>> map1 = new HashMap<String, List<Mutation>>();
    List<Mutation> mutationList = new ArrayList<Mutation>();

    addSuperColumnToMutation(mutationList,
      "key1".getBytes("utf-8"),
      timestamp);

    addSuperColumnToMutation(mutationList,
      "key2".getBytes("utf-8"),
      timestamp);

    map1.put(SUPERCF, mutationList);

    mutation_map.put(ByteBufferUtil.bytes("4fd1d3a0-a76d-11e0-0000-c6fa7f155dfe"), map1);

    client.getProxyConnection().batch_mutate(mutation_map, ConsistencyLevel.ONE);
  }

  private void addSuperColumnToMutation(List<Mutation> mutationList, byte[] subcolumn, long timestamp) throws Exception {
    SuperColumn superCol = new SuperColumn();
    superCol.setName(subcolumn);

    Column col = new Column();
    col.setName(LONG_STR.getBytes("utf-8"));
    col.setValue(ByteBufferUtil.bytes((long)1223456));
    col.setTimestamp(timestamp);

    superCol.addToColumns(col);

    Column col1 = new Column();
    col1.setName(UTF8_STR.getBytes("utf-8"));
    col1.setValue(ByteBufferUtil.bytes("utf8Test"));
    col1.setTimestamp(timestamp);

    superCol.addToColumns(col1);

    ColumnOrSuperColumn thisCol = new ColumnOrSuperColumn();
    thisCol.setSuper_column(superCol);
    Mutation mutation = new Mutation();
    mutation.setColumn_or_supercolumn(thisCol);

    mutationList.add(mutation);
  }

  /**
   * Insert some test data for counters external table mapping
   *
   * @throws Exception
   */
  private void createCounterCFSchema(CassandraProxyClient client) throws Exception {
    KsDef ks = new KsDef();

    ks.setName(COUNTERKS);
    ks.setStrategy_class("org.apache.cassandra.locator.SimpleStrategy");
    Map<String, String> strategy_options = new HashMap<String, String> ();
    strategy_options.put("replication_factor", "1");
    ks.setStrategy_options(strategy_options);

    CfDef cf = new CfDef();
    cf.setKeyspace(COUNTERKS);
    cf.setName(COUNTERCF);

    cf.setDefault_validation_class("CounterColumnType");
    cf.setKey_validation_class("UTF8Type");



    ks.addToCf_defs(cf);
    client.getProxyConnection().system_add_keyspace(ks);
    client.getProxyConnection().set_keyspace(COUNTERKS);
  }

  private void addCounterCFData(CassandraProxyClient client) throws Exception {
      //add data into this column family
      Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map = new HashMap<ByteBuffer,Map<String,List<Mutation>>>();
      Map<String, List<Mutation>> map1 = new HashMap<String, List<Mutation>>();
      List<Mutation> mutationList = new ArrayList<Mutation>();

      addCounterColumnToMutation(mutationList,
              COUNTER_LONG_STR.getBytes(),
              123456);

      map1.put(COUNTERCF, mutationList);

      mutation_map.put(ByteBufferUtil.bytes("counterRow1"), map1);
      mutation_map.put(ByteBufferUtil.bytes("counterRow2"), map1);

      client.getProxyConnection().batch_mutate(mutation_map, ConsistencyLevel.ONE);
  }

  private void addCounterColumnToMutation(List<Mutation> mutationList, byte[] key, long value) throws Exception {
      CounterColumn cassCol = new CounterColumn();
      cassCol.setName(key);
      cassCol.setValue(value);

      ColumnOrSuperColumn thisCol = new ColumnOrSuperColumn();
      thisCol.setCounter_column(cassCol);
      Mutation mutation = new Mutation();
      mutation.setColumn_or_supercolumn(thisCol);

      mutationList.add(mutation);
  }


  public void stop() throws Exception {
      if (cassandra != null)
      {
        cassandra.stop(null);
        cassandra = null;

        CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
        cleaner.prepare();
     }
  }

}
