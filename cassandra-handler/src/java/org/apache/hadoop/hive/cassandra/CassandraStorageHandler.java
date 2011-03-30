package org.apache.hadoop.hive.cassandra;

import java.util.Map;
import java.util.Properties;

import org.apache.cassandra.thrift.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cassandra.input.HiveCassandraStandardColumnInputFormat;
import org.apache.hadoop.hive.cassandra.output.HiveCassandraOutputFormat;
import org.apache.hadoop.hive.cassandra.serde.StandardColumnSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class CassandraStorageHandler implements HiveStorageHandler, HiveMetaHook {

  private FramedConnWrapper wrap;

  private Configuration configuration;

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    Properties tableProperties = tableDesc.getProperties();

    // Try parsing the keyspace.columnFamily
    String tableName = tableProperties.getProperty(Constants.META_TABLE_NAME);
    String dbName = tableProperties.getProperty(Constants.META_TABLE_DB);

    String keyspace = tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_KEYSPACE_NAME);
    String columnFamily = tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_CF_NAME);
    String columnInfo = tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_COL_MAPPING);

    //Identify Keyspace
    if (keyspace == null) {
        keyspace = dbName;
    }

    jobProperties.put(StandardColumnSerDe.CASSANDRA_KEYSPACE_NAME, keyspace);

    //Identify ColumnFamily
    if (columnFamily == null) {
        columnFamily = tableName;
    }

    jobProperties.put(StandardColumnSerDe.CASSANDRA_CF_NAME, columnFamily);

    //Identify ColumnInfo
    if(columnInfo == null) {
      columnInfo = StandardColumnSerDe.createColumnMappingString(tableProperties.getProperty(org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS));
    }

    jobProperties.put(StandardColumnSerDe.CASSANDRA_COL_MAPPING, columnInfo);

    jobProperties.put(StandardColumnSerDe.CASSANDRA_HOST,
        tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_HOST, "localhost"));

    jobProperties.put(StandardColumnSerDe.CASSANDRA_PORT,
        tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_PORT, "9160"));

    jobProperties.put(StandardColumnSerDe.CASSANDRA_PARTITIONER,
        tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_PARTITIONER,
            "org.apache.cassandra.dht.RandomPartitioner"));

    jobProperties.put(StandardColumnSerDe.CASSANDRA_THRIFT_MODE,
        tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_THRIFT_MODE, "framed"));

    jobProperties.put(StandardColumnSerDe.CASSANDRA_CONSISTENCY_LEVEL,
        tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_CONSISTENCY_LEVEL, "ONE"));

    jobProperties.put(StandardColumnSerDe.CASSANDRA_RANGE_BATCH_SIZE,
        tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_RANGE_BATCH_SIZE, "1000"));

    jobProperties.put(StandardColumnSerDe.CASSANDRA_SLICE_PREDICATE_SIZE,
        tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_SLICE_PREDICATE_SIZE, "1000"));
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveCassandraStandardColumnInputFormat.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveCassandraOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return StandardColumnSerDe.class;
  }

  @Override
  public Configuration getConf() {
    return this.configuration;
  }

  @Override
  public void setConf(Configuration arg0) {
    this.configuration = arg0;
  }

  @Override
  public void preCreateTable(Table tbl) throws MetaException {
    boolean isExternal = MetaStoreUtils.isExternalTable(tbl);

    if (!isExternal) {
      throw new MetaException("Cassandra tables must be external.");
    }

    if (tbl.getSd().getLocation() != null) {
      throw new MetaException("LOCATION may not be specified for Cassandra.");
    }

    Map<String, String> serdeParam = tbl.getSd().getSerdeInfo().getParameters();

    String keyspace     = getCassandraKeyspace(tbl);
    String columnFamily = getCassandraColumnFamily(tbl);

    String cassandraHost = serdeParam.get(StandardColumnSerDe.CASSANDRA_HOST);
    if (cassandraHost == null) {
      cassandraHost = "localhost";
    }

    String cassandraPortStr = serdeParam.get(StandardColumnSerDe.CASSANDRA_PORT);
    if (cassandraPortStr == null) {
      cassandraPortStr = "9160";
    }

    int cassandraPort;
    try {
      cassandraPort = Integer.parseInt(cassandraPortStr);
    } catch (NumberFormatException e) {
      throw new MetaException(StandardColumnSerDe.CASSANDRA_PORT + " must be a number");
    }


    try {
      this.ensureConnection(cassandraHost, cassandraPort);
      try {
        KsDef ks = wrap.getClient().describe_keyspace(keyspace);

        boolean hasCf = false;
        for (CfDef cf : ks.getCf_defs()) {
          if (cf.getName().equalsIgnoreCase(columnFamily)) {
            hasCf = true;
          }
        }

        if (!hasCf) {
          throw new MetaException("columnFamily " + columnFamily + " does not exist");
        }

      } catch (NotFoundException ex) {
        throw new MetaException(keyspace
            + " not found. The storage handler will not create it. ");
      }
    } catch (TException e) {
      throw new MetaException("An internal exception prevented this action from taking place."
          + e.getMessage());
    } catch (InvalidRequestException e) {
      throw new MetaException("An internal exception prevented this action from taking place."
          + e.getMessage());
    }
  }

  private String getCassandraKeyspace(Table tbl) {
    String tableName = tbl.getParameters().get(StandardColumnSerDe.CASSANDRA_KEYSPACE_NAME);
    if (tableName == null) {
      tableName = tbl.getSd().getSerdeInfo().getParameters().get(
          StandardColumnSerDe.CASSANDRA_KEYSPACE_NAME);
    }
    if (tableName == null) {
      tableName = tbl.getDbName();
    }

    tbl.getParameters().put(StandardColumnSerDe.CASSANDRA_KEYSPACE_NAME, tableName);

    return tableName;
  }

  private String getCassandraColumnFamily(Table tbl) {
    String tableName = tbl.getParameters().get(StandardColumnSerDe.CASSANDRA_CF_NAME);

    if (tableName == null) {
      tableName = tbl.getSd().getSerdeInfo().getParameters().get(
          StandardColumnSerDe.CASSANDRA_CF_NAME);
    }

    if (tableName == null) {
      tableName = tbl.getTableName();
    }

    tbl.getParameters().put(StandardColumnSerDe.CASSANDRA_CF_NAME, tableName);

    return tableName;
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    // No work needed
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    // No work needed
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // nothing to do
  }

  public void ensureConnection(String host, int port) throws TTransportException {
    if (wrap == null) {
      wrap = new FramedConnWrapper(host, port, 5000);
      wrap.open();
    }
  }
}
