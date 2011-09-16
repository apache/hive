package org.apache.hadoop.hive.cassandra;

import java.util.Map;
import java.util.Properties;

import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cassandra.input.HiveCassandraStandardColumnInputFormat;
import org.apache.hadoop.hive.cassandra.output.HiveCassandraOutputFormat;
import org.apache.hadoop.hive.cassandra.serde.CassandraColumnSerDe;
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

public class CassandraStorageHandler
  implements HiveStorageHandler, HiveMetaHook {

  private Configuration configuration;

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    Properties tableProperties = tableDesc.getProperties();

    // Try parsing the keyspace.columnFamily
    String tableName = tableProperties.getProperty(Constants.META_TABLE_NAME);
    String dbName = tableProperties.getProperty(Constants.META_TABLE_DB);

    String keyspace = tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_KEYSPACE_NAME);
    String columnFamily = tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_CF_NAME);

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

    //If no column mapping has been configured, we should create the default column mapping.
    String columnInfo = tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_COL_MAPPING);
    if(columnInfo == null) {
      columnInfo = StandardColumnSerDe.createColumnMappingString(
        tableProperties.getProperty(org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS));
    }

    jobProperties.put(StandardColumnSerDe.CASSANDRA_COL_MAPPING, columnInfo);

    jobProperties.put(StandardColumnSerDe.CASSANDRA_HOST,
      tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_HOST, StandardColumnSerDe.DEFAULT_CASSANDRA_HOST));

    jobProperties.put(StandardColumnSerDe.CASSANDRA_PORT,
      tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_PORT, StandardColumnSerDe.DEFAULT_CASSANDRA_PORT));

    jobProperties.put(StandardColumnSerDe.CASSANDRA_PARTITIONER,
      tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_PARTITIONER,
        "org.apache.cassandra.dht.RandomPartitioner"));

    jobProperties.put(StandardColumnSerDe.CASSANDRA_THRIFT_MODE,
      tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_THRIFT_MODE, "framed"));

    jobProperties.put(StandardColumnSerDe.CASSANDRA_CONSISTENCY_LEVEL,
      tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_CONSISTENCY_LEVEL,
        StandardColumnSerDe.DEFAULT_CONSISTENCY_LEVEL));

    jobProperties.put(StandardColumnSerDe.CASSANDRA_RANGE_BATCH_SIZE,
      tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_RANGE_BATCH_SIZE,
        Integer.toString(StandardColumnSerDe.DEFAULT_RANGE_BATCH_SIZE)));

    jobProperties.put(StandardColumnSerDe.CASSANDRA_SLICE_PREDICATE_SIZE,
      tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_SLICE_PREDICATE_SIZE,
        Integer.toString(StandardColumnSerDe.DEFAULT_SLICE_PREDICATE_SIZE)));

    jobProperties.put(StandardColumnSerDe.CASSANDRA_SPLIT_SIZE,
      tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_SPLIT_SIZE,
          Integer.toString(StandardColumnSerDe.DEFAULT_SPLIT_SIZE)));


    jobProperties.put(StandardColumnSerDe.CASSANDRA_BATCH_MUTATION_SIZE,
        tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_BATCH_MUTATION_SIZE,
            Integer.toString(StandardColumnSerDe.DEFAULT_BATCH_MUTATION_SIZE)));

    jobProperties.put(StandardColumnSerDe.CASSANDRA_CF_COUNTERS,
            tableProperties.getProperty(StandardColumnSerDe.CASSANDRA_CF_COUNTERS, "false"));
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
    return CassandraColumnSerDe.class;
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
  public void preCreateTable(Table table) throws MetaException {
    boolean isExternal = MetaStoreUtils.isExternalTable(table);

    if (!isExternal) {
      throw new MetaException("Cassandra tables must be external.");
    }

    if (table.getSd().getLocation() != null) {
      throw new MetaException("LOCATION may not be specified for Cassandra.");
    }

    CassandraManager manager = new CassandraManager(table);

    try {
      //open connection to cassandra
      manager.openConnection();
      KsDef ks = manager.getKeyspaceDesc();

      //create the column family if it doesn't exist.
      manager.createCFIfNotFound(ks);
    } catch(NotFoundException e) {
      manager.createKeyspaceWithColumns();
    } finally {
      manager.closeConnection();
    }
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    // No work needed
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    //TODO: Should this be implemented to drop the table and its data from cassandra
    boolean isExternal = MetaStoreUtils.isExternalTable(table);
    if (deleteData && !isExternal) {
      CassandraManager manager = new CassandraManager(table);

      try {
        //open connection to cassandra
        manager.openConnection();
        //drop the table
        manager.dropTable();
      } finally {
        manager.closeConnection();
      }
    }
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // No work needed
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // nothing to do
  }
}
