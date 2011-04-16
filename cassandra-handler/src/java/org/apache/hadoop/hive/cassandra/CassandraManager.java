package org.apache.hadoop.hive.cassandra;

import java.util.Map;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.hadoop.hive.cassandra.serde.StandardColumnSerDe;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

/**
 * A class to handle the transaction to cassandra backend database.
 *
 */
public class CassandraManager {
  final static public int DEFAULT_REPLICATION_FACTOR = 1;
  final static public String DEFAULT_STRATEGY = "org.apache.cassandra.locator.SimpleStrategy";

  //Cassandra Host Name
  private final String host;

  //Cassandra Host Port
  private final int port;

  //TODO: Replace this with CassandraProxyClient.
  private FramedConnWrapper wrap;

  //table property
  private final Table tbl;

  //key space name
  private String keyspace;

  //column family name
  private String columnName;

  /**
   * Construct a cassandra manager object from meta table object.
   */
  public CassandraManager(Table tbl) throws MetaException {
    Map<String, String> serdeParam = tbl.getSd().getSerdeInfo().getParameters();

    String cassandraHost = serdeParam.get(StandardColumnSerDe.CASSANDRA_HOST);
    if (cassandraHost == null) {
      cassandraHost = "localhost";
    }

    this.host = cassandraHost;

    String cassandraPortStr = serdeParam.get(StandardColumnSerDe.CASSANDRA_PORT);
    if (cassandraPortStr == null) {
      port = 9160;
    } else {
      try {
        port = Integer.parseInt(cassandraPortStr);
      } catch (NumberFormatException e) {
        throw new MetaException(StandardColumnSerDe.CASSANDRA_PORT + " must be a number");
      }
    }

    this.tbl = tbl;
    init();
  }

  private void init() {
    this.keyspace = getCassandraKeyspace();
    this.columnName = getCassandraColumnFamily();
  }

  /**
   * Open connection to the cassandra server.
   *
   * @throws MetaException
   */
  public void openConnection() throws MetaException {
    wrap = new FramedConnWrapper(host, port, 5000);

    try {
      wrap.open();
    } catch (TTransportException e) {
      throw new MetaException("Unable to create connection to the cassandra server " + host + ":" + port);
    }
  }

  private void ensureConnection() throws MetaException {
    if (wrap == null) {
      openConnection();
    }
  }

  /**
   * Return a keyspace description for the given keyspace name from the cassandra host.
   *
   * @param keyspace keyspace name
   * @return keyspace description
   */
  public KsDef getKeyspaceDesc()
    throws NotFoundException, MetaException {
    try {
      ensureConnection();
      return wrap.getClient().describe_keyspace(keyspace);
    } catch (TException e) {
      throw new MetaException("An internal exception prevented this action from taking place."
          + e.getMessage());
    } catch (InvalidRequestException e) {
      throw new MetaException("An internal exception prevented this action from taking place."
          + e.getMessage());
    }
  }

  /**
   * Get Column family based on the configuration in the table.
   */
  private CfDef getColumnFamily(KsDef ks) {
    for (CfDef cf : ks.getCf_defs()) {
      if (cf.getName().equalsIgnoreCase(columnName)) {
        return cf;
      }
    }

    return null;
  }

  /**
   * Create a keyspace with columns defined in the table.
   */
  public KsDef createKeyspaceWithColumns()
    throws MetaException {
    try {
      KsDef ks = new KsDef();
      ks.setName(getCassandraKeyspace());
      ks.setReplication_factor(getReplicationFactor());
      ks.setStrategy_class(getStrategy());

      CfDef cf = new CfDef();
      cf.setKeyspace(keyspace);
      cf.setName(columnName);

      ks.addToCf_defs(cf);

      ensureConnection();
      wrap.getClient().system_add_keyspace(ks);
      return ks;
    } catch (TException e) {
      throw new MetaException("Unable to create key space '" + keyspace + "'. Error:"
          + e.getMessage());
    } catch (InvalidRequestException e) {
      throw new MetaException("Unable to create key space '" + keyspace + "'. Error:"
          + e.getMessage());
    }

  }

  /**
   * Create the column family if it doesn't exist.
   * @param ks
   * @return
   * @throws MetaException
   */
  public CfDef createCFIfNotFound(KsDef ks) throws MetaException {
    CfDef cf = getColumnFamily(ks);
    if (cf == null) {
      return createColumnFamily();
    } else {
      return cf;
    }
  }

  /**
   * Create column family based on the configuration in the table.
   */
  public CfDef createColumnFamily() throws MetaException {
    CfDef cf = new CfDef();
    cf.setKeyspace(keyspace);
    cf.setName(columnName);
    try {
      ensureConnection();
      wrap.getClient().system_add_column_family(cf);
      return cf;
    } catch (TException e) {
      throw new MetaException("Unable to create key space '" + keyspace + "'. Error:"
          + e.getMessage());
    } catch (InvalidRequestException e) {
      throw new MetaException("Unable to create key space '" + keyspace + "'. Error:"
          + e.getMessage());
    }
  }

  /**
   * Get replication factor from the table property.
   * @return replication factor
   * @throws MetaException error
   */
  private int getReplicationFactor() throws MetaException {
    String prop = getPropertyFromTable(StandardColumnSerDe.CASSANDRA_KEYSPACE_REPFACTOR);
    if (prop == null) {
      return DEFAULT_REPLICATION_FACTOR;
    } else {
      try {
        return Integer.parseInt(prop);
      } catch (NumberFormatException e) {
        throw new MetaException(StandardColumnSerDe.CASSANDRA_KEYSPACE_REPFACTOR + " must be a number");
      }
    }
  }

  /**
   * Get replication strategy from the table property.
   *
   * @return strategy
   */
  private String getStrategy() {
    String prop = getPropertyFromTable(StandardColumnSerDe.CASSANDRA_KEYSPACE_STRATEGY);
    if (prop == null) {
      return DEFAULT_STRATEGY;
    } else {
      return prop;
    }
  }

  /**
   * Get keyspace name from the table property.
   *
   * @return keyspace name
   */
  private String getCassandraKeyspace() {
    String tableName = getPropertyFromTable(StandardColumnSerDe.CASSANDRA_KEYSPACE_NAME);

    if (tableName == null) {
      tableName = tbl.getDbName();
    }

    tbl.getParameters().put(StandardColumnSerDe.CASSANDRA_KEYSPACE_NAME, tableName);

    return tableName;
  }

  /**
   * Get cassandra column family from table property.
   *
   * @return cassandra column family name
   */
  private String getCassandraColumnFamily() {
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

  /**
   * Get the value for a given name from the table.
   * It first check the table property. If it is not there, it will check the serde properties.
   *
   * @param columnName given name
   * @return value
   */
  private String getPropertyFromTable(String columnName) {
    String prop = tbl.getParameters().get(columnName);
    if (prop == null) {
      prop = tbl.getSd().getSerdeInfo().getParameters().get(columnName);
    }

    return prop;
  }

  /**
   * Close the connection.
   */
  public void closeConnection() {
    if (wrap != null) {
      wrap.close();
    }
  }

  /**
   * Drop the table defined in the query.
   */
  public void dropTable() throws MetaException {
    try {
      ensureConnection();
      wrap.getClient().system_drop_column_family(columnName);
    } catch (TException e) {
      throw new MetaException("Unable to create key space '" + keyspace + "'. Error:"
          + e.getMessage());
    } catch (InvalidRequestException e) {
      throw new MetaException("Unable to create key space '" + keyspace + "'. Error:"
          + e.getMessage());
    }
  }

}
