package org.apache.hadoop.hive.metastore.dataconnector;

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DatabaseType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.dataconnector.IDataConnectorProvider.*;

public class DataConnectorProviderFactory {
  Logger LOG = LoggerFactory.getLogger(DataConnectorProviderFactory.class);

  private static Map<String, IDataConnectorProvider> cache = null;
  private static DataConnectorProviderFactory singleton = null;
  private static IHMSHandler hmsHandler = null;

  private DataConnectorProviderFactory(IHMSHandler hmsHandler) {
    cache = new HashMap<String, IDataConnectorProvider>();
    this.hmsHandler = hmsHandler;
  }

  public static synchronized DataConnectorProviderFactory getInstance(IHMSHandler hmsHandler) {
    if (singleton == null) {
      singleton = new DataConnectorProviderFactory(hmsHandler);
    }
    return singleton;
  }

  public static synchronized IDataConnectorProvider getDataConnectorProvider(Database db) throws MetaException {
    IDataConnectorProvider provider = null;
    DataConnector connector = null;
    if (db.getType() == DatabaseType.NATIVE) {
      throw new MetaException("Database " + db.getName() + " is of type NATIVE, no connector available");
    }

    String scopedDb = (db.getRemote_dbname() != null) ? db.getRemote_dbname() : db.getName();
    if (cache.containsKey(db.getConnector_name().toLowerCase())) {
      provider = cache.get(db.getConnector_name().toLowerCase());
      if (provider != null) {
        provider.setScope(scopedDb);
      }
      return provider;
    }

    try {
      connector = hmsHandler.get_dataconnector_core(db.getConnector_name());
    } catch (NoSuchObjectException notexists) {
      throw new MetaException("Data connector " + db.getConnector_name() + " associated with database "
          + db.getName() + " does not exist");
    }
    String type = connector.getType();
    switch (type) {
    case DERBY_TYPE:
    case MSSQL_TYPE:
    case MYSQL_TYPE:
    case ORACLE_TYPE:
    case POSTGRES_TYPE:
      try {
        provider = JDBCConnectorProviderFactory.get(scopedDb, connector);
      } catch (Exception e) {
        throw new MetaException("Could not instantiate a provider for database " + db.getName());
      }
      break;
    default:
      throw new MetaException("Data connector of type " + connector.getType() + " not implemented yet");
    }
    cache.put(connector.getName().toLowerCase(), provider);
    return provider;
  }

  public void shutdown() {
    for (IDataConnectorProvider provider: cache.values()) {
      try {
        provider.close();
      } catch(Exception e) {
        LOG.warn("Exception invoking close on dataconnectorprovider:" + provider, e);
      } finally {
        cache.clear();
      }
    }
  }
}
