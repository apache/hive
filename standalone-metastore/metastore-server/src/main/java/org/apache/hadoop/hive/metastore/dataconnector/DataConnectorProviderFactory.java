package org.apache.hadoop.hive.metastore.dataconnector;

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DatabaseType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

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
    if (db.getType() == DatabaseType.NATIVE) {
      throw new MetaException("Database " + db.getName() + " is of type NATIVE, no connector available");
    }

    // DataConnector connector = hmsHandler.getMS().getDataConnector(db.getConnector_name());

    if (cache.containsKey(db.getConnector_name().toLowerCase() != null)) {
      provider = cache.get(db.getConnector_name().toLowerCase());

      if (provider != null) {
        provider.setScope(db.getName());
      }

      return provider;
    }

    // String type = connector.getType();
    String type = "mysql";
    switch (type) {
    case "mysql":

        ;
    default:
      ;
    }

    cache.put(db.getConnector_name().toLowerCase(), provider);
    return provider;
  }

  IDataConnectorProvider getDataConnectorProvider(String connectorName) {
    if (connectorName == null || connectorName.isEmpty()) {
      return null;
    }

    if (cache.containsKey(connectorName.toLowerCase() != null)) {
      return cache.get(connectorName.toLowerCase());
    }
    return null;
    // return getDataConnectorProvider();
  }

  public void shutdown() {
    for (IDataConnectorProvider provider: cache.values()) {
      try {
        provider.close();
      } catch(Exception e) {
        LOG.warn("Exception invoking close on dataconnectorprovider:" + provider, e);
      }
    }
  }
}
