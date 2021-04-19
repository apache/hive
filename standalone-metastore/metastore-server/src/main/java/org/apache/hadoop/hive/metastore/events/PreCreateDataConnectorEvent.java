package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.DataConnector;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class PreCreateDataConnectorEvent extends PreEventContext {
    private final DataConnector connector;

    public PreCreateDataConnectorEvent(DataConnector connector, IHMSHandler hmsHandler) {
      super (PreEventType.CREATE_DATACONNECTOR, hmsHandler);
      this.connector = connector;
    }

    /**
     * @return the connector
     */
    public DataConnector getDataConnector () {
      return connector;
    }
}
