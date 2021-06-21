package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.DataConnector;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class DropDataConnectorEvent extends ListenerEvent {

  private final DataConnector connector;

  public DropDataConnectorEvent(DataConnector connector, boolean status, IHMSHandler handler) {
    super (status, handler);
    this.connector = connector;
  }

  /**
   * @return the connector
   */
  public DataConnector getDataConnector() {
    return connector;
  }

}
