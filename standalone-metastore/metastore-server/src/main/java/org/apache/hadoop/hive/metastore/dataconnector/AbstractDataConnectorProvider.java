package org.apache.hadoop.hive.metastore.dataconnector;

import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import java.net.ConnectException;
import java.util.List;

public abstract class AbstractDataConnectorProvider implements IDataConnectorProvider {
  protected String scoped_db = null;
  protected Object  handle = null;
  protected boolean isOpen = false;
  protected DataConnector connector = null;

  public AbstractDataConnectorProvider(String dbName, DataConnector connector) {
    this.scoped_db = dbName;
    this.connector = connector;
  }

  @Override public final void setScope(String scoped_db) {
    if (scoped_db != null)
      this.scoped_db = scoped_db;
  }

  /**
   * Opens a transport/connection to the datasource. Throws exception if the connection cannot be established.
   * @throws MetaException Throws MetaException if the connector does not have all info for a connection to be setup.
   * @throws ConnectException if the connection could not be established for some reason.
   */
  @Override public void open() throws ConnectException, MetaException {

  }

  /**
   * Closes a transport/connection to the datasource.
   * @throws ConnectException if the connection could not be closed.
   */
  @Override public void close() throws ConnectException {

  }

  /**
   * Returns Hive Table objects from the remote database for tables that match a name pattern.
   * @return List A collection of objects that match the name pattern, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   * @param regex
   */
  @Override public List<Table> getTables(String regex) throws MetaException {
    return null;
  }

  /**
   * Returns a list of all table names from the remote database.
   * @return List A collection of all the table names, null if there are no tables.
   * @throws MetaException To indicate any failures with executing this API
   */
  @Override public List<String> getTableNames() throws MetaException {
    return null;
  }

  /**
   * Fetch a single table with the given name, returns a Hive Table object from the remote database
   * @return Table A Table object for the matching table, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   * @param tableName
   */
  @Override public Table getTable(String tableName) throws MetaException {
    return null;
  }
}
