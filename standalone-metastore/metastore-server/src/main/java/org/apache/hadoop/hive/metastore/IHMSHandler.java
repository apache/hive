/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.AbortCompactResponse;
import org.apache.hadoop.hive.metastore.api.AbortCompactionRequest;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.thrift.TException;

/**
 * An interface wrapper for HMSHandler.  This interface contains methods that need to be
 * called by internal classes but that are not part of the thrift interface.
 */
@InterfaceAudience.Private
public interface IHMSHandler extends ThriftHiveMetastore.Iface, Configurable {

  void init() throws MetaException;

  /**
   * Get a reference to the underlying RawStore.
   * @return the RawStore instance.
   * @throws MetaException if the creation of a new RawStore object is necessary but fails.
   */
  RawStore getMS() throws MetaException;

  /**
   * Get a reference to the underlying TxnStore.
   * @return the TxnStore instance.
   */
  TxnStore getTxnHandler();

  /**
   * Get a reference to Hive's warehouse object (the class that does all the physical operations).
   * @return Warehouse instance.
   */
  Warehouse getWh();

  /**
   * Equivalent to get_database, but does not write to audit logs, or fire pre-event listeners.
   * Meant to be used for internal hive classes that don't use the thrift interface.
   * @param catName catalog name
   * @param name database name
   * @return database object
   * @throws NoSuchObjectException If the database does not exist.
   * @throws MetaException If another error occurs.
   */
  Database get_database_core(final String catName, final String name)
      throws NoSuchObjectException, MetaException;

  /**
   * Equivalent of get_table, but does not log audits and fire pre-event listener.
   * Meant to be used for calls made by other hive classes, that are not using the
   * thrift interface.
   * @param catName catalog name
   * @param dbname database name
   * @param name table name
   * @return Table object
   * @throws NoSuchObjectException If the table does not exist.
   * @throws MetaException  If another error occurs.
   */
  @Deprecated
  Table get_table_core(final String catName, final String dbname, final String name)
      throws MetaException, NoSuchObjectException;
  @Deprecated
  Table get_table_core(final String catName, final String dbname,
                       final String name,
                       final String writeIdList)
      throws MetaException, NoSuchObjectException;

  /**
   *
   * @param getTableRequest request object to query table in HMS
   * @return Table Object
   * @throws NoSuchObjectException If the table does not exist.
   * @throws MetaException  If another error occurs
   */
  Table get_table_core(final GetTableRequest getTableRequest)
      throws MetaException, NoSuchObjectException;

  /**
   * Get a list of all transactional listeners.
   * @return list of listeners.
   */
  List<TransactionalMetaStoreEventListener> getTransactionalListeners();

  /**
   * Get a list of all non-transactional listeners.
   * @return list of non-transactional listeners.
   */
  List<MetaStoreEventListener> getListeners();

  /**
   * Equivalent to get_connector, but does not write to audit logs, or fire pre-event listeners.
   * Meant to be used for internal hive classes that don't use the thrift interface.
   * @param name connector name
   * @return DataConnector object
   * @throws NoSuchObjectException If the connector does not exist.
   * @throws MetaException If another error occurs.
   */
  DataConnector get_dataconnector_core(final String name)
      throws NoSuchObjectException, MetaException;

    AbortCompactResponse abort_Compactions(AbortCompactionRequest rqst) throws TException;
}
