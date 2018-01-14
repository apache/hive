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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.txn.TxnStore;

/**
 * An interface wrapper for HMSHandler.  This interface contains methods that need to be
 * called by internal classes but that are not part of the thrift interface.
 */
@InterfaceAudience.Private
public interface IHMSHandler extends ThriftHiveMetastore.Iface, Configurable {

  void init() throws MetaException;

  /**
   * Get the id of the thread of this handler.
   * @return thread id
   */
  int getThreadId();

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
   * @param name database name
   * @return database object
   * @throws NoSuchObjectException If the database does not exist.
   * @throws MetaException If another error occurs.
   */
  Database get_database_core(final String name) throws NoSuchObjectException, MetaException;

  /**
   * Equivalent of get_table, but does not log audits and fire pre-event listener.
   * Meant to be used for calls made by other hive classes, that are not using the
   * thrift interface.
   * @param dbname database name
   * @param name table name
   * @return Table object
   * @throws NoSuchObjectException If the table does not exist.
   * @throws MetaException  If another error occurs.
   */
  Table get_table_core(final String dbname, final String name) throws MetaException,
      NoSuchObjectException;

  /**
   * Get a list of all transactional listeners.
   * @return list of listeners.
   */
  List<TransactionalMetaStoreEventListener> getTransactionalListeners();
}
