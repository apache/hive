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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Interface for Alter Table and Alter Partition code
 */
public interface AlterHandler extends Configurable {

  /**
   * @deprecated As of release 2.2.0. Replaced by {@link #alterTable(RawStore, Warehouse, String,
   * String, String, Table, EnvironmentContext, IHMSHandler, String)}
   *
   * handles alter table, the changes could be cascaded to partitions if applicable
   *
   * @param msdb
   *          object to get metadata
   * @param wh
   *          Hive Warehouse where table data is stored
   * @param catName
   *          catalog of the table being altered
   * @param dbname
   *          database of the table being altered
   * @param name
   *          original name of the table being altered. same as
   *          <i>newTable.tableName</i> if alter op is not a rename.
   * @param newTable
   *          new table object
   * @throws InvalidOperationException
   *           thrown if the newTable object is invalid
   * @throws MetaException
   *           thrown if there is any other error
   */
  @Deprecated
  default void alterTable(RawStore msdb, Warehouse wh, String catName, String dbname,
    String name, Table newTable, EnvironmentContext envContext)
      throws InvalidOperationException, MetaException {
    alterTable(msdb, wh, catName, dbname, name, newTable, envContext, null, null);
  }

  /**
   * handles alter table, the changes could be cascaded to partitions if applicable
   *
   * @param msdb
   *          object to get metadata
   * @param wh
   *          Hive Warehouse where table data is stored
   * @param catName catalog of the table being altered
   * @param dbname
   *          database of the table being altered
   * @param name
   *          original name of the table being altered. same as
   *          <i>newTable.tableName</i> if alter op is not a rename.
   * @param newTable
   *          new table object
   * @param handler
   *          HMSHandle object (required to log event notification)
   * @throws InvalidOperationException
   *           thrown if the newTable object is invalid
   * @throws MetaException
   *           thrown if there is any other error
   */
  void alterTable(RawStore msdb, Warehouse wh, String catName, String dbname,
      String name, Table newTable, EnvironmentContext envContext,
      IHMSHandler handler,  String writeIdList)
          throws InvalidOperationException, MetaException;

  /**
   * @deprecated As of release 2.2.0.  Replaced by {@link #alterPartitions(RawStore, Warehouse, String,
   * String, String, List, EnvironmentContext, String, long, IHMSHandler)}
   *
   * handles alter partition
   *
   * @param msdb
   *          object to get metadata
   * @param wh
   * @param dbname
   *          database of the partition being altered
   * @param name
   *          table of the partition being altered
   * @param part_vals
   *          original values of the partition being altered
   * @param new_part
   *          new partition object
   * @return the altered partition
   * @throws InvalidOperationException
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   */
  @Deprecated
  Partition alterPartition(final RawStore msdb, Warehouse wh, final String dbname,
    final String name, final List<String> part_vals, final Partition new_part,
    EnvironmentContext environmentContext)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException, MetaException, NoSuchObjectException;

  /**
   * handles alter partition
   *
   * @param msdb
   *          object to get metadata
   * @param wh physical warehouse class
   * @param catName catalog name
   * @param dbname
   *          database of the partition being altered
   * @param name
   *          table of the partition being altered
   * @param part_vals
   *          original values of the partition being altered
   * @param new_part
   *          new partition object
   * @param handler
   *          HMSHandle object (required to log event notification)
   * @return the altered partition
   * @throws InvalidOperationException
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   */
  Partition alterPartition(final RawStore msdb, Warehouse wh, final String catName,
                           final String dbname, final String name, final List<String> part_vals,
                           final Partition new_part, EnvironmentContext environmentContext,
                           IHMSHandler handler,  String validWriteIds)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException, MetaException, NoSuchObjectException;

  /**
   * @deprecated As of release 3.0.0. Replaced by {@link #alterPartitions(RawStore, Warehouse, String,
   * String, String, List, EnvironmentContext, String, long, IHMSHandler)}
   *
   * handles alter partitions
   *
   * @param msdb
   *          object to get metadata
   * @param wh
   * @param dbname
   *          database of the partition being altered
   * @param name
   *          table of the partition being altered
   * @param new_parts
   *          new partition list
   * @return the altered partition list
   * @throws InvalidOperationException
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   */
  @Deprecated
  List<Partition> alterPartitions(final RawStore msdb, Warehouse wh,
    final String dbname, final String name, final List<Partition> new_parts,
    EnvironmentContext environmentContext)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException, MetaException;

  /**
   * handles alter partitions
   *
   * @param msdb
   *          object to get metadata
   * @param wh
   * @param dbname
   *          database of the partition being altered
   * @param name
   *          table of the partition being altered
   * @param new_parts
   *          new partition list
   * @param handler
   *          HMSHandle object (required to log event notification)
   * @return the altered partition list
   * @throws InvalidOperationException
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   */
  List<Partition> alterPartitions(final RawStore msdb, Warehouse wh, final String catName,
    final String dbname, final String name, final List<Partition> new_parts,
    EnvironmentContext environmentContext,  String writeIdList, long writeId,
    IHMSHandler handler)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException, MetaException;
}
