/**
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
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Interface for Alter Table and Alter Partition code
 */
public interface AlterHandler extends Configurable {

  /**
   * handles alter table
   *
   * @param msdb
   *          object to get metadata
   * @param wh
   *          TODO
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
   *           thrown if there is any other erro
   */
  public abstract void alterTable(RawStore msdb, Warehouse wh, String dbname,
      String name, Table newTable) throws InvalidOperationException,
      MetaException;

  /**
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
  public abstract Partition alterPartition(final RawStore msdb, Warehouse wh, final String dbname,
      final String name, final List<String> part_vals, final Partition new_part)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException,
      MetaException;

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
   * @return the altered partition list
   * @throws InvalidOperationException
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   */
  public abstract List<Partition> alterPartitions(final RawStore msdb, Warehouse wh,
      final String dbname, final String name, final List<Partition> new_part)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException,
      MetaException;
}
