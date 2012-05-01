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

package org.apache.hadoop.hive.metastore.hooks;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.HiveAlterHandler;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;

/*
 * Subclass of HiveAlterHandler.  Checks that if the table, or partition's table has a
 * creation_cluster set, that cluster matches the current cluster where the metastore is running.
 * If so, or if the table or partition's table does not have creation_cluster set, it calls its
 * super classes implementation of the alter method, i.e. it behaves normally.  If not, it throws
 * a MetaException.
 */
public class FbhiveAlterHandler extends HiveAlterHandler {
  public static final Log LOG = LogFactory.getLog(FbhiveAlterHandler.class);

  @Override
  public Partition alterPartition(RawStore ms, Warehouse wh, String dbname,
      String name, List<String> part_vals, Partition new_part)
      throws InvalidOperationException, InvalidObjectException,
      AlreadyExistsException, MetaException {

    String exception = "Partition in table " + name + " cannot be altered.";
    checkTableCluster(ms, dbname, name, exception);

    return super.alterPartition(ms, wh, dbname, name, part_vals, new_part);
  }

  @Override
  public void alterTable(RawStore msdb, Warehouse wh, String dbname,
      String name, Table newt) throws InvalidOperationException, MetaException {

    String exception = "Table " + name + " cannot be altered.";
    checkTableCluster(msdb, dbname, name, exception);

    super.alterTable(msdb, wh, dbname, name, newt);
  }

  private void checkTableCluster(RawStore msdb, String dbName, String tableName,
      String exception) throws MetaException{

    Table oldt = msdb.getTable(dbName.toLowerCase(), tableName.toLowerCase());
    if (oldt != null) {
      String creationCluster = oldt.getParameters().get(HookUtils.TABLE_CREATION_CLUSTER);
      String currentCluster = hiveConf.get(FBHiveConf.FB_CURRENT_CLUSTER);
      if (creationCluster != null &&
          currentCluster != null &&
          !creationCluster.equals(currentCluster)) {
        throw new MetaException(exception +
            " Table's cluster is " + creationCluster + "," +
            " whereas current package is " + currentCluster);
      }
    }
  }
}
