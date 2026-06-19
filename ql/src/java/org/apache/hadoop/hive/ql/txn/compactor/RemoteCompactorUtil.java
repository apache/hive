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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.convertToGetPartitionsByNamesRequest;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

public class RemoteCompactorUtil {

  static final private String CLASS_NAME = RemoteCompactorUtil.class.getName();
  protected static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  public static Table resolveTable(HiveConf conf, IMetaStoreClient msc, CompactionInfo ci) throws MetaException {
    try {
      return msc.getTable(getDefaultCatalog(conf), ci.dbname, ci.tableName);
    } catch (TException e) {
      LOG.error("Unable to find table " + ci.getFullTableName(), e);
      throw new MetaException(e.toString());
    }
  }

  public static List<Partition> getPartitionsByNames(IMetaStoreClient msc, String dbName, String tableName,
      String partName) throws MetaException {
    try {
      GetPartitionsByNamesRequest req = convertToGetPartitionsByNamesRequest(dbName, tableName,
          Collections.singletonList(partName));
      return msc.getPartitionsByNames(req).getPartitions();
    } catch (TException e) {
      LOG.error("Unable to get partitions by name = {}.{}.{}", dbName, tableName, partName);
      throw new MetaException(e.toString());
    }
  }
}
