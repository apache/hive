/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.thrift.TException;

/**
 * TableOperations that only writes metadata files during commit.
 * Skips HMS reads, locking, and persistence — all handled by the coordinator.
 */
public class StagingTableOperations extends HiveTableOperations {

  private String newMetadataLocation;

  public StagingTableOperations(
      Configuration conf,
      ClientPool<IMetaStoreClient, TException> metaClients,
      FileIO fileIO,
      String catalogName,
      String database,
      String table) {
    super(conf, metaClients, fileIO, catalogName, database, table);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    newMetadataLocation = writeNewMetadataIfRequired(base == null, metadata);
  }

  public String metadataLocation() {
    return newMetadataLocation;
  }
}
