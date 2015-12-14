/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate;

import java.util.List;

import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClient;
import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClientBuilder;
import org.apache.hive.hcatalog.streaming.mutate.client.AcidTable;
import org.apache.hive.hcatalog.streaming.mutate.client.Transaction;
import org.apache.hive.hcatalog.streaming.mutate.worker.BucketIdResolver;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinator;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinatorBuilder;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorFactory;

public class ExampleUseCase {

  private String metaStoreUri;
  private String databaseName;
  private String tableName;
  private boolean createPartitions = true;
  private List<String> partitionValues1, partitionValues2, partitionValues3;
  private Object record1, record2, record3;
  private MutatorFactory mutatorFactory;

  /* This is an illustration, not a functioning example. */ 
  public void example() throws Exception {
    // CLIENT/TOOL END
    //
    // Singleton instance in the job client

    // Create a client to manage our transaction
    MutatorClient client = new MutatorClientBuilder()
        .addSinkTable(databaseName, tableName, createPartitions)
        .metaStoreUri(metaStoreUri)
        .build();

    // Get the transaction
    Transaction transaction = client.newTransaction();

    // Get serializable details of the destination tables
    List<AcidTable> tables = client.getTables();

    transaction.begin();

    // CLUSTER / WORKER END
    //
    // Job submitted to the cluster
    // 

    BucketIdResolver bucketIdResolver = mutatorFactory.newBucketIdResolver(tables.get(0).getTotalBuckets());
    record1 = bucketIdResolver.attachBucketIdToRecord(record1);

    // --------------------------------------------------------------
    // DATA SHOULD GET SORTED BY YOUR ETL/MERGE PROCESS HERE
    //
    // Group the data by (partitionValues, ROW__ID.bucketId)
    // Order the groups by (ROW__ID.lastTransactionId, ROW__ID.rowId)
    // --------------------------------------------------------------
    
    // One of these runs at the output of each reducer
    //
    MutatorCoordinator coordinator = new MutatorCoordinatorBuilder()
        .metaStoreUri(metaStoreUri)
        .table(tables.get(0))
        .mutatorFactory(mutatorFactory)
        .build();
    
    coordinator.insert(partitionValues1, record1);
    coordinator.update(partitionValues2, record2);
    coordinator.delete(partitionValues3, record3);

    coordinator.close();

    // CLIENT/TOOL END
    //
    // The tasks have completed, control is back at the tool

    transaction.commit();

    client.close();
  }

}
