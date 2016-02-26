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

package org.apache.hive.hcatalog.streaming.mutate;

import static org.apache.hive.hcatalog.streaming.TransactionBatch.TxnState.ABORTED;
import static org.apache.hive.hcatalog.streaming.TransactionBatch.TxnState.COMMITTED;
import static org.apache.hive.hcatalog.streaming.mutate.StreamingTestUtils.databaseBuilder;
import static org.apache.hive.hcatalog.streaming.mutate.StreamingTestUtils.tableBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hive.hcatalog.streaming.TestStreaming;
import org.apache.hive.hcatalog.streaming.mutate.StreamingAssert.Factory;
import org.apache.hive.hcatalog.streaming.mutate.StreamingAssert.Record;
import org.apache.hive.hcatalog.streaming.mutate.StreamingTestUtils.TableBuilder;
import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClient;
import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClientBuilder;
import org.apache.hive.hcatalog.streaming.mutate.client.AcidTable;
import org.apache.hive.hcatalog.streaming.mutate.client.Transaction;
import org.apache.hive.hcatalog.streaming.mutate.worker.BucketIdResolver;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinator;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinatorBuilder;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * This test is based on {@link TestStreaming} and has a similar core set of tests to ensure that basic transactional
 * behaviour is as expected in the {@link RecordMutator} line. This is complemented with a set of tests related to the
 * use of update and delete operations.
 */
public class TestMutations {

  private static final List<String> EUROPE_FRANCE = Arrays.asList("Europe", "France");
  private static final List<String> EUROPE_UK = Arrays.asList("Europe", "UK");
  private static final List<String> ASIA_INDIA = Arrays.asList("Asia", "India");
  // id
  private static final int[] BUCKET_COLUMN_INDEXES = new int[] { 0 };
  private static final int RECORD_ID_COLUMN = 2;

  @Rule
  public TemporaryFolder warehouseFolder = new TemporaryFolder();

  private StreamingTestUtils testUtils = new StreamingTestUtils();
  private HiveConf conf;
  private IMetaStoreClient metaStoreClient;
  private String metaStoreUri;
  private Database database;
  private TableBuilder partitionedTableBuilder;
  private TableBuilder unpartitionedTableBuilder;
  private Factory assertionFactory;

  public TestMutations() throws Exception {
    conf = testUtils.newHiveConf(metaStoreUri);
    testUtils.prepareTransactionDatabase(conf);
    metaStoreClient = testUtils.newMetaStoreClient(conf);
    assertionFactory = new StreamingAssert.Factory(metaStoreClient, conf);
  }

  @Before
  public void setup() throws Exception {
    database = databaseBuilder(warehouseFolder.getRoot()).name("testing").dropAndCreate(metaStoreClient);

    partitionedTableBuilder = tableBuilder(database)
        .name("partitioned")
        .addColumn("id", "int")
        .addColumn("msg", "string")
        .partitionKeys("continent", "country")
        .bucketCols(Collections.singletonList("string"));

    unpartitionedTableBuilder = tableBuilder(database)
        .name("unpartitioned")
        .addColumn("id", "int")
        .addColumn("msg", "string")
        .bucketCols(Collections.singletonList("string"));
  }

  @Test
  public void testTransactionBatchEmptyCommitPartitioned() throws Exception {
    Table table = partitionedTableBuilder.addPartition(ASIA_INDIA).create(metaStoreClient);

    MutatorClient client = new MutatorClientBuilder()
        .addSinkTable(table.getDbName(), table.getTableName(), true)
        .metaStoreUri(metaStoreUri)
        .build();
    client.connect();

    Transaction transaction = client.newTransaction();

    transaction.begin();

    transaction.commit();
    assertThat(transaction.getState(), is(COMMITTED));
    client.close();
  }

  @Test
  public void testTransactionBatchEmptyCommitUnpartitioned() throws Exception {
    Table table = unpartitionedTableBuilder.create(metaStoreClient);

    MutatorClient client = new MutatorClientBuilder()
        .addSinkTable(table.getDbName(), table.getTableName(), false)
        .metaStoreUri(metaStoreUri)
        .build();
    client.connect();

    Transaction transaction = client.newTransaction();

    transaction.begin();

    transaction.commit();
    assertThat(transaction.getState(), is(COMMITTED));
    client.close();
  }

  @Test
  public void testTransactionBatchEmptyAbortPartitioned() throws Exception {
    Table table = partitionedTableBuilder.addPartition(ASIA_INDIA).create(metaStoreClient);

    MutatorClient client = new MutatorClientBuilder()
        .addSinkTable(table.getDbName(), table.getTableName(), true)
        .metaStoreUri(metaStoreUri)
        .build();
    client.connect();

    Transaction transaction = client.newTransaction();

    List<AcidTable> destinations = client.getTables();

    transaction.begin();

    MutatorFactory mutatorFactory = new ReflectiveMutatorFactory(conf, MutableRecord.class, RECORD_ID_COLUMN,
        BUCKET_COLUMN_INDEXES);
    MutatorCoordinator coordinator = new MutatorCoordinatorBuilder()
        .metaStoreUri(metaStoreUri)
        .table(destinations.get(0))
        .mutatorFactory(mutatorFactory)
        .build();

    coordinator.close();

    transaction.abort();
    assertThat(transaction.getState(), is(ABORTED));
    client.close();
  }

  @Test
  public void testTransactionBatchEmptyAbortUnartitioned() throws Exception {
    Table table = unpartitionedTableBuilder.create(metaStoreClient);

    MutatorClient client = new MutatorClientBuilder()
        .addSinkTable(table.getDbName(), table.getTableName(), false)
        .metaStoreUri(metaStoreUri)
        .build();
    client.connect();

    Transaction transaction = client.newTransaction();

    List<AcidTable> destinations = client.getTables();

    transaction.begin();

    MutatorFactory mutatorFactory = new ReflectiveMutatorFactory(conf, MutableRecord.class, RECORD_ID_COLUMN,
        BUCKET_COLUMN_INDEXES);
    MutatorCoordinator coordinator = new MutatorCoordinatorBuilder()
        .metaStoreUri(metaStoreUri)
        .table(destinations.get(0))
        .mutatorFactory(mutatorFactory)
        .build();

    coordinator.close();

    transaction.abort();
    assertThat(transaction.getState(), is(ABORTED));
    client.close();
  }

  @Test
  public void testTransactionBatchCommitPartitioned() throws Exception {
    Table table = partitionedTableBuilder.addPartition(ASIA_INDIA).create(metaStoreClient);

    MutatorClient client = new MutatorClientBuilder()
        .addSinkTable(table.getDbName(), table.getTableName(), true)
        .metaStoreUri(metaStoreUri)
        .build();
    client.connect();

    Transaction transaction = client.newTransaction();

    List<AcidTable> destinations = client.getTables();

    transaction.begin();

    MutatorFactory mutatorFactory = new ReflectiveMutatorFactory(conf, MutableRecord.class, RECORD_ID_COLUMN,
        BUCKET_COLUMN_INDEXES);
    MutatorCoordinator coordinator = new MutatorCoordinatorBuilder()
        .metaStoreUri(metaStoreUri)
        .table(destinations.get(0))
        .mutatorFactory(mutatorFactory)
        .build();

    BucketIdResolver bucketIdAppender = mutatorFactory.newBucketIdResolver(destinations.get(0).getTotalBuckets());
    MutableRecord record = (MutableRecord) bucketIdAppender.attachBucketIdToRecord(new MutableRecord(1,
        "Hello streaming"));
    coordinator.insert(ASIA_INDIA, record);
    coordinator.close();

    transaction.commit();

    StreamingAssert streamingAssertions = assertionFactory.newStreamingAssert(table, ASIA_INDIA);
    streamingAssertions.assertMinTransactionId(1L);
    streamingAssertions.assertMaxTransactionId(1L);
    streamingAssertions.assertExpectedFileCount(1);

    List<Record> readRecords = streamingAssertions.readRecords();
    assertThat(readRecords.size(), is(1));
    assertThat(readRecords.get(0).getRow(), is("{1, Hello streaming}"));
    assertThat(readRecords.get(0).getRecordIdentifier(), is(new RecordIdentifier(1L, 0, 0L)));

    assertThat(transaction.getState(), is(COMMITTED));
    client.close();
  }

  @Test
  public void testMulti() throws Exception {
    Table table = partitionedTableBuilder.addPartition(ASIA_INDIA).create(metaStoreClient);

    MutatorClient client = new MutatorClientBuilder()
        .addSinkTable(table.getDbName(), table.getTableName(), true)
        .metaStoreUri(metaStoreUri)
        .build();
    client.connect();

    Transaction transaction = client.newTransaction();

    List<AcidTable> destinations = client.getTables();

    transaction.begin();

    MutatorFactory mutatorFactory = new ReflectiveMutatorFactory(conf, MutableRecord.class, RECORD_ID_COLUMN,
        BUCKET_COLUMN_INDEXES);
    MutatorCoordinator coordinator = new MutatorCoordinatorBuilder()
        .metaStoreUri(metaStoreUri)
        .table(destinations.get(0))
        .mutatorFactory(mutatorFactory)
        .build();

    BucketIdResolver bucketIdResolver = mutatorFactory.newBucketIdResolver(destinations.get(0).getTotalBuckets());
    MutableRecord asiaIndiaRecord1 = (MutableRecord) bucketIdResolver.attachBucketIdToRecord(new MutableRecord(1,
        "Hello streaming"));
    MutableRecord europeUkRecord1 = (MutableRecord) bucketIdResolver.attachBucketIdToRecord(new MutableRecord(2,
        "Hello streaming"));
    MutableRecord europeFranceRecord1 = (MutableRecord) bucketIdResolver.attachBucketIdToRecord(new MutableRecord(3,
        "Hello streaming"));
    MutableRecord europeFranceRecord2 = (MutableRecord) bucketIdResolver.attachBucketIdToRecord(new MutableRecord(4,
        "Bonjour streaming"));

    coordinator.insert(ASIA_INDIA, asiaIndiaRecord1);
    coordinator.insert(EUROPE_UK, europeUkRecord1);
    coordinator.insert(EUROPE_FRANCE, europeFranceRecord1);
    coordinator.insert(EUROPE_FRANCE, europeFranceRecord2);
    coordinator.close();

    transaction.commit();

    // ASIA_INDIA
    StreamingAssert streamingAssertions = assertionFactory.newStreamingAssert(table, ASIA_INDIA);
    streamingAssertions.assertMinTransactionId(1L);
    streamingAssertions.assertMaxTransactionId(1L);
    streamingAssertions.assertExpectedFileCount(1);

    List<Record> readRecords = streamingAssertions.readRecords();
    assertThat(readRecords.size(), is(1));
    assertThat(readRecords.get(0).getRow(), is("{1, Hello streaming}"));
    assertThat(readRecords.get(0).getRecordIdentifier(), is(new RecordIdentifier(1L, 0, 0L)));

    // EUROPE_UK
    streamingAssertions = assertionFactory.newStreamingAssert(table, EUROPE_UK);
    streamingAssertions.assertMinTransactionId(1L);
    streamingAssertions.assertMaxTransactionId(1L);
    streamingAssertions.assertExpectedFileCount(1);

    readRecords = streamingAssertions.readRecords();
    assertThat(readRecords.size(), is(1));
    assertThat(readRecords.get(0).getRow(), is("{2, Hello streaming}"));
    assertThat(readRecords.get(0).getRecordIdentifier(), is(new RecordIdentifier(1L, 0, 0L)));

    // EUROPE_FRANCE
    streamingAssertions = assertionFactory.newStreamingAssert(table, EUROPE_FRANCE);
    streamingAssertions.assertMinTransactionId(1L);
    streamingAssertions.assertMaxTransactionId(1L);
    streamingAssertions.assertExpectedFileCount(1);

    readRecords = streamingAssertions.readRecords();
    assertThat(readRecords.size(), is(2));
    assertThat(readRecords.get(0).getRow(), is("{3, Hello streaming}"));
    assertThat(readRecords.get(0).getRecordIdentifier(), is(new RecordIdentifier(1L, 0, 0L)));
    assertThat(readRecords.get(1).getRow(), is("{4, Bonjour streaming}"));
    assertThat(readRecords.get(1).getRecordIdentifier(), is(new RecordIdentifier(1L, 0, 1L)));

    client.close();
  }

  @Test
  public void testTransactionBatchCommitUnpartitioned() throws Exception {
    Table table = unpartitionedTableBuilder.create(metaStoreClient);

    MutatorClient client = new MutatorClientBuilder()
        .addSinkTable(table.getDbName(), table.getTableName(), false)
        .metaStoreUri(metaStoreUri)
        .build();
    client.connect();

    Transaction transaction = client.newTransaction();

    List<AcidTable> destinations = client.getTables();

    transaction.begin();

    MutatorFactory mutatorFactory = new ReflectiveMutatorFactory(conf, MutableRecord.class, RECORD_ID_COLUMN,
        BUCKET_COLUMN_INDEXES);
    MutatorCoordinator coordinator = new MutatorCoordinatorBuilder()
        .metaStoreUri(metaStoreUri)
        .table(destinations.get(0))
        .mutatorFactory(mutatorFactory)
        .build();

    BucketIdResolver bucketIdResolver = mutatorFactory.newBucketIdResolver(destinations.get(0).getTotalBuckets());
    MutableRecord record = (MutableRecord) bucketIdResolver.attachBucketIdToRecord(new MutableRecord(1,
        "Hello streaming"));

    coordinator.insert(Collections.<String> emptyList(), record);
    coordinator.close();

    transaction.commit();

    StreamingAssert streamingAssertions = assertionFactory.newStreamingAssert(table);
    streamingAssertions.assertMinTransactionId(1L);
    streamingAssertions.assertMaxTransactionId(1L);
    streamingAssertions.assertExpectedFileCount(1);

    List<Record> readRecords = streamingAssertions.readRecords();
    assertThat(readRecords.size(), is(1));
    assertThat(readRecords.get(0).getRow(), is("{1, Hello streaming}"));
    assertThat(readRecords.get(0).getRecordIdentifier(), is(new RecordIdentifier(1L, 0, 0L)));

    assertThat(transaction.getState(), is(COMMITTED));
    client.close();
  }

  @Test
  public void testTransactionBatchAbort() throws Exception {
    Table table = partitionedTableBuilder.addPartition(ASIA_INDIA).create(metaStoreClient);

    MutatorClient client = new MutatorClientBuilder()
        .addSinkTable(table.getDbName(), table.getTableName(), true)
        .metaStoreUri(metaStoreUri)
        .build();
    client.connect();

    Transaction transaction = client.newTransaction();

    List<AcidTable> destinations = client.getTables();

    transaction.begin();

    MutatorFactory mutatorFactory = new ReflectiveMutatorFactory(conf, MutableRecord.class, RECORD_ID_COLUMN,
        BUCKET_COLUMN_INDEXES);
    MutatorCoordinator coordinator = new MutatorCoordinatorBuilder()
        .metaStoreUri(metaStoreUri)
        .table(destinations.get(0))
        .mutatorFactory(mutatorFactory)
        .build();

    BucketIdResolver bucketIdResolver = mutatorFactory.newBucketIdResolver(destinations.get(0).getTotalBuckets());
    MutableRecord record1 = (MutableRecord) bucketIdResolver.attachBucketIdToRecord(new MutableRecord(1,
        "Hello streaming"));
    MutableRecord record2 = (MutableRecord) bucketIdResolver.attachBucketIdToRecord(new MutableRecord(2,
        "Welcome to streaming"));

    coordinator.insert(ASIA_INDIA, record1);
    coordinator.insert(ASIA_INDIA, record2);
    coordinator.close();

    transaction.abort();

    assertThat(transaction.getState(), is(ABORTED));

    client.close();

    StreamingAssert streamingAssertions = assertionFactory.newStreamingAssert(table, ASIA_INDIA);
    streamingAssertions.assertNothingWritten();
  }

  @Test
  public void testUpdatesAndDeletes() throws Exception {
    // Set up some base data then stream some inserts/updates/deletes to a number of partitions
    MutatorFactory mutatorFactory = new ReflectiveMutatorFactory(conf, MutableRecord.class, RECORD_ID_COLUMN,
        BUCKET_COLUMN_INDEXES);

    // INSERT DATA
    //
    Table table = partitionedTableBuilder.addPartition(ASIA_INDIA).addPartition(EUROPE_FRANCE).create(metaStoreClient);

    MutatorClient client = new MutatorClientBuilder()
        .addSinkTable(table.getDbName(), table.getTableName(), true)
        .metaStoreUri(metaStoreUri)
        .build();
    client.connect();

    Transaction insertTransaction = client.newTransaction();

    List<AcidTable> destinations = client.getTables();

    insertTransaction.begin();

    MutatorCoordinator insertCoordinator = new MutatorCoordinatorBuilder()
        .metaStoreUri(metaStoreUri)
        .table(destinations.get(0))
        .mutatorFactory(mutatorFactory)
        .build();

    BucketIdResolver bucketIdResolver = mutatorFactory.newBucketIdResolver(destinations.get(0).getTotalBuckets());
    MutableRecord asiaIndiaRecord1 = (MutableRecord) bucketIdResolver.attachBucketIdToRecord(new MutableRecord(1,
        "Namaste streaming 1"));
    MutableRecord asiaIndiaRecord2 = (MutableRecord) bucketIdResolver.attachBucketIdToRecord(new MutableRecord(2,
        "Namaste streaming 2"));
    MutableRecord europeUkRecord1 = (MutableRecord) bucketIdResolver.attachBucketIdToRecord(new MutableRecord(3,
        "Hello streaming 1"));
    MutableRecord europeUkRecord2 = (MutableRecord) bucketIdResolver.attachBucketIdToRecord(new MutableRecord(4,
        "Hello streaming 2"));
    MutableRecord europeFranceRecord1 = (MutableRecord) bucketIdResolver.attachBucketIdToRecord(new MutableRecord(5,
        "Bonjour streaming 1"));
    MutableRecord europeFranceRecord2 = (MutableRecord) bucketIdResolver.attachBucketIdToRecord(new MutableRecord(6,
        "Bonjour streaming 2"));

    insertCoordinator.insert(ASIA_INDIA, asiaIndiaRecord1);
    insertCoordinator.insert(ASIA_INDIA, asiaIndiaRecord2);
    insertCoordinator.insert(EUROPE_UK, europeUkRecord1);
    insertCoordinator.insert(EUROPE_UK, europeUkRecord2);
    insertCoordinator.insert(EUROPE_FRANCE, europeFranceRecord1);
    insertCoordinator.insert(EUROPE_FRANCE, europeFranceRecord2);
    insertCoordinator.close();

    insertTransaction.commit();

    assertThat(insertTransaction.getState(), is(COMMITTED));
    client.close();

    // MUTATE DATA
    //
    client = new MutatorClientBuilder()
        .addSinkTable(table.getDbName(), table.getTableName(), true)
        .metaStoreUri(metaStoreUri)
        .build();
    client.connect();

    Transaction mutateTransaction = client.newTransaction();

    destinations = client.getTables();

    mutateTransaction.begin();

    MutatorCoordinator mutateCoordinator = new MutatorCoordinatorBuilder()
        .metaStoreUri(metaStoreUri)
        .table(destinations.get(0))
        .mutatorFactory(mutatorFactory)
        .build();

    bucketIdResolver = mutatorFactory.newBucketIdResolver(destinations.get(0).getTotalBuckets());
    MutableRecord asiaIndiaRecord3 = (MutableRecord) bucketIdResolver.attachBucketIdToRecord(new MutableRecord(20,
        "Namaste streaming 3"));

    mutateCoordinator.update(ASIA_INDIA, new MutableRecord(2, "UPDATED: Namaste streaming 2", new RecordIdentifier(1L,
        0, 1L)));
    mutateCoordinator.insert(ASIA_INDIA, asiaIndiaRecord3);
    mutateCoordinator.delete(EUROPE_UK, new MutableRecord(3, "Hello streaming 1", new RecordIdentifier(1L, 0, 0L)));
    mutateCoordinator.delete(EUROPE_FRANCE,
        new MutableRecord(5, "Bonjour streaming 1", new RecordIdentifier(1L, 0, 0L)));
    mutateCoordinator.update(EUROPE_FRANCE, new MutableRecord(6, "UPDATED: Bonjour streaming 2", new RecordIdentifier(
        1L, 0, 1L)));
    mutateCoordinator.close();

    mutateTransaction.commit();

    assertThat(mutateTransaction.getState(), is(COMMITTED));

    StreamingAssert indiaAssertions = assertionFactory.newStreamingAssert(table, ASIA_INDIA);
    indiaAssertions.assertMinTransactionId(1L);
    indiaAssertions.assertMaxTransactionId(2L);
    List<Record> indiaRecords = indiaAssertions.readRecords();
    assertThat(indiaRecords.size(), is(3));
    assertThat(indiaRecords.get(0).getRow(), is("{1, Namaste streaming 1}"));
    assertThat(indiaRecords.get(0).getRecordIdentifier(), is(new RecordIdentifier(1L, 0, 0L)));
    assertThat(indiaRecords.get(1).getRow(), is("{2, UPDATED: Namaste streaming 2}"));
    assertThat(indiaRecords.get(1).getRecordIdentifier(), is(new RecordIdentifier(1L, 0, 1L)));
    assertThat(indiaRecords.get(2).getRow(), is("{20, Namaste streaming 3}"));
    assertThat(indiaRecords.get(2).getRecordIdentifier(), is(new RecordIdentifier(2L, 0, 0L)));

    StreamingAssert ukAssertions = assertionFactory.newStreamingAssert(table, EUROPE_UK);
    ukAssertions.assertMinTransactionId(1L);
    ukAssertions.assertMaxTransactionId(2L);
    List<Record> ukRecords = ukAssertions.readRecords();
    assertThat(ukRecords.size(), is(1));
    assertThat(ukRecords.get(0).getRow(), is("{4, Hello streaming 2}"));
    assertThat(ukRecords.get(0).getRecordIdentifier(), is(new RecordIdentifier(1L, 0, 1L)));

    StreamingAssert franceAssertions = assertionFactory.newStreamingAssert(table, EUROPE_FRANCE);
    franceAssertions.assertMinTransactionId(1L);
    franceAssertions.assertMaxTransactionId(2L);
    List<Record> franceRecords = franceAssertions.readRecords();
    assertThat(franceRecords.size(), is(1));
    assertThat(franceRecords.get(0).getRow(), is("{6, UPDATED: Bonjour streaming 2}"));
    assertThat(franceRecords.get(0).getRecordIdentifier(), is(new RecordIdentifier(1L, 0, 1L)));

    client.close();
  }
  
}
