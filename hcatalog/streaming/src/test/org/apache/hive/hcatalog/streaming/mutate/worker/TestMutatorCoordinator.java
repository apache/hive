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
package org.apache.hive.hcatalog.streaming.mutate.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hive.hcatalog.streaming.mutate.client.AcidTable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestMutatorCoordinator {

  private static final List<String> UNPARTITIONED = Collections.<String> emptyList();
  private static final List<String> PARTITION_B = Arrays.asList("B");
  private static final List<String> PARTITION_A = Arrays.asList("A");
  private static final long TRANSACTION_ID = 2L;
  private static final int BUCKET_ID = 0;
  private static final Path PATH_A = new Path("X");
  private static final Path PATH_B = new Path("B");
  private static final Object RECORD = "RECORD";
  private static final RecordIdentifier ROW__ID_B0_R0 = new RecordIdentifier(10L, BUCKET_ID, 0L);
  private static final RecordIdentifier ROW__ID_B0_R1 = new RecordIdentifier(10L, BUCKET_ID, 1L);
  private static final RecordIdentifier ROW__ID_B1_R0 = new RecordIdentifier(10L, BUCKET_ID + 1, 0L);
  private static final RecordIdentifier ROW__ID_INSERT = new RecordIdentifier(-1L, BUCKET_ID, -1L);

  @Mock
  private MutatorFactory mockMutatorFactory;
  @Mock
  private PartitionHelper mockPartitionHelper;
  @Mock
  private GroupingValidator mockGroupingValidator;
  @Mock
  private SequenceValidator mockSequenceValidator;
  @Mock
  private AcidTable mockAcidTable;
  @Mock
  private RecordInspector mockRecordInspector;
  @Mock
  private BucketIdResolver mockBucketIdResolver;
  @Mock
  private Mutator mockMutator;

  private MutatorCoordinator coordinator;

  private HiveConf configuration = new HiveConf();

  @Before
  public void createCoordinator() throws Exception {
    when(mockAcidTable.getOutputFormatName()).thenReturn(OrcOutputFormat.class.getName());
    when(mockAcidTable.getTotalBuckets()).thenReturn(1);
    when(mockAcidTable.getTransactionId()).thenReturn(TRANSACTION_ID);
    when(mockAcidTable.createPartitions()).thenReturn(true);
    when(mockMutatorFactory.newRecordInspector()).thenReturn(mockRecordInspector);
    when(mockMutatorFactory.newBucketIdResolver(anyInt())).thenReturn(mockBucketIdResolver);
    when(mockMutatorFactory.newMutator(any(OrcOutputFormat.class), anyLong(), any(Path.class), anyInt())).thenReturn(
        mockMutator);
    when(mockPartitionHelper.getPathForPartition(any(List.class))).thenReturn(PATH_A);
    when(mockRecordInspector.extractRecordIdentifier(RECORD)).thenReturn(ROW__ID_INSERT);
    when(mockSequenceValidator.isInSequence(any(RecordIdentifier.class))).thenReturn(true);
    when(mockGroupingValidator.isInSequence(any(List.class), anyInt())).thenReturn(true);

    coordinator = new MutatorCoordinator(configuration, mockMutatorFactory, mockPartitionHelper, mockGroupingValidator,
        mockSequenceValidator, mockAcidTable, false);
  }

  @Test
  public void insert() throws Exception {
    coordinator.insert(UNPARTITIONED, RECORD);

    verify(mockPartitionHelper).createPartitionIfNotExists(UNPARTITIONED);
    verify(mockMutatorFactory).newMutator(any(OrcOutputFormat.class), eq(TRANSACTION_ID), eq(PATH_A), eq(BUCKET_ID));
    verify(mockMutator).insert(RECORD);
  }

  @Test
  public void multipleInserts() throws Exception {
    coordinator.insert(UNPARTITIONED, RECORD);
    coordinator.insert(UNPARTITIONED, RECORD);
    coordinator.insert(UNPARTITIONED, RECORD);

    verify(mockPartitionHelper).createPartitionIfNotExists(UNPARTITIONED);
    verify(mockMutatorFactory).newMutator(any(OrcOutputFormat.class), eq(TRANSACTION_ID), eq(PATH_A), eq(BUCKET_ID));
    verify(mockMutator, times(3)).insert(RECORD);
  }

  @Test
  public void insertPartitionChanges() throws Exception {
    when(mockPartitionHelper.getPathForPartition(PARTITION_A)).thenReturn(PATH_A);
    when(mockPartitionHelper.getPathForPartition(PARTITION_B)).thenReturn(PATH_B);

    coordinator.insert(PARTITION_A, RECORD);
    coordinator.insert(PARTITION_B, RECORD);

    verify(mockPartitionHelper).createPartitionIfNotExists(PARTITION_A);
    verify(mockPartitionHelper).createPartitionIfNotExists(PARTITION_B);
    verify(mockMutatorFactory).newMutator(any(OrcOutputFormat.class), eq(TRANSACTION_ID), eq(PATH_A), eq(BUCKET_ID));
    verify(mockMutatorFactory).newMutator(any(OrcOutputFormat.class), eq(TRANSACTION_ID), eq(PATH_B), eq(BUCKET_ID));
    verify(mockMutator, times(2)).insert(RECORD);
  }

  @Test
  public void bucketChanges() throws Exception {
    when(mockRecordInspector.extractRecordIdentifier(RECORD)).thenReturn(ROW__ID_B0_R0, ROW__ID_B1_R0);

    when(mockBucketIdResolver.computeBucketId(RECORD)).thenReturn(0, 1);

    coordinator.update(UNPARTITIONED, RECORD);
    coordinator.delete(UNPARTITIONED, RECORD);

    verify(mockMutatorFactory).newMutator(any(OrcOutputFormat.class), eq(TRANSACTION_ID), eq(PATH_A), eq(BUCKET_ID));
    verify(mockMutatorFactory)
        .newMutator(any(OrcOutputFormat.class), eq(TRANSACTION_ID), eq(PATH_A), eq(BUCKET_ID + 1));
    verify(mockMutator).update(RECORD);
    verify(mockMutator).delete(RECORD);
  }

  @Test
  public void partitionThenBucketChanges() throws Exception {
    when(mockRecordInspector.extractRecordIdentifier(RECORD)).thenReturn(ROW__ID_B0_R0, ROW__ID_B0_R1, ROW__ID_B1_R0,
        ROW__ID_INSERT);

    when(mockBucketIdResolver.computeBucketId(RECORD)).thenReturn(0, 0, 1, 0);

    when(mockPartitionHelper.getPathForPartition(PARTITION_A)).thenReturn(PATH_A);
    when(mockPartitionHelper.getPathForPartition(PARTITION_B)).thenReturn(PATH_B);

    coordinator.update(PARTITION_A, RECORD); /* PaB0 */
    coordinator.insert(PARTITION_B, RECORD); /* PbB0 */
    coordinator.delete(PARTITION_B, RECORD); /* PbB0 */
    coordinator.update(PARTITION_B, RECORD); /* PbB1 */

    verify(mockPartitionHelper).createPartitionIfNotExists(PARTITION_B);
    verify(mockMutatorFactory).newMutator(any(OrcOutputFormat.class), eq(TRANSACTION_ID), eq(PATH_A), eq(BUCKET_ID));
    verify(mockMutatorFactory, times(2)).newMutator(any(OrcOutputFormat.class), eq(TRANSACTION_ID), eq(PATH_B),
        eq(BUCKET_ID));
    verify(mockMutatorFactory)
        .newMutator(any(OrcOutputFormat.class), eq(TRANSACTION_ID), eq(PATH_B), eq(BUCKET_ID + 1));
    verify(mockMutator, times(2)).update(RECORD);
    verify(mockMutator).delete(RECORD);
    verify(mockMutator).insert(RECORD);
    verify(mockSequenceValidator, times(4)).reset();
  }

  @Test
  public void partitionThenBucketChangesNoCreateAsPartitionEstablished() throws Exception {
    when(mockRecordInspector.extractRecordIdentifier(RECORD)).thenReturn(ROW__ID_B0_R0, ROW__ID_INSERT);
    when(mockBucketIdResolver.computeBucketId(RECORD)).thenReturn(0, 0);
    when(mockPartitionHelper.getPathForPartition(PARTITION_B)).thenReturn(PATH_B);

    coordinator.delete(PARTITION_B, RECORD); /* PbB0 */
    coordinator.insert(PARTITION_B, RECORD); /* PbB0 */

    verify(mockPartitionHelper, never()).createPartitionIfNotExists(anyList());
  }

  @Test(expected = RecordSequenceException.class)
  public void outOfSequence() throws Exception {
    when(mockSequenceValidator.isInSequence(any(RecordIdentifier.class))).thenReturn(false);

    coordinator.update(UNPARTITIONED, RECORD);
    coordinator.delete(UNPARTITIONED, RECORD);

    verify(mockPartitionHelper).createPartitionIfNotExists(UNPARTITIONED);
    verify(mockMutatorFactory).newMutator(any(OrcOutputFormat.class), eq(TRANSACTION_ID), eq(PATH_A), eq(BUCKET_ID));
    verify(mockMutator).update(RECORD);
    verify(mockMutator).delete(RECORD);
  }

  @Test(expected = GroupRevisitedException.class)
  public void revisitGroup() throws Exception {
    when(mockGroupingValidator.isInSequence(any(List.class), anyInt())).thenReturn(false);

    coordinator.update(UNPARTITIONED, RECORD);
    coordinator.delete(UNPARTITIONED, RECORD);

    verify(mockPartitionHelper).createPartitionIfNotExists(UNPARTITIONED);
    verify(mockMutatorFactory).newMutator(any(OrcOutputFormat.class), eq(TRANSACTION_ID), eq(PATH_A), eq(BUCKET_ID));
    verify(mockMutator).update(RECORD);
    verify(mockMutator).delete(RECORD);
  }

  @Test(expected = BucketIdException.class)
  public void insertWithBadBucket() throws Exception {
    when(mockRecordInspector.extractRecordIdentifier(RECORD)).thenReturn(ROW__ID_B0_R0);

    when(mockBucketIdResolver.computeBucketId(RECORD)).thenReturn(1);

    coordinator.insert(UNPARTITIONED, RECORD);
  }

  @Test(expected = BucketIdException.class)
  public void updateWithBadBucket() throws Exception {
    when(mockRecordInspector.extractRecordIdentifier(RECORD)).thenReturn(ROW__ID_B0_R0);

    when(mockBucketIdResolver.computeBucketId(RECORD)).thenReturn(1);

    coordinator.update(UNPARTITIONED, RECORD);
  }

  @Test
  public void deleteWithBadBucket() throws Exception {
    when(mockRecordInspector.extractRecordIdentifier(RECORD)).thenReturn(ROW__ID_B0_R0);

    when(mockBucketIdResolver.computeBucketId(RECORD)).thenReturn(1);

    coordinator.delete(UNPARTITIONED, RECORD);
  }

  @Test
  public void closeNoRecords() throws Exception {
    coordinator.close();

    // No mutator created
    verifyZeroInteractions(mockMutator);
  }

  @Test
  public void closeUsedCoordinator() throws Exception {
    coordinator.insert(UNPARTITIONED, RECORD);
    coordinator.close();

    verify(mockMutator).close();
    verify(mockPartitionHelper).close();
  }
}
