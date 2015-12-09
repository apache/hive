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

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.hcatalog.streaming.mutate.client.AcidTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the application of an ordered sequence of mutation events to a given ACID table. Events must be grouped
 * by partition, then bucket and ordered by origTxnId, then rowId. Ordering is enforced by the {@link SequenceValidator}
 * and grouping is by the {@link GroupingValidator}. An acid delta file is created for each combination partition, and
 * bucket id (a single transaction id is implied). Once a delta file has been closed it cannot be reopened. Therefore
 * care is needed as to group the data correctly otherwise failures will occur if a delta belonging to group has been
 * previously closed. The {@link MutatorCoordinator} will seamlessly handle transitions between groups, creating and
 * closing {@link Mutator Mutators} as needed to write to the appropriate partition and bucket. New partitions will be
 * created in the meta store if {@link AcidTable#createPartitions()} is set.
 * <p/>
 * {@link #insert(List, Object) Insert} events must be artificially assigned appropriate bucket ids in the preceding
 * grouping phase so that they are grouped correctly. Note that any transaction id or row id assigned to the
 * {@link RecordIdentifier RecordIdentifier} of such events will be ignored by both the coordinator and the underlying
 * {@link RecordUpdater}.
 */
public class MutatorCoordinator implements Closeable, Flushable {

  private static final Logger LOG = LoggerFactory.getLogger(MutatorCoordinator.class);

  private final MutatorFactory mutatorFactory;
  private final GroupingValidator groupingValidator;
  private final SequenceValidator sequenceValidator;
  private final AcidTable table;
  private final RecordInspector recordInspector;
  private final PartitionHelper partitionHelper;
  private final AcidOutputFormat<?, ?> outputFormat;
  private final BucketIdResolver bucketIdResolver;
  private final HiveConf configuration;
  private final boolean deleteDeltaIfExists;

  private int bucketId;
  private List<String> partitionValues;
  private Path partitionPath;
  private Mutator mutator;

  MutatorCoordinator(HiveConf configuration, MutatorFactory mutatorFactory, PartitionHelper partitionHelper,
      AcidTable table, boolean deleteDeltaIfExists) throws WorkerException {
    this(configuration, mutatorFactory, partitionHelper, new GroupingValidator(), new SequenceValidator(), table,
        deleteDeltaIfExists);
  }

  /** Visible for testing only. */
  MutatorCoordinator(HiveConf configuration, MutatorFactory mutatorFactory, PartitionHelper partitionHelper,
      GroupingValidator groupingValidator, SequenceValidator sequenceValidator, AcidTable table,
      boolean deleteDeltaIfExists) throws WorkerException {
    this.configuration = configuration;
    this.mutatorFactory = mutatorFactory;
    this.partitionHelper = partitionHelper;
    this.groupingValidator = groupingValidator;
    this.sequenceValidator = sequenceValidator;
    this.table = table;
    this.deleteDeltaIfExists = deleteDeltaIfExists;
    this.recordInspector = this.mutatorFactory.newRecordInspector();
    bucketIdResolver = this.mutatorFactory.newBucketIdResolver(table.getTotalBuckets());

    bucketId = -1;
    outputFormat = createOutputFormat(table.getOutputFormatName(), configuration);
  }

  /**
   * We expect records grouped by (partitionValues,bucketId) and ordered by (origTxnId,rowId).
   * 
   * @throws BucketIdException The bucket ID in the {@link RecordIdentifier} of the record does not match that computed
   *           using the values in the record's bucketed columns.
   * @throws RecordSequenceException The record was submitted that was not in the correct ascending (origTxnId, rowId)
   *           sequence.
   * @throws GroupRevisitedException If an event was submitted for a (partition, bucketId) combination that has already
   *           been closed.
   * @throws PartitionCreationException Could not create a new partition in the meta store.
   * @throws WorkerException
   */
  public void insert(List<String> partitionValues, Object record) throws WorkerException {
    reconfigureState(OperationType.INSERT, partitionValues, record);
    try {
      mutator.insert(record);
      LOG.debug("Inserted into partition={}, record={}", partitionValues, record);
    } catch (IOException e) {
      throw new WorkerException("Failed to insert record '" + record + " using mutator '" + mutator + "'.", e);
    }
  }

  /**
   * We expect records grouped by (partitionValues,bucketId) and ordered by (origTxnId,rowId).
   * 
   * @throws BucketIdException The bucket ID in the {@link RecordIdentifier} of the record does not match that computed
   *           using the values in the record's bucketed columns.
   * @throws RecordSequenceException The record was submitted that was not in the correct ascending (origTxnId, rowId)
   *           sequence.
   * @throws GroupRevisitedException If an event was submitted for a (partition, bucketId) combination that has already
   *           been closed.
   * @throws PartitionCreationException Could not create a new partition in the meta store.
   * @throws WorkerException
   */
  public void update(List<String> partitionValues, Object record) throws WorkerException {
    reconfigureState(OperationType.UPDATE, partitionValues, record);
    try {
      mutator.update(record);
      LOG.debug("Updated in partition={}, record={}", partitionValues, record);
    } catch (IOException e) {
      throw new WorkerException("Failed to update record '" + record + " using mutator '" + mutator + "'.", e);
    }
  }

  /**
   * We expect records grouped by (partitionValues,bucketId) and ordered by (origTxnId,rowId).
   * 
   * @throws BucketIdException The bucket ID in the {@link RecordIdentifier} of the record does not match that computed
   *           using the values in the record's bucketed columns.
   * @throws RecordSequenceException The record was submitted that was not in the correct ascending (origTxnId, rowId)
   *           sequence.
   * @throws GroupRevisitedException If an event was submitted for a (partition, bucketId) combination that has already
   *           been closed.
   * @throws PartitionCreationException Could not create a new partition in the meta store.
   * @throws WorkerException
   */
  public void delete(List<String> partitionValues, Object record) throws WorkerException {
    reconfigureState(OperationType.DELETE, partitionValues, record);
    try {
      mutator.delete(record);
      LOG.debug("Deleted from partition={}, record={}", partitionValues, record);
    } catch (IOException e) {
      throw new WorkerException("Failed to delete record '" + record + " using mutator '" + mutator + "'.", e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (mutator != null) {
        mutator.close();
      }
    } finally {
      partitionHelper.close();
    }
  }

  @Override
  public void flush() throws IOException {
    if (mutator != null) {
      mutator.flush();
    }
  }

  private void reconfigureState(OperationType operationType, List<String> newPartitionValues, Object record)
    throws WorkerException {
    RecordIdentifier newRecordIdentifier = extractRecordIdentifier(operationType, newPartitionValues, record);
    int newBucketId = newRecordIdentifier.getBucketId();

    if (newPartitionValues == null) {
      newPartitionValues = Collections.emptyList();
    }

    try {
      if (partitionHasChanged(newPartitionValues)) {
        if (table.createPartitions() && operationType == OperationType.INSERT) {
          partitionHelper.createPartitionIfNotExists(newPartitionValues);
        }
        Path newPartitionPath = partitionHelper.getPathForPartition(newPartitionValues);
        resetMutator(newBucketId, newPartitionValues, newPartitionPath);
      } else if (bucketIdHasChanged(newBucketId)) {
        resetMutator(newBucketId, partitionValues, partitionPath);
      } else {
        validateRecordSequence(operationType, newRecordIdentifier);
      }
    } catch (IOException e) {
      throw new WorkerException("Failed to reset mutator when performing " + operationType + " of record: " + record, e);
    }
  }

  private RecordIdentifier extractRecordIdentifier(OperationType operationType, List<String> newPartitionValues,
      Object record) throws BucketIdException {
    RecordIdentifier recordIdentifier = recordInspector.extractRecordIdentifier(record);
    int computedBucketId = bucketIdResolver.computeBucketId(record);
    if (operationType != OperationType.DELETE && recordIdentifier.getBucketId() != computedBucketId) {
      throw new BucketIdException("RecordIdentifier.bucketId != computed bucketId (" + computedBucketId
          + ") for record " + recordIdentifier + " in partition " + newPartitionValues + ".");
    }
    return recordIdentifier;
  }

  private void resetMutator(int newBucketId, List<String> newPartitionValues, Path newPartitionPath)
    throws IOException, GroupRevisitedException {
    if (mutator != null) {
      mutator.close();
    }
    validateGrouping(newPartitionValues, newBucketId);
    sequenceValidator.reset();
    if (deleteDeltaIfExists) {
      // TODO: Should this be the concern of the mutator?
      deleteDeltaIfExists(newPartitionPath, table.getTransactionId(), newBucketId);
    }
    mutator = mutatorFactory.newMutator(outputFormat, table.getTransactionId(), newPartitionPath, newBucketId);
    bucketId = newBucketId;
    partitionValues = newPartitionValues;
    partitionPath = newPartitionPath;
    LOG.debug("Reset mutator: bucketId={}, partition={}, partitionPath={}", bucketId, partitionValues, partitionPath);
  }

  private boolean partitionHasChanged(List<String> newPartitionValues) {
    boolean partitionHasChanged = !Objects.equals(this.partitionValues, newPartitionValues);
    if (partitionHasChanged) {
      LOG.debug("Partition changed from={}, to={}", this.partitionValues, newPartitionValues);
    }
    return partitionHasChanged;
  }

  private boolean bucketIdHasChanged(int newBucketId) {
    boolean bucketIdHasChanged = this.bucketId != newBucketId;
    if (bucketIdHasChanged) {
      LOG.debug("Bucket ID changed from={}, to={}", this.bucketId, newBucketId);
    }
    return bucketIdHasChanged;
  }

  private void validateGrouping(List<String> newPartitionValues, int newBucketId) throws GroupRevisitedException {
    if (!groupingValidator.isInSequence(newPartitionValues, bucketId)) {
      throw new GroupRevisitedException("Group out of sequence: state=" + groupingValidator + ", partition="
          + newPartitionValues + ", bucketId=" + newBucketId);
    }
  }

  private void validateRecordSequence(OperationType operationType, RecordIdentifier newRecordIdentifier)
    throws RecordSequenceException {
    boolean identiferOutOfSequence = operationType != OperationType.INSERT
        && !sequenceValidator.isInSequence(newRecordIdentifier);
    if (identiferOutOfSequence) {
      throw new RecordSequenceException("Records not in sequence: state=" + sequenceValidator + ", recordIdentifier="
          + newRecordIdentifier);
    }
  }

  @SuppressWarnings("unchecked")
  private AcidOutputFormat<?, ?> createOutputFormat(String outputFormatName, HiveConf configuration)
    throws WorkerException {
    try {
      return (AcidOutputFormat<?, ?>) ReflectionUtils.newInstance(JavaUtils.loadClass(outputFormatName), configuration);
    } catch (ClassNotFoundException e) {
      throw new WorkerException("Could not locate class for '" + outputFormatName + "'.", e);
    }
  }

  /* A delta may be present from a previous failed task attempt. */
  private void deleteDeltaIfExists(Path partitionPath, long transactionId, int bucketId) throws IOException {
    Path deltaPath = AcidUtils.createFilename(partitionPath,
        new AcidOutputFormat.Options(configuration)
            .bucket(bucketId)
            .minimumTransactionId(transactionId)
            .maximumTransactionId(transactionId));
    FileSystem fileSystem = deltaPath.getFileSystem(configuration);
    if (fileSystem.exists(deltaPath)) {
      LOG.info("Deleting existing delta path: {}", deltaPath);
      fileSystem.delete(deltaPath, false);
    }
  }

}
