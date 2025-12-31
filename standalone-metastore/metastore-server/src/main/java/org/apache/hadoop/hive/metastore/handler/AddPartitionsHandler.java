/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.metastore.handler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.canUpdateStats;

public class AddPartitionsHandler
    extends AbstractOperationHandler<AddPartitionsRequest, AddPartitionsHandler.AddPartitionsResult> {
  private String catName;
  private String dbName;
  private String tblName;
  private Warehouse wh;
  private Table table;
  private Database db;
  private static ExecutorService threadPool;

  AddPartitionsHandler(IHMSHandler handler, AddPartitionsRequest request) throws TException, IOException {
    super(handler, false, request);
  }

  @Override
  protected AddPartitionsResult execute() throws TException, IOException {
    EnvironmentContext envContext = request.getEnvironmentContext();
    boolean success = false;
    // Ensures that the list doesn't have dups, and keeps track of directories we have created.
    final Map<PartValEqWrapperLite, Boolean> addedPartitions = new ConcurrentHashMap<>();
    final List<Partition> newParts = new ArrayList<>();
    final List<Partition> existingParts = new ArrayList<>();
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    List<ColumnStatistics> partsColStats = new ArrayList<>();
    List<Long> partsWriteIds = new ArrayList<>();

    Lock tableLock = ((HMSHandler)handler).getTableLockFor(dbName, tblName);
    tableLock.lock();
    RawStore ms = handler.getMS();
    try {
      ms.openTransaction();
      Set<PartValEqWrapperLite> partsToAdd = new HashSet<>();
      List<FieldSchema> partitionKeys = table.getPartitionKeys();
      Map<String, Partition> nameToPart = new LinkedHashMap<>();
      for (final Partition part : request.getParts()) {
        if (request.isSkipColumnSchemaForPartition()) {
          part.getSd().setCols(table.getSd().getCols());
        }
        // Collect partition column stats to be updated if present. Partition objects passed down
        // here at the time of replication may have statistics in them, which is required to be
        // updated in the metadata. But we don't want it to be part of the Partition object when
        // it's being created or altered, lest it becomes part of the notification event.
        if (part.isSetColStats()) {
          partsColStats.add(part.getColStats());
          part.unsetColStats();
          partsWriteIds.add(part.getWriteId());
        }

        // Iterate through the partitions and validate them. If one of the partitions is
        // incorrect, an exception will be thrown before the threads which create the partition
        // folders are submitted. This way we can be sure that no partition and no partition
        // folder will be created if the list contains an invalid partition.
        validatePartition(part, catName, tblName, dbName, partsToAdd);
        nameToPart.put(Warehouse.makePartName(partitionKeys, part.getValues()), part);
      }

      List<Partition> existedParts =
          ms.getPartitionsByNames(catName, dbName, tblName,
              new GetPartitionsArgs.GetPartitionsArgsBuilder().partNames(new ArrayList<>(nameToPart.keySet())).build());
      List<String> existedPartNames = new ArrayList<>();
      for (Partition part : existedParts) {
        String partName = Warehouse.makePartName(partitionKeys, part.getValues());
        existedPartNames.add(partName);
        existingParts.add(nameToPart.remove(partName));
      }
      if (!request.isIfNotExists() && !existedPartNames.isEmpty()) {
        throw new AlreadyExistsException("Partition(s) already exist, partition name(s): " + existedPartNames);
      }

      if (nameToPart.isEmpty()) {
        return new AddPartitionsResult(true, Collections.emptyList());
      }
      List<Partition> partitionsToAdd = new ArrayList<>(nameToPart.values());
      // Only authorize on newly created partitions
      ((HMSHandler)handler).firePreEvent(new PreAddPartitionEvent(table, partitionsToAdd, handler));

      newParts.addAll(createPartitionFolders(partitionsToAdd, addedPartitions, envContext));

      if (!newParts.isEmpty()) {
        ms.addPartitions(catName, dbName, tblName, newParts);
      }

      // Notification is generated for newly created partitions only. The subset of partitions
      // that already exist (existingParts), will not generate notifications.
      if (!handler.getTransactionalListeners().isEmpty()) {
        transactionalListenerResponses =
            MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
                EventMessage.EventType.ADD_PARTITION,
                new AddPartitionEvent(table, newParts, true, handler), envContext);
      }

      // Update partition column statistics if available. We need a valid writeId list to
      // update column statistics for a transactional table. But during bootstrap replication,
      // where we use this feature, we do not have a valid writeId list which was used to
      // update the stats. But we know for sure that the writeId associated with the stats was
      // valid then (otherwise stats update would have failed on the source). So, craft a valid
      // transaction list with only that writeId and use it to update the stats.
      int cnt = 0;
      for (ColumnStatistics partColStats: partsColStats) {
        long writeId = partsWriteIds.get(cnt++);
        String validWriteIds = null;
        if (writeId > 0) {
          ValidWriteIdList validWriteIdList =
              new ValidReaderWriteIdList(TableName.getDbTable(table.getDbName(),
                  table.getTableName()),
                  new long[0], new BitSet(), writeId);
          validWriteIds = validWriteIdList.toString();
        }
        ((HMSHandler)handler).updatePartitonColStatsInternal(table, null, partColStats, validWriteIds, writeId);
      }

      success = ms.commitTransaction();
    } finally {
      try {
        if (!success) {
          ms.rollbackTransaction();
          cleanupPartitionFolders(addedPartitions, db);
        }
        if (!handler.getListeners().isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
              EventMessage.EventType.ADD_PARTITION,
              new AddPartitionEvent(table, newParts, success, handler),
              envContext, transactionalListenerResponses, ms);

          if (!existingParts.isEmpty()) {
            // The request has succeeded but we failed to add these partitions.
            MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
                EventMessage.EventType.ADD_PARTITION,
                new AddPartitionEvent(table, existingParts, false, handler),
                envContext, null, ms);
          }
        }
      } finally {
        tableLock.unlock();
      }
    }
    return new AddPartitionsResult(success, newParts);
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    this.catName = request.getCatName();
    this.dbName = request.getDbName();
    this.tblName = request.getTblName();
    if (dbName == null || tblName == null) {
      throw new MetaException("The database and table name cannot be null.");
    }
    this.wh = handler.getWh();
    RawStore ms = handler.getMS();
    table = ms.getTable(catName, dbName, tblName, null);
    if (table == null) {
      throw new InvalidObjectException("Unable to add partitions because "
          + TableName.getQualified(catName, dbName, tblName) + " does not exist");
    }

    db = ms.getDatabase(catName, dbName);
    if (MetaStoreUtils.isDatabaseRemote(db)) {
      throw new MetaException("Operation add_partitions not supported for REMOTE database " + dbName);
    }

    if (threadPool == null) {
      synchronized (AddPartitionsHandler.class) {
        if (threadPool == null) {
          int numThreads = MetastoreConf.getIntVar(handler.getConf(), MetastoreConf.ConfVars.FS_HANDLER_THREADS_COUNT);
          threadPool = Executors.newFixedThreadPool(numThreads,
              new ThreadFactoryBuilder().setDaemon(true).setNameFormat("HMSHandler #%d").build());
        }
      }
    }
  }

  /**
   * Validate a partition before creating it. The validation checks
   * <ul>
   * <li>if the database and table names set in the partition are not null and they are matching
   * with the expected values set in the tblName and dbName parameters.</li>
   * <li>if the partition values are set.</li>
   * <li>if none of the partition values is null.</li>
   * <li>if the partition values are matching with the pattern set in the
   * 'metastore.partition.name.whitelist.pattern' configuration property.</li>
   * <li>if the partition doesn't already exist. If the partition already exists, an exception
   * will be thrown if the ifNotExists parameter is false, otherwise it will be just ignored.</li>
   * <li>if the partsToAdd set doesn't contain the partition. The partsToAdd set contains the
   * partitions which are already validated. If the set contains the current partition, it means
   * that the partition is tried to be added multiple times in the same batch. Please note that
   * the set will be updated with the current partition if the validation was successful.</li>
   * </ul>
   * @param part
   * @param catName
   * @param tblName
   * @param dbName
   * @param partsToAdd
   * @throws MetaException
   * @throws TException
   */
  private void validatePartition(final Partition part, final String catName,
      final String tblName, final String dbName, final Set<PartValEqWrapperLite> partsToAdd)
      throws MetaException, TException {
    MetaStoreServerUtils.validatePartitionNameCharacters(part.getValues(), handler.getConf());
    if (part.getDbName() == null || part.getTableName() == null) {
      throw new MetaException("The database and table name must be set in the partition.");
    }

    if (!part.getTableName().equalsIgnoreCase(tblName)
        || !part.getDbName().equalsIgnoreCase(dbName)) {
      String errorMsg = String.format(
          "Partition does not belong to target table %s. It belongs to the table %s.%s : %s",
          TableName.getQualified(catName, dbName, tblName), part.getDbName(),
          part.getTableName(), part.toString());
      throw new MetaException(errorMsg);
    }

    if (part.getValues() == null || part.getValues().isEmpty()) {
      throw new MetaException("The partition values cannot be null or empty.");
    }

    if (part.getValues().contains(null)) {
      throw new MetaException("Partition value cannot be null.");
    }

    if (!partsToAdd.add(new PartValEqWrapperLite(part))) {
      // Technically, for ifNotExists case, we could insert one and discard the other
      // because the first one now "exists", but it seems better to report the problem
      // upstream as such a command doesn't make sense.
      throw new MetaException("Duplicate partitions in the list: " + part);
    }
  }

  /**
   * Create the location folders for the partitions. For each partition a separate thread will be
   * started to create the folder. The method will wait until all threads are finished and returns
   * the partitions whose folders were created successfully. If an error occurs during the
   * execution of a thread, a MetaException will be thrown.
   * @param partitionsToAdd
   * @param addedPartitions
   * @return
   * @throws MetaException
   */
  private List<Partition> createPartitionFolders(final List<Partition> partitionsToAdd,
      final Map<PartValEqWrapperLite, Boolean> addedPartitions,
      EnvironmentContext envContext) throws MetaException {
    final AtomicBoolean failureOccurred = new AtomicBoolean(false);
    final List<Future<Partition>> partFutures = new ArrayList<>(partitionsToAdd.size());
    final Map<PartValEqWrapperLite, Boolean> addedParts = new ConcurrentHashMap<>();

    final UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    for (final Partition partition : partitionsToAdd) {
      initializePartitionParameters(table, partition);

      partFutures.add(threadPool.submit(() -> {
        if (failureOccurred.get()) {
          return null;
        }
        ugi.doAs((PrivilegedExceptionAction<Partition>) () -> {
          try {
            boolean madeDir = createLocationForAddedPartition(table, partition);
            addedParts.put(new PartValEqWrapperLite(partition), madeDir);
            initializeAddedPartition(table, partition, madeDir, envContext);
          } catch (MetaException e) {
            throw new IOException(e.getMessage(), e);
          }
          return null;
        });
        return partition;
      }));
    }

    List<Partition> newParts = new ArrayList<>(partitionsToAdd.size());
    String errorMessage = null;
    for (Future<Partition> partFuture : partFutures) {
      try {
        Partition part = partFuture.get();
        if (part != null && !failureOccurred.get()) {
          newParts.add(part);
        }
      } catch (ExecutionException e) {
        // If an exception is thrown in the execution of a task, set the failureOccurred flag to
        // true. This flag is visible in the tasks and if its value is true, the partition
        // folders won't be created.
        // Then iterate through the remaining tasks and wait for them to finish. The tasks which
        // are started before the flag got set will then finish creating the partition folders.
        // The tasks which are started after the flag got set, won't create the partition
        // folders, to avoid unnecessary work.
        // This way it is sure that all tasks are finished, when entering the finally part where
        // the partition folders are cleaned up. It won't happen that a task is still running
        // when cleaning up the folders, so it is sure we won't have leftover folders.
        // Canceling the other tasks would be also an option but during testing it turned out
        // that it is not a trustworthy solution to avoid leftover folders.
        failureOccurred.compareAndSet(false, true);
        errorMessage = e.getMessage();
      } catch (InterruptedException e) {
        failureOccurred.compareAndSet(false, true);
        errorMessage = e.getMessage();
        // Restore interruption status of the corresponding thread
        Thread.currentThread().interrupt();
      }
    }

    addedPartitions.putAll(addedParts);
    if (failureOccurred.get()) {
      throw new MetaException(errorMessage);
    }

    return newParts;
  }


  private static class PartValEqWrapperLite {
    List<String> values;
    String location;

    PartValEqWrapperLite(Partition partition) {
      this.values = partition.isSetValues()? partition.getValues() : null;
      if (partition.getSd() != null) {
        this.location = partition.getSd().getLocation();
      }
    }

    @Override
    public int hashCode() {
      return values == null ? 0 : values.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || !(obj instanceof PartValEqWrapperLite)) {
        return false;
      }

      List<String> lhsValues = this.values;
      List<String> rhsValues = ((PartValEqWrapperLite)obj).values;

      if (lhsValues == null || rhsValues == null) {
        return lhsValues == rhsValues;
      }

      if (lhsValues.size() != rhsValues.size()) {
        return false;
      }

      for (int i=0; i<lhsValues.size(); ++i) {
        String lhsValue = lhsValues.get(i);
        String rhsValue = rhsValues.get(i);

        if ((lhsValue == null && rhsValue != null)
            || (lhsValue != null && !lhsValue.equals(rhsValue))) {
          return false;
        }
      }

      return true;
    }
  }

  /**
   * Remove the newly created partition folders. The values in the addedPartitions map indicates
   * whether or not the location of the partition was newly created. If the value is false, the
   * partition folder will not be removed.
   * @param addedPartitions
   * @throws MetaException
   * @throws IllegalArgumentException
   */
  private void cleanupPartitionFolders(final Map<PartValEqWrapperLite, Boolean> addedPartitions,
      Database db) throws MetaException, IllegalArgumentException {
    for (Map.Entry<PartValEqWrapperLite, Boolean> e : addedPartitions.entrySet()) {
      if (e.getValue()) {
        // we just created this directory - it's not a case of pre-creation, so we nuke.
        handler.getWh().deleteDir(new Path(e.getKey().location), true, db);
      }
    }
  }

  private void initializeAddedPartition(final Table tbl, final Partition part, boolean madeDir,
      EnvironmentContext environmentContext) throws MetaException {
    initializeAddedPartition(tbl,
        new PartitionSpecProxy.SimplePartitionWrapperIterator(part), madeDir, environmentContext);
  }

  private void initializeAddedPartition(
      final Table tbl, final PartitionSpecProxy.PartitionIterator part, boolean madeDir,
      EnvironmentContext environmentContext) throws MetaException {
    if (canUpdateStats(handler.getConf(), tbl)) {
      MetaStoreServerUtils.updatePartitionStatsFast(part, tbl, wh, madeDir,
          false, environmentContext, true);
    }

    // set create time
    long time = System.currentTimeMillis() / 1000;
    part.setCreateTime((int) time);
    if (part.getParameters() == null ||
        part.getParameters().get(hive_metastoreConstants.DDL_TIME) == null) {
      part.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(time));
    }
  }

  private void initializePartitionParameters(final Table tbl, final Partition part)
      throws MetaException {
    initializePartitionParameters(tbl,
        new PartitionSpecProxy.SimplePartitionWrapperIterator(part));
  }

  private void initializePartitionParameters(final Table tbl,
      final PartitionSpecProxy.PartitionIterator part) throws MetaException {

    // Inherit table properties into partition properties.
    Map<String, String> tblParams = tbl.getParameters();
    String inheritProps = MetastoreConf.getVar(handler.getConf(), MetastoreConf.ConfVars.PART_INHERIT_TBL_PROPS).trim();
    // Default value is empty string in which case no properties will be inherited.
    // * implies all properties needs to be inherited
    Set<String> inheritKeys = new HashSet<>(Arrays.asList(inheritProps.split(",")));
    if (inheritKeys.contains("*")) {
      inheritKeys = tblParams.keySet();
    }

    for (String key : inheritKeys) {
      String paramVal = tblParams.get(key);
      if (null != paramVal) { // add the property only if it exists in table properties
        part.putToParameters(key, paramVal);
      }
    }
  }

  /**
   * Handles the location for a partition being created.
   * @param tbl Table.
   * @param part Partition.
   * @return Whether the partition SD location is set to a newly created directory.
   */
  private boolean createLocationForAddedPartition(
      final Table tbl, final Partition part) throws MetaException {
    Path partLocation = null;
    String partLocationStr = null;
    if (part.getSd() != null) {
      partLocationStr = part.getSd().getLocation();
    }

    if (partLocationStr == null || partLocationStr.isEmpty()) {
      // set default location if not specified and this is
      // a physical table partition (not a view)
      if (tbl.getSd().getLocation() != null) {
        partLocation = new Path(tbl.getSd().getLocation(), Warehouse
            .makePartName(tbl.getPartitionKeys(), part.getValues()));
      }
    } else {
      if (tbl.getSd().getLocation() == null) {
        throw new MetaException("Cannot specify location for a view partition");
      }
      partLocation = wh.getDnsPath(new Path(partLocationStr));
    }

    boolean result = false;
    if (partLocation != null) {
      part.getSd().setLocation(partLocation.toString());

      // Check to see if the directory already exists before calling
      // mkdirs() because if the file system is read-only, mkdirs will
      // throw an exception even if the directory already exists.
      if (!wh.isDir(partLocation)) {
        if (!wh.mkdirs(partLocation)) {
          throw new MetaException(partLocation
              + " is not a directory or unable to create one");
        }
        result = true;
      }
    }
    return result;
  }


  @Override
  protected String getMessagePrefix() {
    return "AddPartitionsHandler [" + id + "] -  Add partitions for " +
        new TableName(catName, dbName, tblName) + ":";
  }

  @Override
  protected String getProgress() {
    return "Adding partitions";
  }

  public record AddPartitionsResult(boolean success, List<Partition> newParts) {

  }
}
