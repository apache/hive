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

package org.apache.hadoop.hive.metastore.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.DropPartitionsExpr;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreDropPartitionEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.thrift.TException;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.checkTableDataShouldBeDeleted;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.isMustPurge;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

public class DropPartitionsHandler
    extends AbstractOperationHandler<DropPartitionsRequest, DropPartitionsHandler.DropPartitionsResult> {
  private String catName;
  private String dbName;
  private String tblName;
  private Table table;
  private List<Partition> partitions;

  DropPartitionsHandler(IHMSHandler handler, DropPartitionsRequest request)
      throws TException, IOException {
    super(handler, false, request);
  }

  @Override
  protected DropPartitionsResult execute() throws TException, IOException {
    EnvironmentContext envContext =
        request.isSetEnvironmentContext() ? request.getEnvironmentContext() : null;
    boolean success = false;
    Map<String, String> transactionalListenerResponses = null;
    RawStore ms = handler.getMS();
    boolean tableDataShouldBeDeleted = checkTableDataShouldBeDeleted(table, request.isDeleteData());
    boolean mustPurge = isMustPurge(request.getEnvironmentContext(), table);
    boolean needsCm = ReplChangeManager.shouldEnableCm(ms.getDatabase(catName, dbName), table);
    DropPartitionsResult result = new DropPartitionsResult(partitions, tableDataShouldBeDeleted,
        TxnUtils.isTransactionalTable(table),
        mustPurge, needsCm);
    try {
      ms.openTransaction();
      // We need Partition-s for firing events and for result; DN needs MPartition-s to drop.
      // Great... Maybe we could bypass fetching MPartitions by issuing direct SQL deletes.
      List<String> colNames = table.getPartitionKeys().stream().map(FieldSchema::getName).toList();
      List<String> partNames = new ArrayList<>(partitions.size());
      for (Partition part : partitions) {
        // TODO - we need to speed this up for the normal path where all partitions are under
        // the table and we don't have to stat every partition
        partNames.add(FileUtils.makePartName(colNames, part.getValues()));
        ((HMSHandler)handler).firePreEvent(new PreDropPartitionEvent(table, part, request.isDeleteData(), handler));
        if (tableDataShouldBeDeleted) {
          if (MetaStoreUtils.isArchived(part)) {
            // Archived partition is only able to delete original location.
            Path archiveParentDir = MetaStoreUtils.getOriginalLocation(part);
            verifyIsWritablePath(archiveParentDir);
            result.addArchToDelete(archiveParentDir);
          } else if ((part.getSd() != null) && (part.getSd().getLocation() != null)) {
            Path partPath = new Path(part.getSd().getLocation());
            verifyIsWritablePath(partPath);
            result.addDirToDelete(new PathAndDepth(partPath, part.getValues().size(), true));
          }
        }
      }
      ms.dropPartitions(catName, dbName, tblName, partNames);
      if (!partitions.isEmpty() && !handler.getTransactionalListeners().isEmpty()) {
        transactionalListenerResponses = MetaStoreListenerNotifier
            .notifyEvent(handler.getTransactionalListeners(), EventMessage.EventType.DROP_PARTITION,
                new DropPartitionEvent(table, partitions, true, request.isDeleteData(), handler), envContext);
      }
      success = ms.commitTransaction();
      result.setSuccess(success);
      return result;
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
      if (!partitions.isEmpty() && !handler.getListeners().isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
            EventMessage.EventType.DROP_PARTITION,
            new DropPartitionEvent(table, partitions, success, request.isDeleteData(), handler),
            envContext,
            transactionalListenerResponses, ms);
      }
    }
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    RawStore ms = handler.getMS();
    dbName = request.getDbName();
    tblName = request.getTblName();
    catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(handler.getConf());
    GetTableRequest getTableRequest = new GetTableRequest(dbName, tblName);
    getTableRequest.setCatName(catName);
    table = handler.get_table_core(getTableRequest);
    boolean hasMissingParts = false;
    boolean ifExists = request.isSetIfExists() && request.isIfExists();
    boolean ignoreProtection = request.isSetIgnoreProtection() && request.isIgnoreProtection();

    List<Partition> parts = null;
    RequestPartsSpec spec = request.getParts();
    if (spec.isSetExprs()) {
      // Dropping by expressions.
      parts = new ArrayList<>(spec.getExprs().size());
      for (DropPartitionsExpr expr : spec.getExprs()) {
        List<Partition> result = new ArrayList<>();
        boolean hasUnknown = ms.getPartitionsByExpr(catName, dbName, tblName, result,
            new GetPartitionsArgs.GetPartitionsArgsBuilder()
                .expr(expr.getExpr()).skipColumnSchemaForPartition(request.isSkipColumnSchemaForPartition())
                .build());
        if (hasUnknown) {
          // Expr is built by DDLSA, it should only contain part cols and simple ops
          throw new MetaException("Unexpected unknown partitions to drop");
        }
        // this is to prevent dropping archived partition which is archived in a
        // different level the drop command specified.
        if (!ignoreProtection && expr.isSetPartArchiveLevel()) {
          for (Partition part : parts) {
            if (MetaStoreUtils.isArchived(part)
                && MetaStoreUtils.getArchivingLevel(part) < expr.getPartArchiveLevel()) {
              throw new MetaException("Cannot drop a subset of partitions "
                  + " in an archive, partition " + part);
            }
          }
        }
        if (result.isEmpty()) {
          hasMissingParts = true;
          if (!ifExists) {
            // fail-fast for missing partition expr
            break;
          }
        }
        parts.addAll(result);
      }
    } else if (spec.isSetNames()) {
      List<String> partNames = spec.getNames();
      parts = ms.getPartitionsByNames(catName, dbName, tblName,
          new GetPartitionsArgs.GetPartitionsArgsBuilder()
              .partNames(partNames).skipColumnSchemaForPartition(request.isSkipColumnSchemaForPartition())
              .build());
      hasMissingParts = parts == null || (parts.size() != partNames.size());
    } else {
      throw new MetaException("Partition spec is not set");
    }

    if (hasMissingParts && !ifExists) {
      throw new NoSuchObjectException("Some partitions to drop are missing");
    }
    this.partitions = parts == null ? Collections.emptyList() : parts;
  }

  public void verifyIsWritablePath(Path dir) throws MetaException {
    try {
      if (!handler.getWh().isWritable(dir.getParent())) {
        throw new MetaException("Table partition not deleted since " + dir.getParent()
            + " is not writable by " + SecurityUtils.getUser());
      }
    } catch (IOException ex) {
      throw new MetaException("Table partition not deleted since " + dir.getParent()
          + " access cannot be checked: " + ex.getMessage());
    }
  }

  @Override
  protected void afterExecute() {
    request = null;
  }

  /**
   * Stores a path and its size.
   */
    public record PathAndDepth(Path path, int depth, boolean isPartitionDir) implements Comparable<PathAndDepth> {

    @Override
      public int hashCode() {
        return Objects.hash(path.hashCode(), depth);
      }

    /**
     * The largest {@code depth} is processed first in a {@link PriorityQueue}.
     */
      @Override
      public int compareTo(PathAndDepth o) {
        return o.depth - depth;
      }
    }

  private static void addParentForDel(PriorityQueue<PathAndDepth> parentsToDelete,
      PathAndDepth p) {
    Path parent = p.path.getParent();
    if (parent != null && p.depth - 1 > 0) {
      parentsToDelete.add(new PathAndDepth(parent, p.depth - 1, false));
    }
  }

  @Override
  protected String getMessagePrefix() {
    return "DropPartitionsHandler [" + id + "] -  Drop partitions from " +
        new TableName(catName, dbName, tblName) + ":";
  }

  @Override
  protected String getProgress() {
    return "Dropping partitions";
  }

  public static class DropPartitionsResult {
    private final List<Partition> partitions;
    private final boolean tableDataShouldBeDeleted;
    private final boolean mustPurge;
    private final boolean needCm;
    private final boolean isTransactionalTable;
    private boolean success;
    private final PriorityQueue<PathAndDepth> dirsToDelete = new PriorityQueue<>();
    private final List<Path> archToDelete = new ArrayList<>();

    public DropPartitionsResult(List<Partition> partitions,
        boolean tableDataShouldBeDeleted,
        boolean isTransactionalTable,
        boolean mustPurge, boolean needCm) {
      this.partitions = partitions;
      this.tableDataShouldBeDeleted = tableDataShouldBeDeleted;
      this.isTransactionalTable = isTransactionalTable;
      this.mustPurge = mustPurge;
      this.needCm = needCm;
    }

    public List<Partition> getPartitions() {
      return partitions;
    }

    public boolean tableDataShouldBeDeleted() {
      return tableDataShouldBeDeleted;
    }

    public boolean isTransactionalTable() {
      return isTransactionalTable;
    }

    public boolean mustPurge() {
      return mustPurge;
    }

    public boolean needCm() {
      return needCm;
    }

    public boolean success() {
      return success;
    }

    public void setSuccess(boolean success) {
      this.success = success;
    }

    public void addDirToDelete(PathAndDepth pathToDelete) {
      this.dirsToDelete.add(pathToDelete);
    }

    public List<Path> getArchToDelete() {
      return archToDelete;
    }

    public void addArchToDelete(Path archToDelete) {
      this.archToDelete.add(archToDelete);
    }

    public Iterator<PathAndDepth> getDirsToDelete() {
      if (dirsToDelete.isEmpty()) {
        return Collections.emptyIterator();
      }
      HashSet<PathAndDepth> processed = new HashSet<>();
      return new Iterator<>() {
        @Override
        public boolean hasNext() {
          while (!dirsToDelete.isEmpty()) {
            PathAndDepth path = dirsToDelete.peek();
            if (processed.contains(path)) {
              dirsToDelete.poll();
              continue;
            }
            return true;
          }
          return false;
        }

        @Override
        public PathAndDepth next() {
          PathAndDepth curPath = dirsToDelete.poll();
          addParentForDel(dirsToDelete, curPath);
          processed.add(curPath);
          return curPath;
        }
      };
    }
  }

}
