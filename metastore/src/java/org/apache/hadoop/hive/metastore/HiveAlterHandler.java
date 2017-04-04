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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hive.common.util.HiveStringUtils;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Hive specific implementation of alter
 */
public class HiveAlterHandler implements AlterHandler {

  protected Configuration hiveConf;
  private static final Logger LOG = LoggerFactory.getLogger(HiveAlterHandler.class
      .getName());

  @Override
  public Configuration getConf() {
    return hiveConf;
  }

  @Override
  @SuppressWarnings("nls")
  public void setConf(Configuration conf) {
    hiveConf = conf;
  }

  @Override
  public void alterTable(RawStore msdb, Warehouse wh, String dbname,
    String name, Table newt, EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException {
    alterTable(msdb, wh, dbname, name, newt, environmentContext, null);
  }

  @Override
  public void alterTable(RawStore msdb, Warehouse wh, String dbname,
      String name, Table newt, EnvironmentContext environmentContext,
      HMSHandler handler) throws InvalidOperationException, MetaException {
    final boolean cascade = environmentContext != null
        && environmentContext.isSetProperties()
        && StatsSetupConst.TRUE.equals(environmentContext.getProperties().get(
            StatsSetupConst.CASCADE));
    if (newt == null) {
      throw new InvalidOperationException("New table is invalid: " + newt);
    }

    if (!MetaStoreUtils.validateName(newt.getTableName(), hiveConf)) {
      throw new InvalidOperationException(newt.getTableName()
          + " is not a valid object name");
    }
    String validate = MetaStoreUtils.validateTblColumns(newt.getSd().getCols());
    if (validate != null) {
      throw new InvalidOperationException("Invalid column " + validate);
    }

    Path srcPath = null;
    FileSystem srcFs = null;
    Path destPath = null;
    FileSystem destFs = null;

    boolean success = false;
    boolean dataWasMoved = false;
    boolean rename = false;
    Table oldt = null;
    List<MetaStoreEventListener> transactionalListeners = null;
    if (handler != null) {
      transactionalListeners = handler.getTransactionalListeners();
    }

    try {
      msdb.openTransaction();
      name = name.toLowerCase();
      dbname = dbname.toLowerCase();

      // check if table with the new name already exists
      if (!newt.getTableName().equalsIgnoreCase(name)
          || !newt.getDbName().equalsIgnoreCase(dbname)) {
        if (msdb.getTable(newt.getDbName(), newt.getTableName()) != null) {
          throw new InvalidOperationException("new table " + newt.getDbName()
              + "." + newt.getTableName() + " already exists");
        }
        rename = true;
      }

      // get old table
      oldt = msdb.getTable(dbname, name);
      if (oldt == null) {
        throw new InvalidOperationException("table " + dbname + "." + name + " doesn't exist");
      }

      if (HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES,
            false)) {
        // Throws InvalidOperationException if the new column types are not
        // compatible with the current column types.
        MetaStoreUtils.throwExceptionIfIncompatibleColTypeChange(
            oldt.getSd().getCols(), newt.getSd().getCols());
      }

      if (cascade) {
        //Currently only column related changes can be cascaded in alter table
        if(MetaStoreUtils.isCascadeNeededInAlterTable(oldt, newt)) {
          List<Partition> parts = msdb.getPartitions(dbname, name, -1);
          for (Partition part : parts) {
            List<FieldSchema> oldCols = part.getSd().getCols();
            part.getSd().setCols(newt.getSd().getCols());
            String oldPartName = Warehouse.makePartName(oldt.getPartitionKeys(), part.getValues());
            updatePartColumnStatsForAlterColumns(msdb, part, oldPartName, part.getValues(), oldCols, part);
            msdb.alterPartition(dbname, name, part.getValues(), part);
          }
        } else {
          LOG.warn("Alter table does not cascade changes to its partitions.");
        }
      }

      //check that partition keys have not changed, except for virtual views
      //however, allow the partition comments to change
      boolean partKeysPartiallyEqual = checkPartialPartKeysEqual(oldt.getPartitionKeys(),
          newt.getPartitionKeys());

      if(!oldt.getTableType().equals(TableType.VIRTUAL_VIEW.toString())){
        if (oldt.getPartitionKeys().size() != newt.getPartitionKeys().size()
            || !partKeysPartiallyEqual) {
          throw new InvalidOperationException(
              "partition keys can not be changed.");
        }
      }

      // rename needs change the data location and move the data to the new location corresponding
      // to the new name if:
      // 1) the table is not a virtual view, and
      // 2) the table is not an external table, and
      // 3) the user didn't change the default location (or new location is empty), and
      // 4) the table was not initially created with a specified location
      if (rename
          && !oldt.getTableType().equals(TableType.VIRTUAL_VIEW.toString())
          && (oldt.getSd().getLocation().compareTo(newt.getSd().getLocation()) == 0
            || StringUtils.isEmpty(newt.getSd().getLocation()))
          && !MetaStoreUtils.isExternalTable(oldt)) {
        Database olddb = msdb.getDatabase(dbname);
        // if a table was created in a user specified location using the DDL like
        // create table tbl ... location ...., it should be treated like an external table
        // in the table rename, its data location should not be changed. We can check
        // if the table directory was created directly under its database directory to tell
        // if it is such a table
        srcPath = new Path(oldt.getSd().getLocation());
        String oldtRelativePath = (new Path(olddb.getLocationUri()).toUri())
            .relativize(srcPath.toUri()).toString();
        boolean tableInSpecifiedLoc = !oldtRelativePath.equalsIgnoreCase(name)
            && !oldtRelativePath.equalsIgnoreCase(name + Path.SEPARATOR);

        if (!tableInSpecifiedLoc) {
          srcFs = wh.getFs(srcPath);

          // get new location
          Database db = msdb.getDatabase(newt.getDbName());
          Path databasePath = constructRenamedPath(wh.getDatabasePath(db), srcPath);
          destPath = new Path(databasePath, newt.getTableName().toLowerCase());
          destFs = wh.getFs(destPath);

          newt.getSd().setLocation(destPath.toString());

          // check that destination does not exist otherwise we will be
          // overwriting data
          // check that src and dest are on the same file system
          if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
            throw new InvalidOperationException("table new location " + destPath
                + " is on a different file system than the old location "
                + srcPath + ". This operation is not supported");
          }

          try {
            if (destFs.exists(destPath)) {
              throw new InvalidOperationException("New location for this table "
                  + newt.getDbName() + "." + newt.getTableName()
                  + " already exists : " + destPath);
            }
            // check that src exists and also checks permissions necessary, rename src to dest
            if (srcFs.exists(srcPath) && srcFs.rename(srcPath, destPath)) {
              dataWasMoved = true;
            }
          } catch (IOException e) {
            LOG.error("Alter Table operation for " + dbname + "." + name + " failed.", e);
            throw new InvalidOperationException("Alter Table operation for " + dbname + "." + name +
                " failed to move data due to: '" + getSimpleMessage(e)
                + "' See hive log file for details.");
          }
          String oldTblLocPath = srcPath.toUri().getPath();
          String newTblLocPath = destPath.toUri().getPath();

          // also the location field in partition
          List<Partition> parts = msdb.getPartitions(dbname, name, -1);
          for (Partition part : parts) {
            String oldPartLoc = part.getSd().getLocation();
            if (oldPartLoc.contains(oldTblLocPath)) {
              URI oldUri = new Path(oldPartLoc).toUri();
              String newPath = oldUri.getPath().replace(oldTblLocPath, newTblLocPath);
              Path newPartLocPath = new Path(oldUri.getScheme(), oldUri.getAuthority(), newPath);
              part.getSd().setLocation(newPartLocPath.toString());
              String oldPartName = Warehouse.makePartName(oldt.getPartitionKeys(), part.getValues());
              try {
                //existing partition column stats is no longer valid, remove them
                msdb.deletePartitionColumnStatistics(dbname, name, oldPartName, part.getValues(), null);
              } catch (InvalidInputException iie) {
                throw new InvalidOperationException("Unable to update partition stats in table rename." + iie);
              }
              msdb.alterPartition(dbname, name, part.getValues(), part);
            }
          }
        }
      } else if (MetaStoreUtils.requireCalStats(hiveConf, null, null, newt, environmentContext) &&
        (newt.getPartitionKeysSize() == 0)) {
          Database db = msdb.getDatabase(newt.getDbName());
          // Update table stats. For partitioned table, we update stats in
          // alterPartition()
          MetaStoreUtils.updateTableStatsFast(db, newt, wh, false, true, environmentContext);
      }

      alterTableUpdateTableColumnStats(msdb, oldt, newt);
      if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                              EventMessage.EventType.ALTER_TABLE,
                                              new AlterTableEvent(oldt, newt, true, handler),
                                              environmentContext);
      }
      // commit the changes
      success = msdb.commitTransaction();
    } catch (InvalidObjectException e) {
      LOG.debug("Failed to get object from Metastore ", e);
      throw new InvalidOperationException(
          "Unable to change partition or table."
              + " Check metastore logs for detailed stack." + e.getMessage());
    } catch (NoSuchObjectException e) {
      LOG.debug("Object not found in metastore ", e);
      throw new InvalidOperationException(
          "Unable to change partition or table. Database " + dbname + " does not exist"
              + " Check metastore logs for detailed stack." + e.getMessage());
    } finally {
      if (!success) {
        LOG.error("Failed to alter table " + dbname + "." + name);
        msdb.rollbackTransaction();
        if (dataWasMoved) {
          try {
            if (destFs.exists(destPath)) {
              if (!destFs.rename(destPath, srcPath)) {
                LOG.error("Failed to restore data from " + destPath + " to " + srcPath
                    + " in alter table failure. Manual restore is needed.");
              }
            }
          } catch (IOException e) {
            LOG.error("Failed to restore data from " + destPath + " to " + srcPath
                +  " in alter table failure. Manual restore is needed.");
          }
        }
      }
    }
  }

  /**
   * RemoteExceptionS from hadoop RPC wrap the stack trace into e.getMessage() which makes
   * logs/stack traces confusing.
   * @param ex
   * @return
   */
  String getSimpleMessage(IOException ex) {
    if(ex instanceof RemoteException) {
      String msg = ex.getMessage();
      if(msg == null || !msg.contains("\n")) {
        return msg;
      }
      return msg.substring(0, msg.indexOf('\n'));
    }
    return ex.getMessage();
  }

  @Override
  public Partition alterPartition(final RawStore msdb, Warehouse wh, final String dbname,
    final String name, final List<String> part_vals, final Partition new_part,
    EnvironmentContext environmentContext)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException, MetaException {
    return alterPartition(msdb, wh, dbname, name, part_vals, new_part, environmentContext, null);
  }

  @Override
  public Partition alterPartition(final RawStore msdb, Warehouse wh, final String dbname,
    final String name, final List<String> part_vals, final Partition new_part,
    EnvironmentContext environmentContext, HMSHandler handler)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException, MetaException {
    boolean success = false;
    Path srcPath = null;
    Path destPath = null;
    FileSystem srcFs = null;
    FileSystem destFs;
    Partition oldPart = null;
    String oldPartLoc = null;
    String newPartLoc = null;
    List<MetaStoreEventListener> transactionalListeners = null;
    if (handler != null) {
      transactionalListeners = handler.getTransactionalListeners();
    }

    // Set DDL time to now if not specified
    if (new_part.getParameters() == null ||
        new_part.getParameters().get(hive_metastoreConstants.DDL_TIME) == null ||
        Integer.parseInt(new_part.getParameters().get(hive_metastoreConstants.DDL_TIME)) == 0) {
      new_part.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
          .currentTimeMillis() / 1000));
    }

    Table tbl = msdb.getTable(dbname, name);
    if (tbl == null) {
      throw new InvalidObjectException(
          "Unable to alter partition because table or database does not exist.");
    }

    //alter partition
    if (part_vals == null || part_vals.size() == 0) {
      try {
        msdb.openTransaction();
        oldPart = msdb.getPartition(dbname, name, new_part.getValues());
        if (MetaStoreUtils.requireCalStats(hiveConf, oldPart, new_part, tbl, environmentContext)) {
          // if stats are same, no need to update
          if (MetaStoreUtils.isFastStatsSame(oldPart, new_part)) {
            MetaStoreUtils.updateBasicState(environmentContext, new_part.getParameters());
          } else {
            MetaStoreUtils.updatePartitionStatsFast(new_part, wh, false, true, environmentContext);
          }
        }

        updatePartColumnStats(msdb, dbname, name, new_part.getValues(), new_part);
        msdb.alterPartition(dbname, name, new_part.getValues(), new_part);
        if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                EventMessage.EventType.ALTER_PARTITION,
                                                new AlterPartitionEvent(oldPart, new_part, tbl, true, handler),
                                                environmentContext);


        }
        success = msdb.commitTransaction();
      } catch (InvalidObjectException e) {
        throw new InvalidOperationException("alter is not possible");
      } catch (NoSuchObjectException e){
        //old partition does not exist
        throw new InvalidOperationException("alter is not possible");
      } finally {
        if(!success) {
          msdb.rollbackTransaction();
        }
      }
      return oldPart;
    }

    //rename partition
    try {
      msdb.openTransaction();
      try {
        oldPart = msdb.getPartition(dbname, name, part_vals);
      } catch (NoSuchObjectException e) {
        // this means there is no existing partition
        throw new InvalidObjectException(
            "Unable to rename partition because old partition does not exist");
      }

      Partition check_part;
      try {
        check_part = msdb.getPartition(dbname, name, new_part.getValues());
      } catch(NoSuchObjectException e) {
        // this means there is no existing partition
        check_part = null;
      }

      if (check_part != null) {
        throw new AlreadyExistsException("Partition already exists:" + dbname + "." + name + "." +
            new_part.getValues());
      }

      // if the external partition is renamed, the file should not change
      if (tbl.getTableType().equals(TableType.EXTERNAL_TABLE.toString())) {
        new_part.getSd().setLocation(oldPart.getSd().getLocation());
        String oldPartName = Warehouse.makePartName(tbl.getPartitionKeys(), oldPart.getValues());
        try {
          //existing partition column stats is no longer valid, remove
          msdb.deletePartitionColumnStatistics(dbname, name, oldPartName, oldPart.getValues(), null);
        } catch (NoSuchObjectException nsoe) {
          //ignore
        } catch (InvalidInputException iie) {
          throw new InvalidOperationException("Unable to update partition stats in table rename." + iie);
        }
        msdb.alterPartition(dbname, name, part_vals, new_part);
      } else {
        try {
          // if tbl location is available use it
          // else derive the tbl location from database location
          destPath = wh.getPartitionPath(msdb.getDatabase(dbname), tbl, new_part.getValues());
          destPath = constructRenamedPath(destPath, new Path(new_part.getSd().getLocation()));
        } catch (NoSuchObjectException e) {
          LOG.debug("Didn't find object in metastore ", e);
          throw new InvalidOperationException(
            "Unable to change partition or table. Database " + dbname + " does not exist"
              + " Check metastore logs for detailed stack." + e.getMessage());
        }

        if (destPath != null) {
          newPartLoc = destPath.toString();
          oldPartLoc = oldPart.getSd().getLocation();
          LOG.info("srcPath:" + oldPartLoc);
          LOG.info("descPath:" + newPartLoc);
          srcPath = new Path(oldPartLoc);
          srcFs = wh.getFs(srcPath);
          destFs = wh.getFs(destPath);
          // check that src and dest are on the same file system
          if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
            throw new InvalidOperationException("New table location " + destPath
              + " is on a different file system than the old location "
              + srcPath + ". This operation is not supported.");
          }

          try {
            srcFs.exists(srcPath);
            if (newPartLoc.compareTo(oldPartLoc) != 0 && destFs.exists(destPath)) {
              throw new InvalidOperationException("New location for this table "
                + tbl.getDbName() + "." + tbl.getTableName()
                + " already exists : " + destPath);
            }
          } catch (IOException e) {
            throw new InvalidOperationException("Unable to access new location "
              + destPath + " for partition " + tbl.getDbName() + "."
              + tbl.getTableName() + " " + new_part.getValues());
          }

          new_part.getSd().setLocation(newPartLoc);
          if (MetaStoreUtils.requireCalStats(hiveConf, oldPart, new_part, tbl, environmentContext)) {
            MetaStoreUtils.updatePartitionStatsFast(new_part, wh, false, true, environmentContext);
          }

          String oldPartName = Warehouse.makePartName(tbl.getPartitionKeys(), oldPart.getValues());
          try {
            //existing partition column stats is no longer valid, remove
            msdb.deletePartitionColumnStatistics(dbname, name, oldPartName, oldPart.getValues(), null);
          } catch (NoSuchObjectException nsoe) {
            //ignore
          } catch (InvalidInputException iie) {
            throw new InvalidOperationException("Unable to update partition stats in table rename." + iie);
          }

          msdb.alterPartition(dbname, name, part_vals, new_part);
        }
      }

      if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                              EventMessage.EventType.ALTER_PARTITION,
                                              new AlterPartitionEvent(oldPart, new_part, tbl, true, handler),
                                              environmentContext);
      }

      success = msdb.commitTransaction();
    } finally {
      if (!success) {
        msdb.rollbackTransaction();
      }

      if (success && newPartLoc != null && newPartLoc.compareTo(oldPartLoc) != 0) {
        //rename the data directory
        try{
          if (srcFs.exists(srcPath)) {
            //if destPath's parent path doesn't exist, we should mkdir it
            Path destParentPath = destPath.getParent();
            if (!wh.mkdirs(destParentPath, true)) {
                throw new IOException("Unable to create path " + destParentPath);
            }

            wh.renameDir(srcPath, destPath, true);
            LOG.info("Partition directory rename from " + srcPath + " to " + destPath + " done.");
          }
        } catch (IOException ex) {
          LOG.error("Cannot rename partition directory from " + srcPath + " to " +
              destPath, ex);
          boolean revertMetaDataTransaction = false;
          try {
            msdb.openTransaction();
            msdb.alterPartition(dbname, name, new_part.getValues(), oldPart);
            if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                    EventMessage.EventType.ALTER_PARTITION,
                                                    new AlterPartitionEvent(new_part, oldPart, tbl, success, handler),
                                                    environmentContext);
            }

            revertMetaDataTransaction = msdb.commitTransaction();
          } catch (Exception ex2) {
            LOG.error("Attempt to revert partition metadata change failed. The revert was attempted " +
                "because associated filesystem rename operation failed with exception " + ex.getMessage(), ex2);
            if (!revertMetaDataTransaction) {
              msdb.rollbackTransaction();
            }
          }

          throw new InvalidOperationException("Unable to access old location "
              + srcPath + " for partition " + tbl.getDbName() + "."
              + tbl.getTableName() + " " + part_vals);
        }
      }
    }
    return oldPart;
  }

  @Override
  public List<Partition> alterPartitions(final RawStore msdb, Warehouse wh, final String dbname,
    final String name, final List<Partition> new_parts,
    EnvironmentContext environmentContext)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException, MetaException {
    return alterPartitions(msdb, wh, dbname, name, new_parts, environmentContext, null);
  }

    @Override
  public List<Partition> alterPartitions(final RawStore msdb, Warehouse wh, final String dbname,
    final String name, final List<Partition> new_parts, EnvironmentContext environmentContext,
    HMSHandler handler)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException, MetaException {
    List<Partition> oldParts = new ArrayList<Partition>();
    List<List<String>> partValsList = new ArrayList<List<String>>();
    List<MetaStoreEventListener> transactionalListeners = null;
    if (handler != null) {
      transactionalListeners = handler.getTransactionalListeners();
    }

    Table tbl = msdb.getTable(dbname, name);
    if (tbl == null) {
      throw new InvalidObjectException(
          "Unable to alter partitions because table or database does not exist.");
    }

    boolean success = false;
    try {
      msdb.openTransaction();
      for (Partition tmpPart: new_parts) {
        // Set DDL time to now if not specified
        if (tmpPart.getParameters() == null ||
            tmpPart.getParameters().get(hive_metastoreConstants.DDL_TIME) == null ||
            Integer.parseInt(tmpPart.getParameters().get(hive_metastoreConstants.DDL_TIME)) == 0) {
          tmpPart.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
              .currentTimeMillis() / 1000));
        }

        Partition oldTmpPart = msdb.getPartition(dbname, name, tmpPart.getValues());
        oldParts.add(oldTmpPart);
        partValsList.add(tmpPart.getValues());

        if (MetaStoreUtils.requireCalStats(hiveConf, oldTmpPart, tmpPart, tbl, environmentContext)) {
          // Check if stats are same, no need to update
          if (MetaStoreUtils.isFastStatsSame(oldTmpPart, tmpPart)) {
            MetaStoreUtils.updateBasicState(environmentContext, tmpPart.getParameters());
          } else {
            MetaStoreUtils.updatePartitionStatsFast(tmpPart, wh, false, true, environmentContext);
          }
        }
        updatePartColumnStats(msdb, dbname, name, oldTmpPart.getValues(), tmpPart);
      }

      msdb.alterPartitions(dbname, name, partValsList, new_parts);
      Iterator<Partition> oldPartsIt = oldParts.iterator();
      for (Partition newPart : new_parts) {
        Partition oldPart;
        if (oldPartsIt.hasNext()) {
          oldPart = oldPartsIt.next();
        } else {
          throw new InvalidOperationException("Missing old partition corresponding to new partition " +
              "when invoking MetaStoreEventListener for alterPartitions event.");
        }

        if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                EventMessage.EventType.ALTER_PARTITION,
                                                new AlterPartitionEvent(oldPart, newPart, tbl, true, handler));
        }
      }

      success = msdb.commitTransaction();
    } catch (InvalidObjectException | NoSuchObjectException e) {
      throw new InvalidOperationException("Alter partition operation failed: " + e);
    } finally {
      if(!success) {
        msdb.rollbackTransaction();
      }
    }

    return oldParts;
  }

  private boolean checkPartialPartKeysEqual(List<FieldSchema> oldPartKeys,
      List<FieldSchema> newPartKeys) {
    //return true if both are null, or false if one is null and the other isn't
    if (newPartKeys == null || oldPartKeys == null) {
      return oldPartKeys == newPartKeys;
    }
    if (oldPartKeys.size() != newPartKeys.size()) {
      return false;
    }
    Iterator<FieldSchema> oldPartKeysIter = oldPartKeys.iterator();
    Iterator<FieldSchema> newPartKeysIter = newPartKeys.iterator();
    FieldSchema oldFs;
    FieldSchema newFs;
    while (oldPartKeysIter.hasNext()) {
      oldFs = oldPartKeysIter.next();
      newFs = newPartKeysIter.next();
      // Alter table can change the type of partition key now.
      // So check the column name only.
      if (!oldFs.getName().equals(newFs.getName())) {
        return false;
      }
    }

    return true;
  }

  /**
   * Uses the scheme and authority of the object's current location and the path constructed
   * using the object's new name to construct a path for the object's new location.
   */
  private Path constructRenamedPath(Path defaultNewPath, Path currentPath) {
    URI currentUri = currentPath.toUri();

    return new Path(currentUri.getScheme(), currentUri.getAuthority(),
        defaultNewPath.toUri().getPath());
  }

  private void updatePartColumnStatsForAlterColumns(RawStore msdb, Partition oldPartition,
      String oldPartName, List<String> partVals, List<FieldSchema> oldCols, Partition newPart)
          throws MetaException, InvalidObjectException {
    String dbName = oldPartition.getDbName();
    String tableName = oldPartition.getTableName();
    try {
      List<String> oldPartNames = Lists.newArrayList(oldPartName);
      List<String> oldColNames = new ArrayList<String>(oldCols.size());
      for (FieldSchema oldCol : oldCols) {
        oldColNames.add(oldCol.getName());
      }
      List<FieldSchema> newCols = newPart.getSd().getCols();
      List<ColumnStatistics> partsColStats = msdb.getPartitionColumnStatistics(dbName, tableName,
          oldPartNames, oldColNames);
      assert (partsColStats.size() <= 1);
      for (ColumnStatistics partColStats : partsColStats) { //actually only at most one loop
        List<ColumnStatisticsObj> statsObjs = partColStats.getStatsObj();
        List<String> deletedCols = new ArrayList<String>();
        for (ColumnStatisticsObj statsObj : statsObjs) {
          boolean found =false;
          for (FieldSchema newCol : newCols) {
            if (statsObj.getColName().equalsIgnoreCase(newCol.getName())
                && statsObj.getColType().equalsIgnoreCase(newCol.getType())) {
              found = true;
              break;
            }
          }
          if (!found) {
            msdb.deletePartitionColumnStatistics(dbName, tableName, oldPartName, partVals,
                statsObj.getColName());
            deletedCols.add(statsObj.getColName());
          }
        }
        StatsSetupConst.removeColumnStatsState(newPart.getParameters(), deletedCols);
      }
    } catch (NoSuchObjectException nsoe) {
      LOG.debug("Could not find db entry." + nsoe);
      //ignore
    } catch (InvalidInputException iie) {
      throw new InvalidObjectException
      ("Invalid input to update partition column stats in alter table change columns" + iie);
    }
  }

  private void updatePartColumnStats(RawStore msdb, String dbName, String tableName,
      List<String> partVals, Partition newPart) throws MetaException, InvalidObjectException {
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    String newDbName = HiveStringUtils.normalizeIdentifier(newPart.getDbName());
    String newTableName = HiveStringUtils.normalizeIdentifier(newPart.getTableName());

    Table oldTable = msdb.getTable(dbName, tableName);
    if (oldTable == null) {
      return;
    }

    try {
      String oldPartName = Warehouse.makePartName(oldTable.getPartitionKeys(), partVals);
      String newPartName = Warehouse.makePartName(oldTable.getPartitionKeys(), newPart.getValues());
      if (!dbName.equals(newDbName) || !tableName.equals(newTableName)
          || !oldPartName.equals(newPartName)) {
        msdb.deletePartitionColumnStatistics(dbName, tableName, oldPartName, partVals, null);
      } else {
        Partition oldPartition = msdb.getPartition(dbName, tableName, partVals);
        if (oldPartition == null) {
          return;
        }
        if (oldPartition.getSd() != null && newPart.getSd() != null) {
        List<FieldSchema> oldCols = oldPartition.getSd().getCols();
          if (!MetaStoreUtils.columnsIncluded(oldCols, newPart.getSd().getCols())) {
            updatePartColumnStatsForAlterColumns(msdb, oldPartition, oldPartName, partVals, oldCols, newPart);
          }
        }
      }
    } catch (NoSuchObjectException nsoe) {
      LOG.debug("Could not find db entry." + nsoe);
      //ignore
    } catch (InvalidInputException iie) {
      throw new InvalidObjectException("Invalid input to update partition column stats." + iie);
    }
  }

  @VisibleForTesting
  void alterTableUpdateTableColumnStats(RawStore msdb,
      Table oldTable, Table newTable)
      throws MetaException, InvalidObjectException {
    String dbName = oldTable.getDbName().toLowerCase();
    String tableName = HiveStringUtils.normalizeIdentifier(oldTable.getTableName());
    String newDbName = newTable.getDbName().toLowerCase();
    String newTableName = HiveStringUtils.normalizeIdentifier(newTable.getTableName());

    try {
      List<FieldSchema> oldCols = oldTable.getSd().getCols();
      List<FieldSchema> newCols = newTable.getSd().getCols();
      List<ColumnStatisticsObj> newStatsObjs = new ArrayList<ColumnStatisticsObj>();
      ColumnStatistics colStats = null;
      boolean updateColumnStats = true;

      // Nothing to update if everything is the same
        if (newDbName.equals(dbName) &&
            newTableName.equals(tableName) &&
            MetaStoreUtils.columnsIncluded(oldCols, newCols)) {
          updateColumnStats = false;
        }

        if (updateColumnStats) {
          List<String> oldColNames = new ArrayList<String>(oldCols.size());
          for (FieldSchema oldCol : oldCols) {
            oldColNames.add(oldCol.getName());
          }

          // Collect column stats which need to be rewritten and remove old stats
          colStats = msdb.getTableColumnStatistics(dbName, tableName, oldColNames);
          if (colStats == null) {
            updateColumnStats = false;
          } else {
            List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
            if (statsObjs != null) {
              List<String> deletedCols = new ArrayList<String>();
              for (ColumnStatisticsObj statsObj : statsObjs) {
                boolean found = false;
                for (FieldSchema newCol : newCols) {
                  if (statsObj.getColName().equalsIgnoreCase(newCol.getName())
                      && statsObj.getColType().equalsIgnoreCase(newCol.getType())) {
                    found = true;
                    break;
                  }
                }

                if (found) {
                  if (!newDbName.equals(dbName) || !newTableName.equals(tableName)) {
                    msdb.deleteTableColumnStatistics(dbName, tableName, statsObj.getColName());
                    newStatsObjs.add(statsObj);
                    deletedCols.add(statsObj.getColName());
                  }
                } else {
                  msdb.deleteTableColumnStatistics(dbName, tableName, statsObj.getColName());
                  deletedCols.add(statsObj.getColName());
                }
              }
              StatsSetupConst.removeColumnStatsState(newTable.getParameters(), deletedCols);
            }
          }
        }

        // Change to new table and append stats for the new table
        msdb.alterTable(dbName, tableName, newTable);
        if (updateColumnStats && !newStatsObjs.isEmpty()) {
          ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
          statsDesc.setDbName(newDbName);
          statsDesc.setTableName(newTableName);
          colStats.setStatsObj(newStatsObjs);
          msdb.updateTableColumnStatistics(colStats);
        }
    } catch (NoSuchObjectException nsoe) {
      LOG.debug("Could not find db entry." + nsoe);
    } catch (InvalidInputException e) {
      //should not happen since the input were verified before passed in
      throw new InvalidObjectException("Invalid inputs to update table column stats: " + e);
    }
  }
}
