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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hive.common.util.HiveStringUtils;

import com.google.common.collect.Lists;

/**
 * Hive specific implementation of alter
 */
public class HiveAlterHandler implements AlterHandler {

  protected Configuration hiveConf;
  private static final Log LOG = LogFactory.getLog(HiveAlterHandler.class
      .getName());

  public Configuration getConf() {
    return hiveConf;
  }

  @SuppressWarnings("nls")
  public void setConf(Configuration conf) {
    hiveConf = conf;
  }

  public void alterTable(RawStore msdb, Warehouse wh, String dbname,
      String name, Table newt) throws InvalidOperationException, MetaException {
    alterTable(msdb, wh, dbname, name, newt, false);
  }

  public void alterTable(RawStore msdb, Warehouse wh, String dbname,
      String name, Table newt, boolean cascade) throws InvalidOperationException, MetaException {
    if (newt == null) {
      throw new InvalidOperationException("New table is invalid: " + newt);
    }

    if (!MetaStoreUtils.validateName(newt.getTableName())) {
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
    boolean moveData = false;
    boolean rename = false;
    Table oldt = null;
    List<ObjectPair<Partition, String>> altps = new ArrayList<ObjectPair<Partition, String>>();

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
        throw new InvalidOperationException("table " + newt.getDbName() + "."
            + newt.getTableName() + " doesn't exist");
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

      // if this alter is a rename, the table is not a virtual view, the user
      // didn't change the default location (or new location is empty), and
      // table is not an external table, that means user is asking metastore to
      // move data to the new location corresponding to the new name
      if (rename
          && !oldt.getTableType().equals(TableType.VIRTUAL_VIEW.toString())
          && (oldt.getSd().getLocation().compareTo(newt.getSd().getLocation()) == 0
            || StringUtils.isEmpty(newt.getSd().getLocation()))
          && !MetaStoreUtils.isExternalTable(oldt)) {

        srcPath = new Path(oldt.getSd().getLocation());
        srcFs = wh.getFs(srcPath);

        // that means user is asking metastore to move data to new location
        // corresponding to the new name
        // get new location
        Path databasePath = constructRenamedPath(
            wh.getDefaultDatabasePath(newt.getDbName()), srcPath);
        destPath = new Path(databasePath, newt.getTableName());
        destFs = wh.getFs(destPath);

        newt.getSd().setLocation(destPath.toString());
        moveData = true;

        // check that destination does not exist otherwise we will be
        // overwriting data
        // check that src and dest are on the same file system
        if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
          throw new InvalidOperationException("table new location " + destPath
              + " is on a different file system than the old location "
              + srcPath + ". This operation is not supported");
        }
        try {
          srcFs.exists(srcPath); // check that src exists and also checks
                                 // permissions necessary
          if (destFs.exists(destPath)) {
            throw new InvalidOperationException("New location for this table "
                + newt.getDbName() + "." + newt.getTableName()
                + " already exists : " + destPath);
          }
        } catch (IOException e) {
          Warehouse.closeFs(srcFs);
          Warehouse.closeFs(destFs);
          throw new InvalidOperationException("Unable to access new location "
              + destPath + " for table " + newt.getDbName() + "."
              + newt.getTableName());
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
            altps.add(ObjectPair.create(part, part.getSd().getLocation()));
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
      } else if (MetaStoreUtils.requireCalStats(hiveConf, null, null, newt) &&
        (newt.getPartitionKeysSize() == 0)) {
          Database db = msdb.getDatabase(newt.getDbName());
          // Update table stats. For partitioned table, we update stats in
          // alterPartition()
          MetaStoreUtils.updateUnpartitionedTableStatsFast(db, newt, wh, false, true);
      }
      updateTableColumnStatsForAlterTable(msdb, oldt, newt);
      // now finally call alter table
      msdb.alterTable(dbname, name, newt);
      // commit the changes
      success = msdb.commitTransaction();
    } catch (InvalidObjectException e) {
      LOG.debug(e);
      throw new InvalidOperationException(
          "Unable to change partition or table."
              + " Check metastore logs for detailed stack." + e.getMessage());
    } catch (NoSuchObjectException e) {
      LOG.debug(e);
      throw new InvalidOperationException(
          "Unable to change partition or table. Database " + dbname + " does not exist"
              + " Check metastore logs for detailed stack." + e.getMessage());
    } finally {
      if (!success) {
        msdb.rollbackTransaction();
      }
      if (success && moveData) {
        // change the file name in hdfs
        // check that src exists otherwise there is no need to copy the data
        // rename the src to destination
        try {
          if (srcFs.exists(srcPath) && !srcFs.rename(srcPath, destPath)) {
            throw new IOException("Renaming " + srcPath + " to " + destPath + " is failed");
          }
        } catch (IOException e) {
          boolean revertMetaDataTransaction = false;
          try {
            msdb.openTransaction();
            msdb.alterTable(dbname, newt.getTableName(), oldt);
            for (ObjectPair<Partition, String> pair : altps) {
              Partition part = pair.getFirst();
              part.getSd().setLocation(pair.getSecond());
              msdb.alterPartition(dbname, name, part.getValues(), part);
            }
            revertMetaDataTransaction = msdb.commitTransaction();
          } catch (Exception e1) {
            // we should log this for manual rollback by administrator
            LOG.error("Reverting metadata by HDFS operation failure failed During HDFS operation failed", e1);
            LOG.error("Table " + Warehouse.getQualifiedName(newt) +
                " should be renamed to " + Warehouse.getQualifiedName(oldt));
            LOG.error("Table " + Warehouse.getQualifiedName(newt) +
                " should have path " + srcPath);
            for (ObjectPair<Partition, String> pair : altps) {
              LOG.error("Partition " + Warehouse.getQualifiedName(pair.getFirst()) +
                  " should have path " + pair.getSecond());
            }
            if (!revertMetaDataTransaction) {
              msdb.rollbackTransaction();
            }
          }
          throw new InvalidOperationException("Unable to access old location "
              + srcPath + " for table " + dbname + "." + name);
        }
      }
    }
    if (!success) {
      throw new MetaException("Committing the alter table transaction was not successful.");
    }
  }

  public Partition alterPartition(final RawStore msdb, Warehouse wh, final String dbname,
      final String name, final List<String> part_vals, final Partition new_part)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException,
      MetaException {
    boolean success = false;

    Path srcPath = null;
    Path destPath = null;
    FileSystem srcFs = null;
    FileSystem destFs = null;
    Partition oldPart = null;
    String oldPartLoc = null;
    String newPartLoc = null;

    // Set DDL time to now if not specified
    if (new_part.getParameters() == null ||
        new_part.getParameters().get(hive_metastoreConstants.DDL_TIME) == null ||
        Integer.parseInt(new_part.getParameters().get(hive_metastoreConstants.DDL_TIME)) == 0) {
      new_part.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
          .currentTimeMillis() / 1000));
    }

    Table tbl = msdb.getTable(dbname, name);
    //alter partition
    if (part_vals == null || part_vals.size() == 0) {
      try {
        oldPart = msdb.getPartition(dbname, name, new_part.getValues());
        if (MetaStoreUtils.requireCalStats(hiveConf, oldPart, new_part, tbl)) {
          MetaStoreUtils.updatePartitionStatsFast(new_part, wh, false, true);
        }
        updatePartColumnStats(msdb, dbname, name, new_part.getValues(), new_part);
        msdb.alterPartition(dbname, name, new_part.getValues(), new_part);
      } catch (InvalidObjectException e) {
        throw new InvalidOperationException("alter is not possible");
      } catch (NoSuchObjectException e){
        //old partition does not exist
        throw new InvalidOperationException("alter is not possible");
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
      Partition check_part = null;
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
      if (tbl == null) {
        throw new InvalidObjectException(
            "Unable to rename partition because table or database do not exist");
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
          destPath = new Path(wh.getTablePath(msdb.getDatabase(dbname), name),
            Warehouse.makePartName(tbl.getPartitionKeys(), new_part.getValues()));
          destPath = constructRenamedPath(destPath, new Path(new_part.getSd().getLocation()));
        } catch (NoSuchObjectException e) {
          LOG.debug(e);
          throw new InvalidOperationException(
            "Unable to change partition or table. Database " + dbname + " does not exist"
              + " Check metastore logs for detailed stack." + e.getMessage());
        }
        if (destPath != null) {
          newPartLoc = destPath.toString();
          oldPartLoc = oldPart.getSd().getLocation();

          srcPath = new Path(oldPartLoc);

          LOG.info("srcPath:" + oldPartLoc);
          LOG.info("descPath:" + newPartLoc);
          srcFs = wh.getFs(srcPath);
          destFs = wh.getFs(destPath);
          // check that src and dest are on the same file system
          if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
            throw new InvalidOperationException("table new location " + destPath
              + " is on a different file system than the old location "
              + srcPath + ". This operation is not supported");
          }
          try {
            srcFs.exists(srcPath); // check that src exists and also checks
            if (newPartLoc.compareTo(oldPartLoc) != 0 && destFs.exists(destPath)) {
              throw new InvalidOperationException("New location for this table "
                + tbl.getDbName() + "." + tbl.getTableName()
                + " already exists : " + destPath);
            }
          } catch (IOException e) {
            Warehouse.closeFs(srcFs);
            Warehouse.closeFs(destFs);
            throw new InvalidOperationException("Unable to access new location "
              + destPath + " for partition " + tbl.getDbName() + "."
              + tbl.getTableName() + " " + new_part.getValues());
          }
          new_part.getSd().setLocation(newPartLoc);
          if (MetaStoreUtils.requireCalStats(hiveConf, oldPart, new_part, tbl)) {
            MetaStoreUtils.updatePartitionStatsFast(new_part, wh, false, true);
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
            LOG.info("rename done!");
          }
        } catch (IOException e) {
          boolean revertMetaDataTransaction = false;
          try {
            msdb.openTransaction();
            msdb.alterPartition(dbname, name, new_part.getValues(), oldPart);
            revertMetaDataTransaction = msdb.commitTransaction();
          } catch (Exception e1) {
            LOG.error("Reverting metadata opeation failed During HDFS operation failed", e1);
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

  public List<Partition> alterPartitions(final RawStore msdb, Warehouse wh, final String dbname,
      final String name, final List<Partition> new_parts)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException,
      MetaException {
    List<Partition> oldParts = new ArrayList<Partition>();
    List<List<String>> partValsList = new ArrayList<List<String>>();
    Table tbl = msdb.getTable(dbname, name);
    try {
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

        if (MetaStoreUtils.requireCalStats(hiveConf, oldTmpPart, tmpPart, tbl)) {
          MetaStoreUtils.updatePartitionStatsFast(tmpPart, wh, false, true);
        }
        updatePartColumnStats(msdb, dbname, name, oldTmpPart.getValues(), tmpPart);
      }
      msdb.alterPartitions(dbname, name, partValsList, new_parts);
    } catch (InvalidObjectException e) {
      throw new InvalidOperationException("alter is not possible");
    } catch (NoSuchObjectException e){
      //old partition does not exist
      throw new InvalidOperationException("alter is not possible");
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
        for (ColumnStatisticsObj statsObj : statsObjs) {
          boolean found =false;
          for (FieldSchema newCol : newCols) {
            if (statsObj.getColName().equals(newCol.getName())
                && statsObj.getColType().equals(newCol.getType())) {
              found = true;
              break;
            }
          }
          if (!found) {
            msdb.deletePartitionColumnStatistics(dbName, tableName, oldPartName, partVals,
                statsObj.getColName());
          }
        }
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
          if (!MetaStoreUtils.areSameColumns(oldCols, newPart.getSd().getCols())) {
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

  private void updateTableColumnStatsForAlterTable(RawStore msdb, Table oldTable, Table newTable)
      throws MetaException, InvalidObjectException {
    String dbName = oldTable.getDbName();
    String tableName = oldTable.getTableName();
    String newDbName = HiveStringUtils.normalizeIdentifier(newTable.getDbName());
    String newTableName = HiveStringUtils.normalizeIdentifier(newTable.getTableName());

    try {
      if (!dbName.equals(newDbName) || !tableName.equals(newTableName)) {
        msdb.deleteTableColumnStatistics(dbName, tableName, null);
      } else {
        List<FieldSchema> oldCols = oldTable.getSd().getCols();
        List<FieldSchema> newCols = newTable.getSd().getCols();
        if (!MetaStoreUtils.areSameColumns(oldCols, newCols)) {
          List<String> oldColNames = new ArrayList<String>(oldCols.size());
          for (FieldSchema oldCol : oldCols) {
            oldColNames.add(oldCol.getName());
          }

          ColumnStatistics cs = msdb.getTableColumnStatistics(dbName, tableName, oldColNames);
          if (cs == null) {
            return;
          }

          List<ColumnStatisticsObj> statsObjs = cs.getStatsObj();
          for (ColumnStatisticsObj statsObj : statsObjs) {
            boolean found = false;
            for (FieldSchema newCol : newCols) {
              if (statsObj.getColName().equalsIgnoreCase(newCol.getName())
                  && statsObj.getColType().equals(newCol.getType())) {
                found = true;
                break;
              }
            }
            if (!found) {
              msdb.deleteTableColumnStatistics(dbName, tableName, statsObj.getColName());
            }
          }
        }
      }
    } catch (NoSuchObjectException nsoe) {
      LOG.debug("Could not find db entry." + nsoe);
    } catch (InvalidInputException e) {
      //should not happen since the input were verified before passed in
      throw new InvalidObjectException("Invalid inputs to update table column stats: " + e);
    }
  }
}
