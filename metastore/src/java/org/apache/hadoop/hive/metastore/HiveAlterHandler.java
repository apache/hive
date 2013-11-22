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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;

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
    String oldTblLoc = null;
    String newTblLoc = null;
    boolean moveData = false;
    boolean rename = false;
    Table oldt = null;
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
      // table is not an external table, that means useris asking metastore to
      // move data to the new location corresponding to the new name
      if (rename
          && !oldt.getTableType().equals(TableType.VIRTUAL_VIEW.toString())
          && (oldt.getSd().getLocation().compareTo(newt.getSd().getLocation()) == 0
            || StringUtils.isEmpty(newt.getSd().getLocation()))
          && !MetaStoreUtils.isExternalTable(oldt)) {
        // that means user is asking metastore to move data to new location
        // corresponding to the new name
        // get new location
        newTblLoc = wh.getTablePath(msdb.getDatabase(newt.getDbName()),
            newt.getTableName()).toString();
        Path newTblPath = constructRenamedPath(new Path(newTblLoc),
            new Path(newt.getSd().getLocation()));
        newTblLoc = newTblPath.toString();
        newt.getSd().setLocation(newTblLoc);
        oldTblLoc = oldt.getSd().getLocation();
        moveData = true;
        // check that destination does not exist otherwise we will be
        // overwriting data
        srcPath = new Path(oldTblLoc);
        srcFs = wh.getFs(srcPath);
        destPath = new Path(newTblLoc);
        destFs = wh.getFs(destPath);
        // check that src and dest are on the same file system
        if (! equalsFileSystem(srcFs, destFs)) {
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
        // also the location field in partition
        List<Partition> parts = msdb.getPartitions(dbname, name, -1);
        for (Partition part : parts) {
          String oldPartLoc = part.getSd().getLocation();
          Path oldPartLocPath = new Path(oldPartLoc);
          String oldTblLocPath = new Path(oldTblLoc).toUri().getPath();
          String newTblLocPath = new Path(newTblLoc).toUri().getPath();
          if (oldPartLoc.contains(oldTblLocPath)) {
            Path newPartLocPath = null;
            URI oldUri = oldPartLocPath.toUri();
            String newPath = oldUri.getPath().replace(oldTblLocPath,
                                                      newTblLocPath);

            newPartLocPath = new Path(oldUri.getScheme(),
                                      oldUri.getAuthority(),
                                      newPath);
            part.getSd().setLocation(newPartLocPath.toString());
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
        try {
          if (srcFs.exists(srcPath)) {
            // rename the src to destination
            srcFs.rename(srcPath, destPath);
          }
        } catch (IOException e) {
          boolean revertMetaDataTransaction = false;
          try {
            msdb.openTransaction();
            msdb.alterTable(dbname, newt.getTableName(), oldt);
            revertMetaDataTransaction = msdb.commitTransaction();
          } catch (Exception e1) {
            LOG.error("Reverting metadata opeation failed During HDFS operation failed", e1);
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

  /**
   * @param fs1
   * @param fs2
   * @return return true if both file system arguments point to same file system
   */
  private boolean equalsFileSystem(FileSystem fs1, FileSystem fs2) {
    //When file system cache is disabled, you get different FileSystem objects
    // for same file system, so '==' can't be used in such cases
    //FileSystem api doesn't have a .equals() function implemented, so using
    //the uri for comparison. FileSystem already uses uri+Configuration for
    //equality in its CACHE .
    //Once equality has been added in HDFS-4321, we should make use of it
    return fs1.getUri().equals(fs2.getUri());
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
          if (srcFs != destFs) {
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
            if (!wh.mkdirs(destParentPath)) {
                throw new IOException("Unable to create path " + destParentPath);
            }
            srcFs.rename(srcPath, destPath);
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
}
