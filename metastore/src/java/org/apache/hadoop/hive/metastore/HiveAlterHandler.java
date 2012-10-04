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
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

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

    if (!MetaStoreUtils.validateName(newt.getTableName())
        || !MetaStoreUtils.validateColNames(newt.getSd().getCols())) {
      throw new InvalidOperationException(newt.getTableName()
          + " is not a valid object name");
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
        newTblLoc = wh.getTablePath(msdb.getDatabase(newt.getDbName()), newt.getTableName()).toString();
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
        if (srcFs != destFs) {
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

  public Partition alterPartition(final RawStore msdb, Warehouse wh, final String dbname,
      final String name, final List<String> part_vals, final Partition new_part)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException,
      MetaException {
    boolean success = false;

    Path srcPath = null;
    Path destPath = null;
    FileSystem srcFs = null;
    FileSystem destFs = null;
    Table tbl = null;
    Partition oldPart = null;
    String oldPartLoc = null;
    String newPartLoc = null;
    // Set DDL time to now if not specified
    if (new_part.getParameters() == null ||
        new_part.getParameters().get(Constants.DDL_TIME) == null ||
        Integer.parseInt(new_part.getParameters().get(Constants.DDL_TIME)) == 0) {
      new_part.putToParameters(Constants.DDL_TIME, Long.toString(System
          .currentTimeMillis() / 1000));
    }
    //alter partition
    if (part_vals == null || part_vals.size() == 0) {
      try {
        oldPart = msdb.getPartition(dbname, name, new_part.getValues());
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
      tbl = msdb.getTable(dbname, name);
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
          destPath = constructRenamedPartitionPath(destPath, new_part);
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
    try {
      for (Partition tmpPart: new_parts) {
        // Set DDL time to now if not specified
        if (tmpPart.getParameters() == null ||
            tmpPart.getParameters().get(Constants.DDL_TIME) == null ||
            Integer.parseInt(tmpPart.getParameters().get(Constants.DDL_TIME)) == 0) {
          tmpPart.putToParameters(Constants.DDL_TIME, Long.toString(System
              .currentTimeMillis() / 1000));
        }
        Partition oldTmpPart = msdb.getPartition(dbname, name, tmpPart.getValues());
        oldParts.add(oldTmpPart);
        partValsList.add(tmpPart.getValues());
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
      if (!oldFs.getName().equals(newFs.getName()) ||
          !oldFs.getType().equals(newFs.getType())) {
        return false;
      }
    }

    return true;
  }

  /**
   * Uses the scheme and authority of the partition's current location, and the path constructed
   * using the partition's new name to construct a path for the partition's new location.
   */
  private Path constructRenamedPartitionPath(Path defaultPath, Partition part) {
    Path oldPath = new Path(part.getSd().getLocation());
    URI oldUri = oldPath.toUri();

    return new Path(oldUri.getScheme(), oldUri.getAuthority(), defaultPath.toUri().getPath());
  }
}
