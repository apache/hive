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
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Hive specific implementation of alter 
 */
public class HiveAlterHandler  implements AlterHandler {

  private Configuration hiveConf;
  private static final Log LOG = LogFactory.getLog(HiveAlterHandler.class.getName());

  public Configuration getConf() { 
    return hiveConf;
  }

  @SuppressWarnings("nls")
  public void setConf(Configuration conf) {
    this.hiveConf = conf;
  }

  public void alterTable(RawStore msdb, Warehouse wh, String dbname, String name, Table newt)
  throws InvalidOperationException, MetaException {
    if (newt == null) {
      throw new InvalidOperationException("New table is invalid: " + newt);
    }
    
    if(!MetaStoreUtils.validateName(newt.getTableName()) ||
        !MetaStoreUtils.validateColNames(newt.getSd().getCols())) {
      throw new InvalidOperationException(newt.getTableName() + " is not a valid object name");
    }

    if (newt.getViewExpandedText() != null) {
      throw new InvalidOperationException(
        newt.getTableName()
        + " is a view, so it cannot be modified via ALTER TABLE");
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
    try {
      msdb.openTransaction();
      name = name.toLowerCase();
      dbname = dbname.toLowerCase();

      // check if table with the new name already exists
      if (!newt.getTableName().equalsIgnoreCase(name)
          || !newt.getDbName().equalsIgnoreCase(dbname)) {
        if(msdb.getTable(newt.getDbName(), newt.getTableName()) != null) {
          throw new InvalidOperationException("new table " + newt.getDbName() 
              + "." + newt.getTableName() + " already exists");
        }
        rename = true; 
      }

      // get old table
      Table oldt = msdb.getTable(dbname, name);
      if(oldt == null) {
        throw new InvalidOperationException("table " + newt.getDbName() 
            + "." + newt.getTableName() + " doesn't exist");
      }
      
      // check that partition keys have not changed
      if( oldt.getPartitionKeys().size() != newt.getPartitionKeys().size()
          || !oldt.getPartitionKeys().containsAll(newt.getPartitionKeys())) {
        throw new InvalidOperationException("partition keys can not be changed.");
      }
      
      if (rename  // if this alter is a rename
          && (oldt.getSd().getLocation().compareTo(newt.getSd().getLocation()) == 0 // and user didn't change the default location
              || StringUtils.isEmpty(newt.getSd().getLocation())) // or new location is empty
          && !oldt.getParameters().containsKey("EXTERNAL")) { // and table is not an external table
        // that means user is asking metastore to move data to new location corresponding to the new name
        // get new location
        newTblLoc = wh.getDefaultTablePath(newt.getDbName(), newt.getTableName()).toString();
        newt.getSd().setLocation(newTblLoc);
        oldTblLoc = oldt.getSd().getLocation();
        moveData = true;
        // check that destination does not exist otherwise we will be overwriting data
        srcPath = new Path(oldTblLoc);
        srcFs = wh.getFs(srcPath);
        destPath = new Path(newTblLoc);
        destFs = wh.getFs(destPath);
        // check that src and dest are on the same file system
        if (srcFs != destFs) {
          throw new InvalidOperationException("table new location " + destPath 
              + " is on a different file system than the old location " + srcPath
              + ". This operation is not supported");
        }
        try {
          srcFs.exists(srcPath); // check that src exists and also checks permissions necessary
          if(destFs.exists(destPath)) {
            throw new InvalidOperationException("New location for this table "+ newt.getDbName() 
                + "." + newt.getTableName() + " already exists : " +  destPath); 
          }
        } catch (IOException e) {
          throw new InvalidOperationException("Unable to access new location " + destPath + " for table "
              + newt.getDbName() + "." + newt.getTableName() );
        }
        // also the location field in partition
        List<Partition> parts = msdb.getPartitions(dbname, name, 0);
        for (Partition part : parts) {
          String oldPartLoc = part.getSd().getLocation();
          if (oldPartLoc.contains(oldTblLoc)) {
            part.getSd().setLocation(part.getSd().getLocation().replace(oldTblLoc, newTblLoc));
            msdb.alterPartition(dbname, name, part);
          }
        }
      }
      // now finally call alter table
      msdb.alterTable(dbname, name, newt);
      // commit the changes
      success = msdb.commitTransaction();
    } catch (InvalidObjectException e) {
      LOG.debug(e);
      throw new InvalidOperationException("Unable to change partition or table." +
      		" Check metastore logs for detailed stack." + e.getMessage());
    } finally {
      if(!success) {
        msdb.rollbackTransaction();
      }
      if(success && moveData) {
        // change the file name in hdfs
        // check that src exists otherwise there is no need to copy the data
        try {
          if (srcFs.exists(srcPath)) {
            // rename the src to destination
            srcFs.rename(srcPath, destPath);
          }
        } catch (IOException e) {
          throw new InvalidOperationException("Unable to access old location " + srcPath + " for table "
              + dbname + "." + name );
        }
      }
    }

  }
}
