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

package org.apache.hadoop.hive.ql.index.compact;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.index.HiveIndex;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.api.StageType;

public class IndexMetadataChangeTask extends Task<IndexMetadataChangeWork>{

  private static final long serialVersionUID = 1L;

  @Override
  protected int execute(DriverContext driverContext) {
    
    try {
      Hive db = Hive.get(conf);
      IndexMetadataChangeWork work = this.getWork();
      String tblName = work.getIndexTbl();
      Table tbl = db.getTable(work.getDbName(), tblName);
      if (tbl == null ) {
        console.printError("Index table can not be null.");
        return 1;
      }

      if (!tbl.getTableType().equals(TableType.INDEX_TABLE)) {
        console.printError("Table " + tbl.getTableName() + " not specified.");
        return 1;
      }

      if (tbl.isPartitioned() && work.getPartSpec() == null) {
        console.printError("Index table is partitioned, but no partition specified.");
        return 1;
      }
      
      if (work.getPartSpec() != null) {
        Partition part = db.getPartition(tbl, work.getPartSpec(), false);
        if (part == null) {
          console.printError("Partition " + Warehouse.makePartName(work.getPartSpec()).toString()
              + " does not exist.");
          return 1;
        }
        
        Path url = new Path(part.getDataLocation().toString());
        FileSystem fs = url.getFileSystem(conf);
        FileStatus fstat = fs.getFileStatus(url);
        
        part.getParameters().put(HiveIndex.INDEX_TABLE_CREATETIME, Long.toString(fstat.getModificationTime()));
        db.alterPartition(tbl.getTableName(), part);
      } else {
        Path url = new Path(tbl.getDataLocation().toString());
        FileSystem fs = url.getFileSystem(conf);
        FileStatus fstat = fs.getFileStatus(url);
        tbl.getParameters().put(HiveIndex.INDEX_TABLE_CREATETIME, Long.toString(fstat.getModificationTime()));
        db.alterTable(tbl.getTableName(), tbl);
      }
    } catch (Exception e) {
      e.printStackTrace();
      console.printError("Error changing index table/partition metadata "
          + e.getMessage());
      return 1;
    }
    return 0;
  }

  @Override
  public String getName() {
    return "IndexMetadataChangeTask";
  }
  
  @Override
  public int getType() {
    return StageType.DDL;
  }

  @Override
  protected void localizeMRTmpFilesImpl(Context ctx) {
  }

}
