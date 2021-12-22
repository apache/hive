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

package org.apache.hadoop.hive.ql.ddl.table.info.show.status.formatter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.ddl.table.info.desc.formatter.JsonDescTableFormatter;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.formatting.MapBuilder;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Formats SHOW TABLE STATUS commands to json format.
 */
public class JsonShowTableStatusFormatter extends ShowTableStatusFormatter {
  @Override
  public void showTableStatus(DataOutputStream out, Hive db, HiveConf conf, List<Table> tables, Partition partition)
      throws HiveException {
    List<Map<String, Object>> tableData = new ArrayList<>();
    try {
      for (Table table : tables) {
        tableData.add(makeOneTableStatus(table, db, conf, partition));
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
    ShowUtils.asJson(out, MapBuilder.create().put("tables", tableData).build());
  }

  private Map<String, Object> makeOneTableStatus(Table table, Hive db, HiveConf conf, Partition partition)
      throws HiveException, IOException {
    StorageInfo storageInfo = getStorageInfo(table, partition);

    MapBuilder builder = MapBuilder.create();
    builder.put("tableName", table.getTableName());
    builder.put("ownerType", (table.getOwnerType() != null) ? table.getOwnerType().name() : "null");
    builder.put("owner", table.getOwner());
    builder.put("location", storageInfo.location);
    builder.put("inputFormat", storageInfo.inputFormatClass);
    builder.put("outputFormat", storageInfo.outputFormatClass);
    builder.put("columns", JsonDescTableFormatter.createColumnsInfo(table.getCols(), Collections.emptyList()));

    builder.put("partitioned", table.isPartitioned());
    if (table.isPartitioned()) {
      builder.put("partitionColumns", JsonDescTableFormatter.createColumnsInfo(table.getPartCols(),
          Collections.emptyList()));
    }

    if (table.getTableType() != TableType.VIRTUAL_VIEW) {
      putFileSystemStats(builder, getLocations(db, partition, table), conf, table.getPath());
    }

    return builder.build();
  }

  private void putFileSystemStats(MapBuilder builder, List<Path> locations, HiveConf conf, Path tablePath)
      throws IOException {
    FileData fileData = getFileData(conf, locations, tablePath);

    builder
      .put("totalNumberFiles", fileData.numOfFiles,       !fileData.unknown)
      .put("totalFileSize",    fileData.totalFileSize,    !fileData.unknown)
      .put("maxFileSize",      fileData.maxFileSize,      !fileData.unknown)
      .put("minFileSize",      fileData.getMinFileSize(), !fileData.unknown)
      .put("lastAccessTime",   fileData.lastAccessTime,   !(fileData.unknown || fileData.lastAccessTime < 0))
      .put("lastUpdateTime",   fileData.lastUpdateTime,   !fileData.unknown);
  }
}
