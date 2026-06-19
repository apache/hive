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
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Formats SHOW TABLE STATUS commands to text format.
 */
public class TextShowTableStatusFormatter extends ShowTableStatusFormatter {
  @Override
  public void showTableStatus(DataOutputStream out, Hive db, HiveConf conf, List<Table> tables, Partition partition)
      throws HiveException {
    try {
      for (Table table : tables) {
        writeBasicInfo(out, table);
        writeStorageInfo(out, partition, table);
        writeColumnsInfo(out, table);
        writeFileSystemInfo(out, db, conf, partition, table);
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  private void writeBasicInfo(DataOutputStream out, Table table) throws IOException, UnsupportedEncodingException {
    out.write(("tableName:" + table.getTableName()).getBytes(StandardCharsets.UTF_8));
    out.write(Utilities.newLineCode);
    out.write(("owner:" + table.getOwner()).getBytes(StandardCharsets.UTF_8));
    out.write(Utilities.newLineCode);
  }

  private void writeStorageInfo(DataOutputStream out, Partition partition, Table table)
      throws HiveException, IOException {
    StorageInfo storageInfo = getStorageInfo(table, partition);

    out.write(("location:" + storageInfo.location).getBytes(StandardCharsets.UTF_8));
    out.write(Utilities.newLineCode);
    out.write(("inputformat:" + storageInfo.inputFormatClass).getBytes(StandardCharsets.UTF_8));
    out.write(Utilities.newLineCode);
    out.write(("outputformat:" + storageInfo.outputFormatClass).getBytes(StandardCharsets.UTF_8));
  }

  private void writeColumnsInfo(DataOutputStream out, Table table) throws IOException, UnsupportedEncodingException {
    String columns = MetaStoreUtils.getDDLFromFieldSchema("columns", table.getCols());
    String partitionColumns = table.isPartitioned() ?
        MetaStoreUtils.getDDLFromFieldSchema("partition_columns", table.getPartCols()) : "";

    out.write(Utilities.newLineCode);
    out.write(("columns:" + columns).getBytes(StandardCharsets.UTF_8));
    out.write(Utilities.newLineCode);
    out.write(("partitioned:" + table.isPartitioned()).getBytes(StandardCharsets.UTF_8));
    out.write(Utilities.newLineCode);
    out.write(("partitionColumns:" + partitionColumns).getBytes(StandardCharsets.UTF_8));
    out.write(Utilities.newLineCode);
  }

  private void writeFileSystemInfo(DataOutputStream out, Hive db, HiveConf conf, Partition partition, Table table)
      throws HiveException, IOException {
    List<Path> locations = getLocations(db, partition, table);
    if (!locations.isEmpty()) {
      writeFileSystemStats(out, conf, locations, table.getPath());
    }

    out.write(Utilities.newLineCode);
  }

  private static final String UNKNOWN = "unknown";

  // TODO: why is this in text formatter?!!
  //       This computes stats and should be in stats (de-duplicated too).
  private void writeFileSystemStats(DataOutputStream out, HiveConf conf, List<Path> locations, Path tablePath)
      throws IOException {
    FileData fileData = getFileData(conf, locations, tablePath);

    out.write("totalNumberFiles:".getBytes(StandardCharsets.UTF_8));
    out.write((fileData.unknown ? UNKNOWN : "" + fileData.numOfFiles).getBytes(StandardCharsets.UTF_8));
    out.write(Utilities.newLineCode);

    if (fileData.numOfErasureCodedFiles > 0) {
      out.write("totalNumberErasureCodedFiles:".getBytes(StandardCharsets.UTF_8));
      out.write((fileData.unknown ? UNKNOWN : "" + fileData.numOfErasureCodedFiles).getBytes(StandardCharsets.UTF_8));
      out.write(Utilities.newLineCode);
    }

    out.write("totalFileSize:".getBytes(StandardCharsets.UTF_8));
    out.write((fileData.unknown ? UNKNOWN : "" + fileData.totalFileSize).getBytes(StandardCharsets.UTF_8));
    out.write(Utilities.newLineCode);

    out.write("maxFileSize:".getBytes(StandardCharsets.UTF_8));
    out.write((fileData.unknown ? UNKNOWN : "" + fileData.maxFileSize).getBytes(StandardCharsets.UTF_8));
    out.write(Utilities.newLineCode);

    out.write("minFileSize:".getBytes(StandardCharsets.UTF_8));
    if (fileData.numOfFiles > 0) {
      out.write((fileData.unknown ? UNKNOWN : "" + fileData.minFileSize).getBytes(StandardCharsets.UTF_8));
    } else {
      out.write((fileData.unknown ? UNKNOWN : "" + 0).getBytes(StandardCharsets.UTF_8));
    }
    out.write(Utilities.newLineCode);

    out.write("lastAccessTime:".getBytes(StandardCharsets.UTF_8));
    out.writeBytes((fileData.unknown || fileData.lastAccessTime < 0) ? UNKNOWN : "" + fileData.lastAccessTime);
    out.write(Utilities.newLineCode);

    out.write("lastUpdateTime:".getBytes(StandardCharsets.UTF_8));
    out.write((fileData.unknown ? UNKNOWN : "" + fileData.lastUpdateTime).getBytes(StandardCharsets.UTF_8));
    out.write(Utilities.newLineCode);
  }
}
