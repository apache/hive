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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Formats SHOW TABLE STATUS results.
 */
public abstract class ShowTableStatusFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(ShowTableStatusFormatter.class);

  public static ShowTableStatusFormatter getFormatter(HiveConf conf) {
    if (MetaDataFormatUtils.isJson(conf)) {
      return new JsonShowTableStatusFormatter();
    } else {
      return new TextShowTableStatusFormatter();
    }
  }

  public abstract void showTableStatus(DataOutputStream out, Hive db, HiveConf conf, List<Table> tables, Partition par)
      throws HiveException;

  StorageInfo getStorageInfo(Table table, Partition partition) throws HiveException {
    String location = null;
    String inputFormatClass = null;
    String outputFormatClass = null;
    if (partition != null) {
      if (partition.getLocation() != null) {
        location = partition.getDataLocation().toString();
      }
      inputFormatClass = partition.getInputFormatClass() == null ? null : partition.getInputFormatClass().getName();
      outputFormatClass = partition.getOutputFormatClass() == null ? null : partition.getOutputFormatClass().getName();
    } else {
      if (table.getPath() != null) {
        location = table.getDataLocation().toString();
      }
      inputFormatClass = table.getInputFormatClass() == null ? null : table.getInputFormatClass().getName();
      outputFormatClass = table.getOutputFormatClass() == null ? null : table.getOutputFormatClass().getName();
    }

    return new StorageInfo(location, inputFormatClass, outputFormatClass);
  }

  static final class StorageInfo {
    final String location;
    final String inputFormatClass;
    final String outputFormatClass;

    private StorageInfo(String location, String inputFormatClass, String outputFormatClass) {
      this.location = location;
      this.inputFormatClass = inputFormatClass;
      this.outputFormatClass = outputFormatClass;
    }
  }

  List<Path> getLocations(Hive db, Partition partition, Table table) throws HiveException {
    List<Path> locations = new ArrayList<Path>();
    if (table.isPartitioned()) {
      if (partition == null) {
        for (Partition currPartition : db.getPartitions(table)) {
          if (currPartition.getLocation() != null) {
            locations.add(new Path(currPartition.getLocation()));
          }
        }
      } else {
        if (partition.getLocation() != null) {
          locations.add(new Path(partition.getLocation()));
        }
      }
    } else {
      if (table.getPath() != null) {
        locations.add(table.getPath());
      }
    }
    return locations;
  }

  FileData getFileData(HiveConf conf, List<Path> locations, Path tablePath) throws IOException {
    FileData fileData = new FileData();
    FileSystem fileSystem = tablePath.getFileSystem(conf);
    // in case all files in locations do not exist
    try {
      FileStatus tmpStatus = fileSystem.getFileStatus(tablePath);
      fileData.lastAccessTime = tmpStatus.getAccessTime();
      fileData.lastUpdateTime = tmpStatus.getModificationTime();
    } catch (IOException e) {
      LOG.warn("Cannot access File System. File System status will be unknown.", e);
      fileData.unknown = true;
    }

    if (!fileData.unknown) {
      for (Path location : locations) {
        try {
          FileStatus status = fileSystem.getFileStatus(location);
          // no matter loc is the table location or part location, it must be a
          // directory.
          if (!status.isDirectory()) {
            continue;
          }
          processDir(status, fileSystem, fileData);
        } catch (IOException e) {
          // ignore
        }
      }
    }
    return fileData;
  }

  private void processDir(FileStatus status, FileSystem fileSystem, FileData fileData) throws IOException {
    fileData.lastAccessTime = Math.max(fileData.lastAccessTime, status.getAccessTime());
    fileData.lastUpdateTime = Math.max(fileData.lastUpdateTime, status.getModificationTime());

    FileStatus[] entryStatuses = fileSystem.listStatus(status.getPath());
    for (FileStatus entryStatus : entryStatuses) {
      if (entryStatus.isDirectory()) {
        processDir(entryStatus, fileSystem, fileData);
        continue;
      }

      fileData.numOfFiles++;
      if (entryStatus.isErasureCoded()) {
        fileData.numOfErasureCodedFiles++;
      }

      long fileLength = entryStatus.getLen();
      fileData.totalFileSize += fileLength;
      fileData.maxFileSize = Math.max(fileData.maxFileSize, fileLength);
      fileData.minFileSize = Math.min(fileData.minFileSize, fileLength);

      fileData.lastAccessTime = Math.max(fileData.lastAccessTime, entryStatus.getAccessTime());
      fileData.lastUpdateTime = Math.max(fileData.lastUpdateTime, entryStatus.getModificationTime());
    }
  }

  static class FileData {
    boolean unknown = false;
    long totalFileSize = 0;
    long maxFileSize = 0;
    long minFileSize = Long.MAX_VALUE;
    long lastAccessTime = 0;
    long lastUpdateTime = 0;
    int numOfFiles = 0;
    int numOfErasureCodedFiles = 0;
    
    long getMinFileSize() {
      return numOfFiles > 0 ? minFileSize : 0;
    }
  }
}
