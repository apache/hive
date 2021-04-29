/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;

public class HiveTableUtil {

  private HiveTableUtil() {
  }

  /**
   * Import files from given partitions to an Iceberg table.
   * @param sourceLocation location of the HMS table
   * @param format inputformat class name of the HMS table
   * @param partitionSpecProxy  list of HMS table partitions wrapped in partitionSpecProxy
   * @param partitionKeys list of partition keys
   * @param icebergTableProperties destination iceberg table properties
   * @param conf a Hadoop configuration
   */
  public static void importFiles(String sourceLocation,
      String format, PartitionSpecProxy partitionSpecProxy, List<FieldSchema> partitionKeys,
      Properties icebergTableProperties, Configuration conf) throws MetaException {

    RemoteIterator<LocatedFileStatus> filesIterator = null;
    // if the table is unpartitioned, get the list of files by scanning everything from the root of the HMS table.
    // this operation must be done before the iceberg table is created
    if (partitionSpecProxy.size() == 0) {
      filesIterator = getFilesIterator(new Path(sourceLocation), conf);
    }

    Table icebergTable = Catalogs.createTable(conf, icebergTableProperties);
    AppendFiles append = icebergTable.newAppend();
    PartitionSpec spec = icebergTable.spec();
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(icebergTable.properties());
    String nameMappingString = icebergTable.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping nameMapping = nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;

    try {
      if (partitionSpecProxy.size() == 0) {
        List<DataFile> dataFiles = getDataFiles(filesIterator, Collections.emptyMap(), format, spec, metricsConfig,
            nameMapping, conf);
        dataFiles.forEach(append::appendFile);
      } else {
        PartitionSpecProxy.PartitionIterator partitionIterator = partitionSpecProxy.getPartitionIterator();
        List<Callable<Void>> tasks = new ArrayList<>();
        while (partitionIterator.hasNext()) {
          Partition partition = partitionIterator.next();
          Callable<Void> task = () -> {
            Path partitionPath = new Path(partition.getSd().getLocation());
            String partitionName = Warehouse.makePartName(partitionKeys, partition.getValues());
            Map<String, String> partitionSpec = Warehouse.makeSpecFromName(partitionName);
            RemoteIterator<LocatedFileStatus> iterator = getFilesIterator(partitionPath, conf);
            List<DataFile> dataFiles =
                getDataFiles(iterator, partitionSpec, format.toLowerCase(), spec, metricsConfig, nameMapping, conf);
            synchronized (append) {
              dataFiles.forEach(append::appendFile);
            }
            return null;
          };
          tasks.add(task);
        }
        int numThreads = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_SERVER2_ICEBERG_METADATA_GENERATOR_THREADS);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads,
            new ThreadFactoryBuilder().setNameFormat("iceberg-metadata-generator-%d").setDaemon(true).build());
        executor.invokeAll(tasks);
        executor.shutdown();
      }
      append.commit();
    } catch (IOException | InterruptedException e) {
      throw new MetaException("Cannot import hive data into iceberg table.\n" + e.getMessage());
    }
  }

  private static List<DataFile> getDataFiles(RemoteIterator<LocatedFileStatus> fileStatusIterator,
      Map<String, String> partitionKeys, String format, PartitionSpec spec, MetricsConfig metricsConfig,
      NameMapping nameMapping, Configuration conf) throws IOException {
    List<DataFile> dataFiles = new ArrayList<>();
    while (fileStatusIterator.hasNext()) {
      LocatedFileStatus fileStatus = fileStatusIterator.next();
      String fileName = fileStatus.getPath().getName();
      if (fileName.startsWith(".") || fileName.startsWith("_")) {
        continue;
      }
      dataFiles.addAll(DataUtil.listPartition(partitionKeys, fileStatus.getPath().toString(), format, spec, conf,
          metricsConfig, nameMapping));
    }
    return dataFiles;
  }

  public static RemoteIterator<LocatedFileStatus> getFilesIterator(Path path, Configuration conf) throws MetaException {
    try {
      FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
      return fileSystem.listFiles(path, true);
    } catch (IOException e) {
      throw new MetaException("Exception happened during the collection of file statuses.\n" + e.getMessage());
    }
  }
}
