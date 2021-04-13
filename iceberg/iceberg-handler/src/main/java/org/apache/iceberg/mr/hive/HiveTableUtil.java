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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
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
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HiveTableUtil.class);

  private static class DataFileFilter implements PathFilter {
    private final FileSystem fileSystem;

    private DataFileFilter(FileSystem fileSystem) {
      this.fileSystem = fileSystem;
    }

    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      try {
        return !name.startsWith("_") && !name.startsWith(".") && fileSystem.isFile(path);
      } catch (IOException e) {
        return false;
      }
    }
  }

  private HiveTableUtil() {
  }

  /**
   * Import files from given partitions to an Iceberg table.
   * @param sourceLocation location of the HMS table
   * @param format inputformat class name of the HMS table
   * @param partitionSpecProxy  list of HMS table partitions wrapped in partitionSpecProxy
   * @param icebergTable destination iceberg table
   * @param conf a Hadoop configuration
   */
  public static void importFiles(String sourceLocation, String format, PartitionSpecProxy partitionSpecProxy,
      Table icebergTable, Configuration conf) {
    AppendFiles append = icebergTable.newAppend();
    try {
      URI uri = new URI(sourceLocation);
      FileSystem fileSystem = FileSystem.get(uri, conf);
      PartitionSpec spec = icebergTable.spec();
      MetricsConfig metricsConfig = MetricsConfig.fromProperties(icebergTable.properties());
      String nameMappingString = icebergTable.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
      NameMapping nameMapping = nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
      if (partitionSpecProxy.size() == 0) {
        List<DataFile> dataFiles = getDataFiles(fileSystem, new Path(uri), Collections.emptyMap(), format, spec,
            metricsConfig, nameMapping, conf);
        dataFiles.forEach(append::appendFile);
      } else {
        PartitionSpecProxy.PartitionIterator iterator = partitionSpecProxy.getPartitionIterator();
        List<Callable<List<DataFile>>> tasks = new ArrayList<>();
        while (iterator.hasNext()) {
          Partition partition = iterator.next();
          Callable<List<DataFile>> task = () -> {
            Path partitionPath = new Path(partition.getSd().getLocation());
            Map<String, String> partitionSpec = Warehouse.makeSpecFromName(partitionPath.toString());
            return getDataFiles(fileSystem, partitionPath, partitionSpec, format.toLowerCase(), spec, metricsConfig,
                nameMapping, conf);
          };
          tasks.add(task);
        }
        int numThreads = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_SERVER2_ICEBERG_METADATA_GENERATOR_THREADS);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads,
            new ThreadFactoryBuilder().setNameFormat("Iceberg metadata generator").setDaemon(true).build());
        List<Future<List<DataFile>>> futures = executor.invokeAll(tasks);
        for (Future<List<DataFile>> future : futures) {
          List<DataFile> dataFiles = future.get();
          dataFiles.forEach(append::appendFile);
        }
      }

    } catch (URISyntaxException | IOException | InterruptedException | ExecutionException e) {
      LOG.error("Cannot import hive data into iceberg table", e);
    } finally {
      append.commit();
    }
  }

  private static List<DataFile> getDataFiles(FileSystem fileSystem, Path path, Map<String, String> partition,
      String format, PartitionSpec spec, MetricsConfig metricsConfig, NameMapping nameMapping, Configuration conf)
      throws IOException {
    FileStatus[] fileStatuses = fileSystem.listStatus(path, new DataFileFilter(fileSystem));
    return Arrays.stream(fileStatuses).map(fs -> DataUtil
        .listPartition(partition, fs.getPath().toString(), format, spec,
            conf, metricsConfig, nameMapping)).flatMap(List::stream).collect(Collectors.toList());
  }
}
