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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.data.TableMigrationUtil;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HiveTableUtil.class);

  static final String TABLE_EXTENSION = ".table";
  static final String FOR_COMMIT_EXTENSION = ".forCommit";

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
        List<Callable<Void>> tasks = Lists.newArrayList();
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
        try (ExecutorService executor = Executors.newFixedThreadPool(numThreads,
              new ThreadFactoryBuilder().setNameFormat("iceberg-metadata-generator-%d").setDaemon(true).build())) {
          executor.invokeAll(tasks);
        }
      }
      append.commit();
    } catch (IOException | InterruptedException e) {
      throw new MetaException("Cannot import hive data into iceberg table.\n" + e.getMessage());
    }
  }

  private static List<DataFile> getDataFiles(RemoteIterator<LocatedFileStatus> fileStatusIterator,
      Map<String, String> partitionKeys, String format, PartitionSpec spec, MetricsConfig metricsConfig,
      NameMapping nameMapping, Configuration conf) throws IOException {
    List<DataFile> dataFiles = Lists.newArrayList();
    while (fileStatusIterator.hasNext()) {
      LocatedFileStatus fileStatus = fileStatusIterator.next();
      String fileName = fileStatus.getPath().getName();
      if (fileName.startsWith(".") || fileName.startsWith("_") || fileName.endsWith("metadata.json")) {
        continue;
      }
      dataFiles.addAll(TableMigrationUtil.listPartition(partitionKeys, fileStatus.getPath().toString(), format, spec,
              conf, metricsConfig, nameMapping));
    }
    return dataFiles;
  }

  public static void appendFiles(URI fromURI, String format, Table icebergTbl, boolean isOverwrite,
      Map<String, String> partitionSpec, Configuration conf) throws SemanticException {
    try {
      Transaction transaction = icebergTbl.newTransaction();
      if (isOverwrite) {
        DeleteFiles delete = transaction.newDelete();
        if (partitionSpec != null) {
          Expression partitionExpr =
              IcebergTableUtil.generateExpressionFromPartitionSpec(icebergTbl, partitionSpec, true);
          delete.deleteFromRowFilter(partitionExpr);
        } else {
          delete.deleteFromRowFilter(Expressions.alwaysTrue());
        }
        delete.commit();
      }

      MetricsConfig metricsConfig = MetricsConfig.fromProperties(icebergTbl.properties());
      PartitionSpec spec = icebergTbl.spec();
      String nameMappingStr = icebergTbl.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
      NameMapping nameMapping = null;
      if (nameMappingStr != null) {
        nameMapping = NameMappingParser.fromJson(nameMappingStr);
      }
      AppendFiles append = transaction.newAppend();
      Map<String, String> actualPartitionSpec = Optional.ofNullable(partitionSpec).orElse(Collections.emptyMap());
      String actualFormat = Optional.ofNullable(format).orElse(IOConstants.PARQUET).toLowerCase();
      RemoteIterator<LocatedFileStatus> iterator = getFilesIterator(new Path(fromURI), conf);
      List<DataFile> dataFiles =
          getDataFiles(iterator, actualPartitionSpec, actualFormat, spec, metricsConfig, nameMapping, conf);
      dataFiles.forEach(append::appendFile);
      append.commit();
      transaction.commitTransaction();
    } catch (Exception e) {
      throw new SemanticException("Can not append data files", e);
    }
  }

  public static RemoteIterator<LocatedFileStatus> getFilesIterator(Path path, Configuration conf) throws MetaException {
    try {
      FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
      return fileSystem.listFiles(path, true);
    } catch (IOException e) {
      throw new MetaException("Exception happened during the collection of file statuses.\n" + e.getMessage());
    }
  }

  /**
   * Serializes the Iceberg table object.
   */
  public static String serializeTable(Table table, Configuration config, Properties props,
      List<String> broadcastConfig) {
    Table serializableTable = SerializableTable.copyOf(table);
    if (broadcastConfig != null) {
      broadcastConfig.forEach(cfg ->
          serializableTable.properties().computeIfAbsent(cfg, props::getProperty));
    }
    checkAndSkipIoConfigSerialization(config, serializableTable);
    return SerializationUtil.serializeToBase64(serializableTable);
  }

  /**
   * Returns the Table serialized to the configuration based on the table name.
   * If configuration is missing from the FileIO of the table, it will be populated with the input config.
   *
   * @param config The configuration used to get the data from
   * @param name The name of the table we need as returned by TableDesc.getTableName()
   * @return The Table
   */
  public static Table deserializeTable(Configuration config, String name) {
    Table table = SerializationUtil.deserializeFromBase64(
        config.get(InputFormatConfig.SERIALIZED_TABLE_PREFIX + name));
    String location = config.get(InputFormatConfig.TABLE_LOCATION);
    if (table == null &&
          config.getBoolean(hive_metastoreConstants.TABLE_IS_CTAS, false) &&
          StringUtils.isNotBlank(location)) {
      table = readTableObjectFromFile(location, config);
    }
    checkAndSetIoConfig(config, table);
    return table;
  }

  /**
   * If enabled, it populates the FileIO's hadoop configuration with the input config object.
   * This might be necessary when the table object was serialized without the FileIO config.
   *
   * @param config Configuration to set for FileIO, if enabled
   * @param table The Iceberg table object
   */
  private static void checkAndSetIoConfig(Configuration config, Table table) {
    if (table != null && config.getBoolean(InputFormatConfig.CONFIG_SERIALIZATION_DISABLED,
            InputFormatConfig.CONFIG_SERIALIZATION_DISABLED_DEFAULT) &&
          table.io() instanceof HadoopConfigurable fileIO) {
      fileIO.setConf(config);
    }
  }

  /**
   * If enabled, it ensures that the FileIO's hadoop configuration will not be serialized.
   * This might be desirable for decreasing the overall size of serialized table objects.
   *
   * Note: Skipping FileIO config serialization in this fashion might in turn necessitate calling
   * {@link #checkAndSetIoConfig(Configuration, Table)} on the deserializer-side to enable subsequent use of the FileIO.
   *
   * @param config Configuration to set for FileIO in a transient manner, if enabled
   * @param table The Iceberg table object
   */
  public static void checkAndSkipIoConfigSerialization(Configuration config, Table table) {
    if (table != null && config.getBoolean(InputFormatConfig.CONFIG_SERIALIZATION_DISABLED,
            InputFormatConfig.CONFIG_SERIALIZATION_DISABLED_DEFAULT) &&
          table.io() instanceof HadoopConfigurable fileIO) {
      fileIO.serializeConfWith(conf ->
          new NonSerializingConfig(config)::get);
    }
  }

  private static class NonSerializingConfig implements Serializable {
    private final transient Configuration conf;

    NonSerializingConfig(Configuration conf) {
      this.conf = conf;
    }

    public Configuration get() {
      if (conf == null) {
        throw new IllegalStateException(
            "Configuration was not serialized on purpose but was not set manually either");
      }
      return conf;
    }
  }

  /**
   * Generates the file location for the serialized table object.
   * @param location The location of the table.
   * @param conf The configuration containing the query ID.
   * @return The file path for the serialized table object.
   */
  private static String tableObjectLocation(String location, Configuration conf) {
    String queryId = conf.get(HiveConf.ConfVars.HIVE_QUERY_ID.varname);
    return location + "/temp/" + queryId + TABLE_EXTENSION;
  }

  /**
   * Generates the job temp location based on the job configuration.
   * @param location The location of the table.
   * @param conf The job's configuration.
   * @param jobId The JobID for the task.
   * @return The directory path for the job's temporary location.
   */
  public static String jobLocation(String location, Configuration conf, JobID jobId) {
    String queryId = conf.get(HiveConf.ConfVars.HIVE_QUERY_ID.varname);
    return location + "/temp/" + queryId + "-" + jobId;
  }

  /**
   * Generates file location based on the task configuration and a specific task id.
   * This file will be used to store the data required to generate the Iceberg commit.
   * @param location The location of the table.
   * @param conf The job's configuration.
   * @param jobId The jobId for the task.
   * @param taskId The taskId for the commit file.
   * @return The file path for storing the commit data.
   */
  static String fileForCommitLocation(String location, Configuration conf, JobID jobId, int taskId) {
    return jobLocation(location, conf, jobId) + "/task-" + taskId + FOR_COMMIT_EXTENSION;
  }

  static void createFileForTableObject(Table table, Configuration conf) {
    String filePath = tableObjectLocation(table.location(), conf);
    String bytes = serializeTable(table, conf, null, null);
    OutputFile serializedTableFile = table.io().newOutputFile(filePath);
    try (ObjectOutputStream oos = new ObjectOutputStream(serializedTableFile.createOrOverwrite())) {
      oos.writeObject(bytes);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
    LOG.debug("Iceberg table metadata file is created {}", serializedTableFile);
  }

  static void cleanupTableObjectFile(String location, Configuration configuration) {
    String filePath = tableObjectLocation(location, configuration);
    Path toDelete = new Path(filePath);
    try {
      FileSystem fs = Util.getFs(toDelete, configuration);
      fs.delete(toDelete, true);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  private static Table readTableObjectFromFile(String location, Configuration config) {
    String filePath = tableObjectLocation(location, config);
    try (FileIO io = new HadoopFileIO(config)) {
      try (ObjectInputStream ois = new ObjectInputStream(io.newInputFile(filePath).newStream())) {
        return SerializationUtil.deserializeFromBase64((String) ois.readObject());
      }
    } catch (ClassNotFoundException | IOException e) {
      throw new NotFoundException("Can not read or parse table object file: %s", filePath);
    }
  }

  public static boolean isCtas(Properties properties) {
    return Boolean.parseBoolean(properties.getProperty(hive_metastoreConstants.TABLE_IS_CTAS));
  }

}
