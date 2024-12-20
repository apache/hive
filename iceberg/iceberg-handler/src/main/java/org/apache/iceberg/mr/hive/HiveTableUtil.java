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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.data.TableMigrationUtil;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expressions;
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
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HiveTableUtil.class);

  static final String TABLE_EXTENSION = ".table";

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
    List<DataFile> dataFiles = Lists.newArrayList();
    while (fileStatusIterator.hasNext()) {
      LocatedFileStatus fileStatus = fileStatusIterator.next();
      String fileName = fileStatus.getPath().getName();
      if (fileName.startsWith(".") || fileName.startsWith("_") || fileName.endsWith("metadata.json")) {
        continue;
      }
      partitionKeys.replaceAll((key, value) -> FileUtils.escapePathName(value));

      int[] stringFields = IntStream.range(0, spec.javaClasses().length)
        .filter(i -> spec.javaClasses()[i].isAssignableFrom(String.class)).toArray();

      dataFiles.addAll(Lists.transform(
          TableMigrationUtil.listPartition(partitionKeys, fileStatus.getPath().toString(), format, spec,
            conf, metricsConfig, nameMapping),
          dataFile -> {
            StructLike structLike = dataFile.partition();
            for (int pos : stringFields) {
              structLike.set(pos, FileUtils.unescapePathName(structLike.get(pos, String.class)));
            }
            return dataFile;
          }));
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
          for (Map.Entry<String, String> part : partitionSpec.entrySet()) {
            final Type partKeyType = icebergTbl.schema().findType(part.getKey());
            final Object partKeyVal = Conversions.fromPartitionString(partKeyType, part.getValue());
            delete.deleteFromRowFilter(Expressions.equal(part.getKey(), partKeyVal));
          }
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

  static String generateTableObjectLocation(String tableLocation, Configuration conf) {
    return tableLocation + "/temp/" + conf.get(HiveConf.ConfVars.HIVE_QUERY_ID.varname) + TABLE_EXTENSION;
  }

  static void createFileForTableObject(Table table, Configuration conf) {
    String filePath = generateTableObjectLocation(table.location(), conf);

    Table serializableTable = SerializableTable.copyOf(table);
    HiveIcebergStorageHandler.checkAndSkipIoConfigSerialization(conf, serializableTable);
    String serialized = SerializationUtil.serializeToBase64(serializableTable);

    OutputFile serializedTableFile = table.io().newOutputFile(filePath);
    try (ObjectOutputStream oos = new ObjectOutputStream(serializedTableFile.createOrOverwrite())) {
      oos.writeObject(serialized);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
    LOG.debug("Iceberg table metadata file is created {}", serializedTableFile);
  }

  static void cleanupTableObjectFile(String location, Configuration configuration) {
    String tableObjectLocation = HiveTableUtil.generateTableObjectLocation(location, configuration);
    try {
      Path toDelete = new Path(tableObjectLocation);
      FileSystem fs = Util.getFs(toDelete, configuration);
      fs.delete(toDelete, true);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  static Table readTableObjectFromFile(Configuration config) {
    String location = config.get(InputFormatConfig.TABLE_LOCATION);
    String filePath = HiveTableUtil.generateTableObjectLocation(location, config);

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

  static Properties getSerializationProps() {
    Properties props = new Properties();
    props.put(serdeConstants.SERIALIZATION_FORMAT, "" + Utilities.tabCode);
    props.put(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
    return props;
  }

  static String getParseData(String parseData, String specId, ObjectMapper mapper, Integer currentSpecId)
      throws JsonProcessingException {
    Map<String, String> map = mapper.readValue(parseData, Map.class);
    String partString =
        map.entrySet().stream()
            .filter(entry -> entry.getValue() != null)
            .map(java.lang.Object::toString)
            .collect(Collectors.joining("/"));
    String currentSpecMarker = currentSpecId.toString().equals(specId) ? "current-" : "";
    return String.format("%sspec-id=%s/%s", currentSpecMarker, specId, partString);
  }

  static JobConf getPartJobConf(Configuration confs, org.apache.hadoop.hive.ql.metadata.Table tbl) {
    JobConf job = new JobConf(confs);
    job.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, Constants.ICEBERG_PARTITION_COLUMNS);
    job.set(InputFormatConfig.TABLE_LOCATION, tbl.getPath().toString());
    job.set(InputFormatConfig.TABLE_IDENTIFIER, tbl.getFullyQualifiedName() + ".partitions");
    HiveConf.setVar(job, HiveConf.ConfVars.HIVE_FETCH_OUTPUT_SERDE, Constants.DELIMITED_JSON_SERDE);
    HiveConf.setBoolVar(job, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    return job;
  }
}
