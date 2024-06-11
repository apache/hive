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

package org.apache.iceberg.mr.mapreduce;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapHiveUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataTableScan;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.SystemConfigs;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveVersion;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.HiveIcebergInputFormat;
import org.apache.iceberg.mr.hive.HiveIcebergStorageHandler;
import org.apache.iceberg.mr.hive.IcebergAcidUtil;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.iceberg.util.ThreadPools;

/**
 * Generic Mrv2 InputFormat API for Iceberg.
 *
 * @param <T> T is the in memory data model which can either be Pig tuples, Hive rows. Default is Iceberg records
 */
public class IcebergInputFormat<T> extends InputFormat<Void, T> {

  /**
   * Configures the {@code Job} to use the {@code IcebergInputFormat} and
   * returns a helper to add further configuration.
   *
   * @param job the {@code Job} to configure
   */
  public static InputFormatConfig.ConfigBuilder configure(Job job) {
    job.setInputFormatClass(IcebergInputFormat.class);
    return new InputFormatConfig.ConfigBuilder(job.getConfiguration());
  }

  private static TableScan createTableScan(Table table, Configuration conf) {
    TableScan scan = table.newScan();

    long snapshotId = -1;
    try {
      snapshotId = conf.getLong(InputFormatConfig.SNAPSHOT_ID, -1);
    } catch (NumberFormatException e) {
      String version = conf.get(InputFormatConfig.SNAPSHOT_ID);
      SnapshotRef ref = table.refs().get(version);
      if (ref == null) {
        throw new RuntimeException("Cannot find matching snapshot ID or reference name for version " + version);
      }
      snapshotId = ref.snapshotId();
    }
    String refName = conf.get(InputFormatConfig.OUTPUT_TABLE_SNAPSHOT_REF);
    if (StringUtils.isNotEmpty(refName)) {
      scan = scan.useRef(HiveUtils.getTableSnapshotRef(refName));
    }
    if (snapshotId != -1) {
      scan = scan.useSnapshot(snapshotId);
    }

    long asOfTime = conf.getLong(InputFormatConfig.AS_OF_TIMESTAMP, -1);
    if (asOfTime != -1) {
      scan = scan.asOfTime(asOfTime);
    }

    return scan;
  }

  private static IncrementalAppendScan createIncrementalAppendScan(Table table, Configuration conf) {
    long fromSnapshot = conf.getLong(InputFormatConfig.SNAPSHOT_ID_INTERVAL_FROM, -1);
    return table.newIncrementalAppendScan().fromSnapshotExclusive(fromSnapshot);
  }

  private static <
          T extends Scan<T, FileScanTask, CombinedScanTask>> Scan<T,
          FileScanTask,
          CombinedScanTask> applyConfig(
          Configuration conf, Scan<T, FileScanTask, CombinedScanTask> scanToConfigure) {

    Scan<T, FileScanTask, CombinedScanTask> scan = scanToConfigure.caseSensitive(
            conf.getBoolean(InputFormatConfig.CASE_SENSITIVE, InputFormatConfig.CASE_SENSITIVE_DEFAULT));

    long splitSize = conf.getLong(InputFormatConfig.SPLIT_SIZE, 0);
    if (splitSize > 0) {
      scan = scan.option(TableProperties.SPLIT_SIZE, String.valueOf(splitSize));
    }

    // In case of LLAP-based execution we ask Iceberg not to combine multiple fileScanTasks into one split.
    // This is so that cache affinity can work, and each file(split) is executed/cached on always the same LLAP daemon.
    MapWork mapWork = LlapHiveUtils.findMapWork((JobConf) conf);
    if (mapWork != null && mapWork.getCacheAffinity()) {
      // Iceberg splits logically consist of buckets, where the bucket size equals to openFileCost setting if the files
      // assigned to such bucket are smaller. This is how Iceberg would combine multiple files into one split, so here
      // we need to enforce the bucket size to be equal to split size to avoid file combination.
      Long openFileCost = splitSize > 0 ? splitSize : TableProperties.SPLIT_SIZE_DEFAULT;
      scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, String.valueOf(openFileCost));
    }
    //  TODO: Currently, this projection optimization stored on scan is not being used effectively on Hive side, as
    //   Hive actually uses conf to propagate the projected columns to let the final reader to read the only
    //   projected columns data. See IcebergInputFormat::readSchema(Configuration conf, Table table, boolean
    //   caseSensitive). But we can consider using this projection optimization stored on scan in the future when
    //   needed.
    Schema readSchema = InputFormatConfig.readSchema(conf);
    if (readSchema != null) {
      scan = scan.project(readSchema);
    } else {
      String[] selectedColumns = InputFormatConfig.selectedColumns(conf);
      if (selectedColumns != null) {
        scan = scan.select(selectedColumns);
      }
    }

    // TODO add a filter parser to get rid of Serialization
    Expression filter = SerializationUtil.deserializeFromBase64(conf.get(InputFormatConfig.FILTER_EXPRESSION));
    if (filter != null) {
      // In order to prevent the filter expression to be attached to every file scan task generated we call
      // ignoreResiduals() here. The passed in filter will still be effective during split generation.
      // On the execution side residual expressions will be mined from the passed job conf.
      scan = scan.filter(filter).ignoreResiduals();
    }
    return scan;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    Configuration conf = context.getConfiguration();
    Table table = Optional
        .ofNullable(HiveIcebergStorageHandler.table(conf, conf.get(InputFormatConfig.TABLE_IDENTIFIER)))
        .orElseGet(() -> {
          Table tbl = Catalogs.loadTable(conf);
          conf.set(InputFormatConfig.TABLE_IDENTIFIER, tbl.name());
          conf.set(InputFormatConfig.SERIALIZED_TABLE_PREFIX + tbl.name(), SerializationUtil.serializeToBase64(tbl));
          return tbl;
        });
    final ExecutorService workerPool =
        ThreadPools.newWorkerPool("iceberg-plan-worker-pool",
            conf.getInt(SystemConfigs.WORKER_THREAD_POOL_SIZE.propertyKey(), ThreadPools.WORKER_THREAD_POOL_SIZE));
    try {
      return planInputSplits(table, conf, workerPool);
    } finally {
      workerPool.shutdown();
    }
  }

  private List<InputSplit> planInputSplits(Table table, Configuration conf, ExecutorService workerPool) {
    List<InputSplit> splits = Lists.newArrayList();
    boolean applyResidual = !conf.getBoolean(InputFormatConfig.SKIP_RESIDUAL_FILTERING, false);
    InputFormatConfig.InMemoryDataModel model = conf.getEnum(InputFormatConfig.IN_MEMORY_DATA_MODEL,
        InputFormatConfig.InMemoryDataModel.GENERIC);

    long fromVersion = conf.getLong(InputFormatConfig.SNAPSHOT_ID_INTERVAL_FROM, -1);
    Scan<? extends Scan, FileScanTask, CombinedScanTask> scan;
    if (fromVersion != -1) {
      scan = applyConfig(conf, createIncrementalAppendScan(table, conf));
    } else {
      scan = applyConfig(conf, createTableScan(table, conf));
    }
    scan = scan.planWith(workerPool);

    boolean allowDataFilesWithinTableLocationOnly =
        conf.getBoolean(HiveConf.ConfVars.HIVE_ICEBERG_ALLOW_DATAFILES_IN_TABLE_LOCATION_ONLY.varname,
            HiveConf.ConfVars.HIVE_ICEBERG_ALLOW_DATAFILES_IN_TABLE_LOCATION_ONLY.defaultBoolVal);
    Path tableLocation = new Path(conf.get(InputFormatConfig.TABLE_LOCATION));


    try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
      tasksIterable.forEach(task -> {
        if (applyResidual && (model == InputFormatConfig.InMemoryDataModel.HIVE ||
            model == InputFormatConfig.InMemoryDataModel.PIG)) {
          // TODO: We do not support residual evaluation for HIVE and PIG in memory data model yet
          checkResiduals(task);
        }
        if (allowDataFilesWithinTableLocationOnly) {
          validateFileLocations(task, tableLocation);
        }
        splits.add(new IcebergSplit(conf, task));
      });
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to close table scan: %s", scan), e);
    }

    // If enabled, do not serialize FileIO hadoop config to decrease split size
    // However, do not skip serialization for metatable queries, because some metadata tasks cache the IO object and we
    // wouldn't be able to inject the config into these tasks on the deserializer-side, unlike for standard queries
    if (scan instanceof DataTableScan) {
      HiveIcebergStorageHandler.checkAndSkipIoConfigSerialization(conf, table);
    }

    return splits;
  }

  private static void validateFileLocations(CombinedScanTask split, Path tableLocation) {
    for (FileScanTask fileScanTask : split.files()) {
      if (!FileUtils.isPathWithinSubtree(new Path(fileScanTask.file().path().toString()), tableLocation)) {
        throw new AuthorizationException("The table contains paths which are outside the table location");
      }
    }
  }

  private static void checkResiduals(CombinedScanTask task) {
    task.files().forEach(fileScanTask -> {
      Expression residual = fileScanTask.residual();
      if (residual != null && !residual.equals(Expressions.alwaysTrue())) {
        throw new UnsupportedOperationException(
            String.format(
                "Filter expression %s is not completely satisfied. Additional rows " +
                    "can be returned not satisfied by the filter expression", residual));
      }
    });
  }

  @Override
  public RecordReader<Void, T> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new IcebergRecordReader<>();
  }

  private static final class IcebergRecordReader<T> extends RecordReader<Void, T> {

    private static final String HIVE_VECTORIZED_READER_CLASS = "org.apache.iceberg.mr.hive.vector.HiveVectorizedReader";
    private static final DynMethods.StaticMethod HIVE_VECTORIZED_READER_BUILDER;

    static {
      if (HiveVersion.min(HiveVersion.HIVE_3)) {
        HIVE_VECTORIZED_READER_BUILDER = DynMethods.builder("reader")
            .impl(HIVE_VECTORIZED_READER_CLASS,
                Table.class,
                Path.class,
                FileScanTask.class,
                Map.class,
                TaskAttemptContext.class,
                Expression.class,
                Schema.class)
            .buildStatic();
      } else {
        HIVE_VECTORIZED_READER_BUILDER = null;
      }
    }

    private TaskAttemptContext context;
    private Configuration conf;
    private Schema expectedSchema;
    private String nameMapping;
    private boolean reuseContainers;
    private boolean caseSensitive;
    private InputFormatConfig.InMemoryDataModel inMemoryDataModel;
    private Iterator<FileScanTask> tasks;
    private T current;
    private CloseableIterator<T> currentIterator;
    private Table table;
    private boolean fetchVirtualColumns;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext newContext) {
      // For now IcebergInputFormat does its own split planning and does not accept FileSplit instances
      CombinedScanTask task = ((IcebergSplit) split).task();
      this.context = newContext;
      this.conf = newContext.getConfiguration();
      this.table = SerializationUtil.deserializeFromBase64(
                conf.get(InputFormatConfig.SERIALIZED_TABLE_PREFIX + conf.get(InputFormatConfig.TABLE_IDENTIFIER)));
      HiveIcebergStorageHandler.checkAndSetIoConfig(conf, table);
      this.tasks = task.files().iterator();
      this.nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
      this.caseSensitive = conf.getBoolean(InputFormatConfig.CASE_SENSITIVE, InputFormatConfig.CASE_SENSITIVE_DEFAULT);
      this.expectedSchema = readSchema(conf, table, caseSensitive);
      this.reuseContainers = conf.getBoolean(InputFormatConfig.REUSE_CONTAINERS, false);
      this.inMemoryDataModel = conf.getEnum(InputFormatConfig.IN_MEMORY_DATA_MODEL,
              InputFormatConfig.InMemoryDataModel.GENERIC);
      this.fetchVirtualColumns = InputFormatConfig.fetchVirtualColumns(conf);
      this.currentIterator = nextTask();
    }

    private CloseableIterator<T> nextTask() {
      CloseableIterator<T> closeableIterator = open(tasks.next(), expectedSchema).iterator();
      if (!fetchVirtualColumns || Utilities.getIsVectorized(conf)) {
        return closeableIterator;
      }
      return new IcebergAcidUtil.VirtualColumnAwareIterator<T>(closeableIterator, expectedSchema, conf);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      while (true) {
        if (currentIterator.hasNext()) {
          current = currentIterator.next();
          return true;
        } else if (tasks.hasNext()) {
          currentIterator.close();
          this.currentIterator = nextTask();
        } else {
          currentIterator.close();
          return false;
        }
      }
    }

    @Override
    public Void getCurrentKey() {
      return null;
    }

    @Override
    public T getCurrentValue() {
      return current;
    }

    @Override
    public float getProgress() {
      // TODO: We could give a more accurate progress based on records read from the file. Context.getProgress does not
      // have enough information to give an accurate progress value. This isn't that easy, since we don't know how much
      // of the input split has been processed and we are pushing filters into Parquet and ORC. But we do know when a
      // file is opened and could count the number of rows returned, so we can estimate. And we could also add a row
      // count to the readers so that we can get an accurate count of rows that have been either returned or filtered
      // out.
      return context.getProgress();
    }

    @Override
    public void close() throws IOException {
      currentIterator.close();
    }

    private CloseableIterable<T> openVectorized(FileScanTask task, Schema readSchema) {
      Preconditions.checkArgument(!task.file().format().equals(FileFormat.AVRO),
          "Vectorized execution is not yet supported for Iceberg avro tables. " +
              "Please turn off vectorization and retry the query.");
      Preconditions.checkArgument(HiveVersion.min(HiveVersion.HIVE_3),
          "Vectorized read is unsupported for Hive 2 integration.");

      Path path = new Path(task.file().path().toString());
      Map<Integer, ?> idToConstant = constantsMap(task, HiveIdentityPartitionConverters::convertConstant);
      Expression residual = HiveIcebergInputFormat.residualForTask(task, context.getConfiguration());

      // TODO: We have to take care of the EncryptionManager when LLAP and vectorization is used
      CloseableIterable<T> iterator = HIVE_VECTORIZED_READER_BUILDER.invoke(table, path, task,
          idToConstant, context, residual, readSchema);

      return applyResidualFiltering(iterator, residual, readSchema);
    }

    private CloseableIterable<T> openGeneric(FileScanTask task, Schema readSchema) {
      if (task.isDataTask()) {
        // When querying metadata tables, the currentTask is a DataTask and the data has to
        // be fetched from the task instead of reading it from files.
        IcebergInternalRecordWrapper wrapper =
            new IcebergInternalRecordWrapper(table.schema().asStruct(), readSchema.asStruct());
        return (CloseableIterable) CloseableIterable.transform(((DataTask) task).rows(), row -> wrapper.wrap(row));
      }

      DataFile file = task.file();
      InputFile inputFile = table.encryption().decrypt(EncryptedFiles.encryptedInput(
          table.io().newInputFile(file.path().toString()),
          file.keyMetadata()));

      CloseableIterable<T> iterable;
      switch (file.format()) {
        case AVRO:
          iterable = newAvroIterable(inputFile, task, readSchema);
          break;
        case ORC:
          iterable = newOrcIterable(inputFile, task, readSchema);
          break;
        case PARQUET:
          iterable = newParquetIterable(inputFile, task, readSchema);
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Cannot read %s file: %s", file.format().name(), file.path()));
      }

      return iterable;
    }

    @SuppressWarnings("unchecked")
    private CloseableIterable<T> open(FileScanTask currentTask, Schema readSchema) {
      switch (inMemoryDataModel) {
        case PIG:
          // TODO: Support Pig and Hive object models for IcebergInputFormat
          throw new UnsupportedOperationException("Pig and Hive object models are not supported.");
        case HIVE:
          return openVectorized(currentTask, readSchema);
        case GENERIC:
          DeleteFilter deletes = new GenericDeleteFilter(table.io(), currentTask, table.schema(), readSchema);
          Schema requiredSchema = deletes.requiredSchema();
          return deletes.filter(openGeneric(currentTask, requiredSchema));
        default:
          throw new UnsupportedOperationException("Unsupported memory model");
      }
    }

    private CloseableIterable<T> applyResidualFiltering(CloseableIterable<T> iter, Expression residual,
                                                        Schema readSchema) {
      boolean applyResidual = !context.getConfiguration().getBoolean(InputFormatConfig.SKIP_RESIDUAL_FILTERING, false);

      if (applyResidual && residual != null && residual != Expressions.alwaysTrue()) {
        // Date and timestamp values are not the correct type for Evaluator.
        // Wrapping to return the expected type.
        InternalRecordWrapper wrapper = new InternalRecordWrapper(readSchema.asStruct());
        Evaluator filter = new Evaluator(readSchema.asStruct(), residual, caseSensitive);
        return CloseableIterable.filter(iter, record -> filter.eval(wrapper.wrap((StructLike) record)));
      } else {
        return iter;
      }
    }

    private CloseableIterable<T> newAvroIterable(
        InputFile inputFile, FileScanTask task, Schema readSchema) {
      Expression residual = HiveIcebergInputFormat.residualForTask(task, context.getConfiguration());
      Avro.ReadBuilder avroReadBuilder = Avro.read(inputFile)
          .project(readSchema)
          .split(task.start(), task.length());

      if (reuseContainers) {
        avroReadBuilder.reuseContainers();
      }

      if (nameMapping != null) {
        avroReadBuilder.withNameMapping(NameMappingParser.fromJson(nameMapping));
      }

      avroReadBuilder.createReaderFunc(
          (expIcebergSchema, expAvroSchema) ->
              DataReader.create(expIcebergSchema, expAvroSchema,
                  constantsMap(task, IdentityPartitionConverters::convertConstant)));

      return applyResidualFiltering(avroReadBuilder.build(), residual, readSchema);
    }

    private CloseableIterable<T> newParquetIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
      Expression residual = HiveIcebergInputFormat.residualForTask(task, context.getConfiguration());

      Parquet.ReadBuilder parquetReadBuilder = Parquet.read(inputFile)
          .project(readSchema)
          .filter(residual)
          .caseSensitive(caseSensitive)
          .split(task.start(), task.length());

      if (reuseContainers) {
        parquetReadBuilder.reuseContainers();
      }

      if (nameMapping != null) {
        parquetReadBuilder.withNameMapping(NameMappingParser.fromJson(nameMapping));
      }

      parquetReadBuilder.createReaderFunc(
          fileSchema -> GenericParquetReaders.buildReader(
              readSchema, fileSchema, constantsMap(task, IdentityPartitionConverters::convertConstant)));

      return applyResidualFiltering(parquetReadBuilder.build(), residual, readSchema);
    }

    private CloseableIterable<T> newOrcIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
      Map<Integer, ?> idToConstant = constantsMap(task, IdentityPartitionConverters::convertConstant);
      Schema readSchemaWithoutConstantAndMetadataFields = schemaWithoutConstantsAndMeta(readSchema, idToConstant);
      Expression residual = HiveIcebergInputFormat.residualForTask(task, context.getConfiguration());

      ORC.ReadBuilder orcReadBuilder = ORC.read(inputFile)
          .project(readSchemaWithoutConstantAndMetadataFields)
          .filter(residual)
          .caseSensitive(caseSensitive)
          .split(task.start(), task.length());

      if (nameMapping != null) {
        orcReadBuilder.withNameMapping(NameMappingParser.fromJson(nameMapping));
      }

      orcReadBuilder.createReaderFunc(
          fileSchema -> GenericOrcReader.buildReader(
              readSchema, fileSchema, idToConstant));

      return applyResidualFiltering(orcReadBuilder.build(), residual, readSchema);
    }

    private Map<Integer, ?> constantsMap(FileScanTask task, BiFunction<Type, Object, Object> converter) {
      PartitionSpec spec = task.spec();
      Set<Integer> idColumns = spec.identitySourceIds();
      Schema partitionSchema = TypeUtil.select(expectedSchema, idColumns);
      boolean projectsIdentityPartitionColumns = !partitionSchema.columns().isEmpty();
      if (expectedSchema.findField(MetadataColumns.PARTITION_COLUMN_ID) != null) {
        Types.StructType partitionType = Partitioning.partitionType(table);
        return PartitionUtil.constantsMap(task, partitionType, converter);
      } else if (projectsIdentityPartitionColumns) {
        Types.StructType partitionType = Partitioning.partitionType(table);
        return PartitionUtil.constantsMap(task, partitionType, converter);
      } else {
        return Collections.emptyMap();
      }
    }

    private static Schema readSchema(Configuration conf, Table table, boolean caseSensitive) {
      Schema readSchema = InputFormatConfig.readSchema(conf);

      if (readSchema != null) {
        return readSchema;
      }

      String[] selectedColumns = InputFormatConfig.selectedColumns(conf);
      readSchema = table.schema();

      if (selectedColumns != null) {
        readSchema =
            caseSensitive ? readSchema.select(selectedColumns) : readSchema.caseInsensitiveSelect(selectedColumns);
      }

      if (InputFormatConfig.fetchVirtualColumns(conf)) {
        return IcebergAcidUtil.createFileReadSchemaWithVirtualColums(readSchema.columns(), table);
      }

      return readSchema;
    }

    private static Schema schemaWithoutConstantsAndMeta(Schema readSchema, Map<Integer, ?> idToConstant) {
      // remove the nested fields of the partition struct
      Set<Integer> partitionFields = Optional.ofNullable(readSchema.findField(MetadataColumns.PARTITION_COLUMN_ID))
          .map(Types.NestedField::type)
          .map(Type::asStructType)
          .map(Types.StructType::fields)
          .map(fields -> fields.stream().map(Types.NestedField::fieldId).collect(Collectors.toSet()))
          .orElseGet(Collections::emptySet);

      // remove constants and meta columns too
      Set<Integer> collect = Stream.of(idToConstant.keySet(), MetadataColumns.metadataFieldIds(), partitionFields)
          .flatMap(Set::stream)
          .collect(Collectors.toSet());

      return TypeUtil.selectNot(readSchema, collect);
    }
  }
}
