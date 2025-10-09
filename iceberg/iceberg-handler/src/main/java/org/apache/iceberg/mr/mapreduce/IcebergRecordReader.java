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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.data.CachingDeleteLoader;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hive.HiveVersion;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.mr.hive.HiveIcebergInputFormat;
import org.apache.iceberg.mr.hive.IcebergAcidUtil;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;

public final class IcebergRecordReader<T> extends AbstractIcebergRecordReader<T> {

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

  private Iterator<FileScanTask> tasks;
  private CloseableIterator<T> currentIterator;
  private T current;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext newContext) {
    // For now IcebergInputFormat does its own split planning and does not accept FileSplit instances
    super.initialize(split, newContext);
    ScanTaskGroup<FileScanTask> taskGroup = ((IcebergSplit) split).taskGroup();
    this.tasks = taskGroup.tasks().iterator();
    this.currentIterator = nextTask();
  }

  private CloseableIterator<T> nextTask() {
    CloseableIterator<T> closeableIterator = open(tasks.next(), expectedSchema).iterator();
    if (!isFetchVirtualColumns() || Utilities.getIsVectorized(conf)) {
      return closeableIterator;
    }
    return new IcebergAcidUtil.VirtualColumnAwareIterator<>(closeableIterator,
        expectedSchema, conf);
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    while (!currentIterator.hasNext()) {
      currentIterator.close();
      if (!tasks.hasNext()) {
        return false;
      }
      currentIterator = nextTask();
    }
    current = currentIterator.next();
    return true;
  }

  @Override
  public T getCurrentValue() {
    return current;
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

    Path path = new Path(task.file().location());
    Map<Integer, ?> idToConstant = constantsMap(task, HiveIdentityPartitionConverters::convertConstant);
    Expression residual = HiveIcebergInputFormat.residualForTask(task, getContext().getConfiguration());

    // TODO: We have to take care of the EncryptionManager when LLAP and vectorization is used
    CloseableIterable<T> iterator = HIVE_VECTORIZED_READER_BUILDER.invoke(table, path, task,
        idToConstant, getContext(), residual, readSchema);

    return applyResidualFiltering(iterator, residual, readSchema);
  }

  @SuppressWarnings("unchecked")
  private CloseableIterable<T> open(FileScanTask currentTask, Schema readSchema) {
    switch (getInMemoryDataModel()) {
      case HIVE:
        return openVectorized(currentTask, readSchema);
      case GENERIC:
        DeleteFilter<Record> deletes = new GenericDeleteFilter(table.io(), currentTask, table.schema(), readSchema) {
          @Override
          protected DeleteLoader newDeleteLoader() {
              return new CachingDeleteLoader(this::loadInputFile, conf);
          }
        };
        Schema requiredSchema = deletes.requiredSchema();
        return deletes.filter(openGeneric(currentTask, requiredSchema));
      default:
        throw new UnsupportedOperationException("Unsupported memory model");
    }
  }

  private CloseableIterable openGeneric(FileScanTask task, Schema readSchema) {
    if (task.isDataTask()) {
      // When querying metadata tables, the currentTask is a DataTask and the data has to
      // be fetched from the task instead of reading it from files.
      IcebergInternalRecordWrapper wrapper =
          new IcebergInternalRecordWrapper(readSchema.asStruct());
      return CloseableIterable.transform(((DataTask) task).rows(), wrapper::wrap);
    }

    DataFile file = task.file();
    InputFile inputFile = table.encryption().decrypt(
        EncryptedFiles.encryptedInput(table.io().newInputFile(file.location()),
        file.keyMetadata()));
    Expression residual = HiveIcebergInputFormat.residualForTask(task, getContext().getConfiguration());

    CloseableIterable<T> iterable = switch (file.format()) {
      case AVRO -> newAvroIterable(inputFile, task, readSchema);
      case ORC -> newOrcIterable(inputFile, task, residual, readSchema);
      case PARQUET -> newParquetIterable(inputFile, task, residual, readSchema);
      default -> throw new UnsupportedOperationException(
          String.format("Cannot read %s file: %s", file.format().name(), file.location()));
    };
    return applyResidualFiltering(iterable, residual, readSchema);
  }

  private CloseableIterable<T> newAvroIterable(
      InputFile inputFile, FileScanTask task, Schema readSchema) {
    Avro.ReadBuilder avroReadBuilder = Avro.read(inputFile)
        .project(readSchema)
        .split(task.start(), task.length());

    if (isReuseContainers()) {
      avroReadBuilder.reuseContainers();
    }
    if (getNameMapping() != null) {
      avroReadBuilder.withNameMapping(NameMappingParser.fromJson(getNameMapping()));
    }
    avroReadBuilder.createResolvingReader(
        schema -> PlannedDataReader.create(schema,
            constantsMap(task, IdentityPartitionConverters::convertConstant)));
    return avroReadBuilder.build();
  }

  private CloseableIterable<T> newParquetIterable(
      InputFile inputFile, FileScanTask task, Expression residual, Schema readSchema) {
    Parquet.ReadBuilder parquetReadBuilder = Parquet.read(inputFile)
        .project(readSchema)
        .filter(residual)
        .caseSensitive(isCaseSensitive())
        .split(task.start(), task.length());

    if (isReuseContainers()) {
      parquetReadBuilder.reuseContainers();
    }
    if (getNameMapping() != null) {
      parquetReadBuilder.withNameMapping(NameMappingParser.fromJson(getNameMapping()));
    }
    parquetReadBuilder.createReaderFunc(
        fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema,
            constantsMap(task, IdentityPartitionConverters::convertConstant)));
    return parquetReadBuilder.build();
  }

  private CloseableIterable<T> newOrcIterable(
      InputFile inputFile, FileScanTask task, Expression residual, Schema readSchema) {
    Map<Integer, ?> idToConstant =
        constantsMap(task, IdentityPartitionConverters::convertConstant);
    Schema readSchemaWithoutConstantAndMetadataFields =
        schemaWithoutConstantsAndMeta(readSchema, idToConstant);
    // ORC does not support reuse containers yet
    ORC.ReadBuilder orcReadBuilder = ORC.read(inputFile)
        .project(readSchemaWithoutConstantAndMetadataFields)
        .filter(residual)
        .caseSensitive(isCaseSensitive())
        .split(task.start(), task.length());

    if (getNameMapping() != null) {
      orcReadBuilder.withNameMapping(NameMappingParser.fromJson(getNameMapping()));
    }
    orcReadBuilder.createReaderFunc(
        fileSchema -> GenericOrcReader.buildReader(readSchema, fileSchema, idToConstant));
    return orcReadBuilder.build();
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
