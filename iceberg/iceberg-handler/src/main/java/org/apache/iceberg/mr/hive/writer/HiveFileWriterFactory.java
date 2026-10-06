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

package org.apache.iceberg.mr.hive.writer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.VariantUtil;

class HiveFileWriterFactory implements FileWriterFactory<Record> {

  private final GenericFileWriterFactory delegate;
  private final Table table;
  private final FileFormat dataFileFormat;
  private final Schema dataSchema;
  private final Map<String, String> properties;
  private Record sampleRecord = null;

  HiveFileWriterFactory(
      Table table,
      FileFormat dataFileFormat,
      Schema dataSchema,
      SortOrder dataSortOrder,
      FileFormat deleteFileFormat,
      int[] equalityFieldIds,
      Schema equalityDeleteRowSchema,
      SortOrder equalityDeleteSortOrder) {
    this.table = table;
    this.dataFileFormat = dataFileFormat;
    this.dataSchema = dataSchema;
    this.properties = table.properties();

    GenericFileWriterFactory.Builder builder = new GenericFileWriterFactory.Builder(table)
        .dataFileFormat(dataFileFormat)
        .dataSchema(dataSchema)
        .dataSortOrder(dataSortOrder)
        .deleteFileFormat(deleteFileFormat)
        .equalityFieldIds(equalityFieldIds)
        .equalityDeleteRowSchema(equalityDeleteRowSchema)
        .equalityDeleteSortOrder(equalityDeleteSortOrder);

    this.delegate = builder.build();
  }

  static Builder builderFor(Table table) {
    return new Builder(table);
  }

  @Override
  public DataWriter<Record> newDataWriter(EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    if (dataFileFormat == FileFormat.PARQUET && VariantUtil.shouldUseVariantShredding(properties, dataSchema)) {
      try {
        return Parquet.writeData(file)
            .schema(dataSchema)
            .createWriterFunc(GenericParquetWriter::create)
            .setAll(properties)
            .metricsConfig(MetricsConfig.forTable(table))
            .withSpec(spec)
            .withPartition(partition)
            .withKeyMetadata(file.keyMetadata())
            .variantShreddingFunc(VariantUtil.variantShreddingFunc(sampleRecord, dataSchema))
            .build();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return delegate.newDataWriter(file, spec, partition);
  }

  @Override
  public EqualityDeleteWriter<Record> newEqualityDeleteWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    return delegate.newEqualityDeleteWriter(file, spec, partition);
  }

  @Override
  public PositionDeleteWriter<Record> newPositionDeleteWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    return delegate.newPositionDeleteWriter(file, spec, partition);
  }

  static class Builder {
    private final Table table;
    private FileFormat dataFileFormat;
    private Schema dataSchema;
    private FileFormat deleteFileFormat;

    Builder(Table table) {
      this.table = table;
    }

    Builder dataFileFormat(FileFormat newDataFileFormat) {
      this.dataFileFormat = newDataFileFormat;
      return this;
    }

    Builder dataSchema(Schema newDataSchema) {
      this.dataSchema = newDataSchema;
      return this;
    }

    Builder deleteFileFormat(FileFormat newDeleteFileFormat) {
      this.deleteFileFormat = newDeleteFileFormat;
      return this;
    }

    HiveFileWriterFactory build() {
      return new HiveFileWriterFactory(
          table,
          dataFileFormat,
          dataSchema,
          null,
          deleteFileFormat,
          null,
          null,
          null);
    }
  }

  /**
   * Set a sample record to use for data-driven variant shredding schema generation.
   * Should be called before the Parquet writer is created.
   */
  public void initialize(Record record) {
    if (sampleRecord == null) {
      sampleRecord = record;
    }
  }
}
