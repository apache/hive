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

import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.BaseFileWriterFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.VariantShreddingFunction;
import org.apache.iceberg.parquet.VariantUtil;

class HiveFileWriterFactory extends BaseFileWriterFactory<Record> {

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
      SortOrder equalityDeleteSortOrder,
      Schema positionDeleteRowSchema) {
    super(
        table,
        dataFileFormat,
        dataSchema,
        dataSortOrder,
        deleteFileFormat,
        equalityFieldIds,
        equalityDeleteRowSchema,
        equalityDeleteSortOrder,
        positionDeleteRowSchema);
    properties = table.properties();
  }

  static Builder builderFor(Table table) {
    return new Builder(table);
  }

  @Override
  protected void configureDataWrite(Avro.DataWriteBuilder builder) {
    builder.createWriterFunc(DataWriter::create);
  }

  @Override
  protected void configureEqualityDelete(Avro.DeleteWriteBuilder builder) {

  }

  @Override
  protected void configurePositionDelete(Avro.DeleteWriteBuilder builder) {
    builder.createWriterFunc(DataWriter::create);
  }

  @Override
  protected void configureDataWrite(Parquet.DataWriteBuilder builder) {
    builder.createWriterFunc(GenericParquetWriter::create);
    // Configure variant shredding if enabled and a sample record is available
    if (VariantUtil.shouldUseVariantShredding(properties, dataSchema())) {
      setVariantShreddingFunc(builder, VariantUtil.variantShreddingFunc(sampleRecord, dataSchema()));
    }
  }

  /**
   * Sets a {@link VariantShreddingFunction} on the underlying Parquet write builder.
   *
   * <p>{@link Parquet.DataWriteBuilder} does not expose {@code variantShreddingFunc} directly; it is set on an
   * internal write builder held in the private {@code appenderBuilder} field. This method uses reflection to
   * access that internal builder and invoke {@code variantShreddingFunc(VariantShreddingFunction)}.
   *
   * TODO: Replace with {@code DataWriteBuilder.variantShreddingFunc(VariantShreddingFunction)}
   * once it becomes publicly available.
   */
  private static void setVariantShreddingFunc(Parquet.DataWriteBuilder dataWriteBuilder,
      VariantShreddingFunction fn) {
    try {
      java.lang.reflect.Field field = dataWriteBuilder.getClass().getDeclaredField("appenderBuilder");
      field.setAccessible(true);
      Object writeBuilder = field.get(dataWriteBuilder);
      writeBuilder.getClass()
          .getMethod("variantShreddingFunc", VariantShreddingFunction.class)
          .invoke(writeBuilder, fn);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void configureEqualityDelete(Parquet.DeleteWriteBuilder builder) {

  }

  @Override
  protected void configurePositionDelete(Parquet.DeleteWriteBuilder builder) {
    builder.createWriterFunc(GenericParquetWriter::create);
  }

  @Override
  protected void configureDataWrite(ORC.DataWriteBuilder builder) {
    builder.createWriterFunc(GenericOrcWriter::buildWriter);
  }

  @Override
  protected void configureEqualityDelete(ORC.DeleteWriteBuilder deleteWriteBuilder) {

  }

  @Override
  protected void configurePositionDelete(ORC.DeleteWriteBuilder deleteWriteBuilder) {
    deleteWriteBuilder.createWriterFunc(GenericOrcWriter::buildWriter);
  }

  static class Builder {
    private final Table table;
    private FileFormat dataFileFormat;
    private Schema dataSchema;
    private FileFormat deleteFileFormat;
    private Schema positionDeleteRowSchema;

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

    Builder positionDeleteRowSchema(Schema newPositionDeleteRowSchema) {
      this.positionDeleteRowSchema = newPositionDeleteRowSchema;
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
          null,
          positionDeleteRowSchema);
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
