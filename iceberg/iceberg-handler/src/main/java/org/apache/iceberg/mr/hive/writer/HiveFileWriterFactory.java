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
import org.apache.iceberg.types.Types;

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
    // Configure variant shredding function if conditions are met:
    if (hasVariantColumns(dataSchema()) && isVariantShreddingEnabled(properties)) {
      var shreddingFunction = Parquet.constructVariantShreddingFunction(sampleRecord, dataSchema());
      builder.variantShreddingFunc(shreddingFunction);
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
   * Check if the schema contains any variant columns.
   */
  private static boolean hasVariantColumns(Schema schema) {
    return schema.columns().stream()
        .anyMatch(field -> field.type() instanceof Types.VariantType);
  }

  /**
   * Check if variant shredding is enabled via table properties.
   */
  private static boolean isVariantShreddingEnabled(Map<String, String> properties) {
    String shreddingEnabled = properties.get("variant.shredding.enabled");
    return "true".equalsIgnoreCase(shreddingEnabled);
  }

  /**
   * Set a sample record to use for data-driven variant shredding schema generation.
   * Should be called before the Parquet writer is created.
   */
  public void initialize(Record record) {
    if (this.sampleRecord == null) {
      this.sampleRecord = record;
    }
  }
}
