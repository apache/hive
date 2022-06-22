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

class HiveFileWriterFactory extends BaseFileWriterFactory<Record> {

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
    builder.createWriterFunc(GenericParquetWriter::buildWriter);
  }

  @Override
  protected void configureEqualityDelete(Parquet.DeleteWriteBuilder builder) {

  }

  @Override
  protected void configurePositionDelete(Parquet.DeleteWriteBuilder builder) {
    builder.createWriterFunc(GenericParquetWriter::buildWriter);
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
}
