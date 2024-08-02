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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public final class IcebergMergeRecordReader<T> extends AbstractIcebergRecordReader<T> {
  private IcebergMergeSplit mergeSplit;
  private CloseableIterator<T> currentIterator;
  private T current;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext newContext) {
    // For now IcebergInputFormat does its own split planning and does not accept FileSplit instances
    super.initialize(split, newContext);
    mergeSplit = (IcebergMergeSplit) split;
    this.currentIterator = nextTask();
  }

  private CloseableIterator<T> nextTask() {
    return openGeneric(mergeSplit.getContentFile(), getTable().schema()).iterator();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    while (true) {
      if (currentIterator.hasNext()) {
        current = currentIterator.next();
        return true;
      } else {
        currentIterator.close();
        return false;
      }
    }
  }

  @Override
  public T getCurrentValue() {
    return current;
  }

  @Override
  public void close() throws IOException {
    currentIterator.close();
  }

  private CloseableIterable<T> openGeneric(ContentFile contentFile, Schema readSchema) {
    InputFile inputFile = null;
    Schema schema = null;
    if (contentFile instanceof DataFile) {
      DataFile dataFile = (DataFile) contentFile;
      inputFile = getTable().encryption().decrypt(EncryptedFiles.encryptedInput(
              getTable().io().newInputFile(dataFile.path().toString()),
              dataFile.keyMetadata()));
      schema = readSchema;
    }
    CloseableIterable<T> iterable;
    switch (contentFile.format()) {
      case AVRO:
        iterable = newAvroIterable(inputFile, schema);
        break;
      case ORC:
        iterable = newOrcIterable(inputFile, schema);
        break;
      case PARQUET:
        iterable = newParquetIterable(inputFile, schema);
        break;
      default:
        throw new UnsupportedOperationException(
          String.format("Cannot read %s file: %s", contentFile.format().name(), contentFile.path()));
    }

    return iterable;
  }

  private CloseableIterable<T> newAvroIterable(
          InputFile inputFile, Schema readSchema) {
    Avro.ReadBuilder avroReadBuilder = Avro.read(inputFile)
        .project(readSchema)
        .split(mergeSplit.getStart(), mergeSplit.getLength());

    if (isReuseContainers()) {
      avroReadBuilder.reuseContainers();
    }

    if (getNameMapping() != null) {
      avroReadBuilder.withNameMapping(NameMappingParser.fromJson(getNameMapping()));
    }

    avroReadBuilder.createReaderFunc(
        (expIcebergSchema, expAvroSchema) ->
             DataReader.create(expIcebergSchema, expAvroSchema, Maps.newHashMap()));

    return applyResidualFiltering(avroReadBuilder.build(), null, readSchema);
  }

  private CloseableIterable<T> newOrcIterable(InputFile inputFile, Schema readSchema) {
    ORC.ReadBuilder orcReadBuilder = ORC.read(inputFile)
        .project(readSchema)
        .caseSensitive(isCaseSensitive())
        .split(mergeSplit.getStart(), mergeSplit.getLength());

    if (getNameMapping() != null) {
      orcReadBuilder.withNameMapping(NameMappingParser.fromJson(getNameMapping()));
    }

    orcReadBuilder.createReaderFunc(
        fileSchema -> GenericOrcReader.buildReader(
            readSchema, fileSchema, Maps.newHashMap()));

    return applyResidualFiltering(orcReadBuilder.build(), null, readSchema);
  }

  private CloseableIterable<T> newParquetIterable(InputFile inputFile, Schema readSchema) {

    Parquet.ReadBuilder parquetReadBuilder = Parquet.read(inputFile)
        .project(readSchema)
        .caseSensitive(isCaseSensitive())
        .split(mergeSplit.getStart(), mergeSplit.getLength());

    if (isReuseContainers()) {
      parquetReadBuilder.reuseContainers();
    }

    if (getNameMapping() != null) {
      parquetReadBuilder.withNameMapping(NameMappingParser.fromJson(getNameMapping()));
    }

    parquetReadBuilder.createReaderFunc(
        fileSchema -> GenericParquetReaders.buildReader(
            readSchema, fileSchema, Maps.newHashMap()));

    return applyResidualFiltering(parquetReadBuilder.build(), null, readSchema);
  }
}
