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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestIcebergSplit {

  private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final int FILE_PATH_ID = MetadataColumns.DELETE_FILE_PATH.fieldId();
  private static final String DATA_FILE = "/table/data/data-0.parquet";

  @Test
  public void testDeletesThatAreNotFileScopedAreWrittenWithoutStats() throws IOException {
    DataFile dataFile = DataFiles.builder(SPEC)
        .withPath(DATA_FILE)
        .withFileSizeInBytes(100)
        .withMetrics(metrics(1, Conversions.toByteBuffer(Types.LongType.get(), 1L),
            Conversions.toByteBuffer(Types.LongType.get(), 9L)))
        .build();
    DeleteFile multiFile = positionDeletes("/table/data/deletes-0.parquet", "/table/data/a", "/table/data/z");
    DeleteFile fileScoped = positionDeletes("/table/data/deletes-1.parquet", DATA_FILE, DATA_FILE);
    DeleteFile dv = FileMetadata.deleteFileBuilder(SPEC)
        .ofPositionDeletes()
        .withFormat(FileFormat.PUFFIN)
        .withPath("/table/data/dv-0.puffin")
        .withFileSizeInBytes(50)
        .withRecordCount(2)
        .withReferencedDataFile(DATA_FILE)
        .withContentOffset(4)
        .withContentSizeInBytes(40)
        .build();
    FileScanTask task = new BaseFileScanTask(dataFile, new DeleteFile[] {multiFile, fileScoped, dv},
        SchemaParser.toJson(SCHEMA), PartitionSpecParser.toJson(SPEC),
        ResidualEvaluator.unpartitioned(Expressions.alwaysTrue()));

    FileScanTask read = roundTrip(new IcebergSplit(new Configuration(), new BaseCombinedScanTask(task)))
        .taskGroup().tasks().iterator().next();
    List<DeleteFile> deletes = read.deletes();

    Assert.assertNotNull(multiFile.lowerBounds());
    DeleteFile readMultiFile = deletes.get(0);
    Assert.assertEquals(multiFile.location(), readMultiFile.location());
    Assert.assertEquals(multiFile.recordCount(), readMultiFile.recordCount());
    Assert.assertNull(readMultiFile.columnSizes());
    Assert.assertNull(readMultiFile.valueCounts());
    Assert.assertNull(readMultiFile.nullValueCounts());
    Assert.assertNull(readMultiFile.lowerBounds());
    Assert.assertNull(readMultiFile.upperBounds());

    DeleteFile readFileScoped = deletes.get(1);
    Assert.assertEquals(fileScoped.lowerBounds(), readFileScoped.lowerBounds());
    Assert.assertEquals(DATA_FILE, ContentFileUtil.referencedDataFileLocation(readFileScoped));

    DeleteFile readDv = deletes.get(2);
    Assert.assertEquals(DATA_FILE, readDv.referencedDataFile());
    Assert.assertEquals(dv.contentOffset(), readDv.contentOffset());
    Assert.assertEquals(dv.contentSizeInBytes(), readDv.contentSizeInBytes());

    Assert.assertEquals(dataFile.lowerBounds(), read.file().lowerBounds());
    Assert.assertEquals(dataFile.valueCounts(), read.file().valueCounts());
  }

  private static DeleteFile positionDeletes(String path, String lowerFilePath, String upperFilePath) {
    return FileMetadata.deleteFileBuilder(SPEC)
        .ofPositionDeletes()
        .withPath(path)
        .withFileSizeInBytes(100)
        .withMetrics(metrics(FILE_PATH_ID, bound(lowerFilePath), bound(upperFilePath)))
        .build();
  }

  private static Metrics metrics(int fieldId, ByteBuffer lower, ByteBuffer upper) {
    return new Metrics(2L, Map.of(fieldId, 10L), Map.of(fieldId, 2L), Map.of(fieldId, 0L), null,
        Map.of(fieldId, lower), Map.of(fieldId, upper));
  }

  private static ByteBuffer bound(String value) {
    return Conversions.toByteBuffer(Types.StringType.get(), value);
  }

  private static IcebergSplit roundTrip(IcebergSplit split) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      split.write(out);
    }
    IcebergSplit read = new IcebergSplit();
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      read.readFields(in);
    }
    return read;
  }
}
