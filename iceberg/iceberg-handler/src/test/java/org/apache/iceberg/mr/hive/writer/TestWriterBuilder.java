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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.ql.Context;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestWriterBuilder extends HiveIcebergWriterTestBase {
  // OutputFileFactory file name: <partitionId>-<taskId>-<operationId>-<fileCount>, where the operationId is
  // <queryId>-<jobId>-<uuid>
  private static final Pattern DATA_FILE_NAME = Pattern.compile(
      "\\d{5}-\\d+-Q_ID-job_test_0000-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-\\d{5}\\..+");

  // The operationId does not depend on the file format or partitioning, so a single run is enough
  @Parameterized.Parameters(name = "fileFormat={0}, partitioned={1}")
  public static Collection<Object[]> parameters() {
    return Collections.singletonList(new Object[] { FileFormat.PARQUET, false });
  }

  @Test
  public void testOperationIdIsUniquePerWriter() throws IOException {
    // Writers built for the same task attempt share the partitionId and taskId of the file name,
    // so only the operationId keeps their data files apart
    List<String> firstFiles = writeAndGetDataFiles();
    List<String> secondFiles = writeAndGetDataFiles();

    Assert.assertFalse("First writer should produce data files", firstFiles.isEmpty());
    Assert.assertFalse("Second writer should produce data files", secondFiles.isEmpty());

    Set<String> overlap = Sets.newHashSet(firstFiles);
    overlap.retainAll(secondFiles);
    Assert.assertTrue("Writers should not produce the same data files: " + overlap, overlap.isEmpty());

    List<String> allFiles = Lists.newArrayList(firstFiles);
    allFiles.addAll(secondFiles);
    for (String path : allFiles) {
      String fileName = path.substring(path.lastIndexOf('/') + 1);
      Assert.assertTrue("Unexpected data file name: " + fileName, DATA_FILE_NAME.matcher(fileName).matches());
    }
  }

  private List<String> writeAndGetDataFiles() throws IOException {
    HiveIcebergWriter writer = writerBuilder.operation(Context.Operation.OTHER).build();

    Container<Record> container = new Container<>();
    container.set(RECORDS.get(0));
    writer.write(container);
    writer.close(false);

    return writer.files().dataFiles().stream()
        .map(DataFile::location)
        .toList();
  }
}
