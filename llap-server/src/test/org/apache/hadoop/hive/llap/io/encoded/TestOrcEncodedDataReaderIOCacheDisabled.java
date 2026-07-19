/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.io.encoded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestOrcEncodedDataReaderIOCacheDisabled {

  private static final TypeDescription ORC_SCHEMA = TypeDescription.fromString("struct<id:bigint,value:string>");
  private static final int ROW_COUNT = 3;
  private static final long[] EXPECTED_IDS = {1L, 2L, 3L};
  private static final String[] EXPECTED_VALUES = {"one", "two", "three"};

  private static HiveConf daemonConf;
  private static Path orcFile;

  @BeforeClass
  public static void setUpClass() throws Exception {
    daemonConf = new HiveConf();
    HiveConf.setVar(daemonConf, ConfVars.LLAP_IO_MEMORY_MODE, "none");

    Path tmpDir = new Path(Files.createTempDirectory("llap-orc-no-io-cache").toString());
    orcFile = new Path(tmpDir, "data.orc");
    writeOrcFile(orcFile, daemonConf);

    LlapProxy.setDaemon(true);
    LlapProxy.initializeLlapIo(daemonConf);
    assertFalse(LlapProxy.getIo().usingLowLevelCache());
  }

  @AfterClass
  public static void tearDownClass() {
    LlapProxy.close();
  }

  @Test
  public void testVectorizedOrcEncodedReadWithIoCacheDisabled() throws Exception {
    JobConf job = buildJobConf(orcFile);
    long fileLen = orcFile.getFileSystem(job).getFileStatus(orcFile).getLen();

    RecordReader<NullWritable, VectorizedRowBatch> reader = LlapProxy.getIo().llapVectorizedOrcReaderForPath(
        null, orcFile, null, Arrays.asList(0, 1), job, 0, fileLen, Reporter.NULL);
    assertNotNull("LLAP should handle this ORC read", reader);

    try {
      VectorizedRowBatch batch = reader.createValue();
      int rowsRead = 0;
      while (reader.next(NullWritable.get(), batch)) {
        LongColumnVector idCol = (LongColumnVector) batch.cols[0];
        BytesColumnVector valueCol = (BytesColumnVector) batch.cols[1];
        for (int i = 0; i < batch.size; i++) {
          assertEquals("id at row " + rowsRead, EXPECTED_IDS[rowsRead], idCol.vector[i]);
          assertEquals("value at row " + rowsRead, EXPECTED_VALUES[rowsRead], valueCol.toString(i));
          rowsRead++;
        }
      }
      assertEquals(ROW_COUNT, rowsRead);
    } finally {
      reader.close();
    }
  }

  private static void writeOrcFile(Path path, HiveConf conf) throws IOException {
    try (Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).setSchema(ORC_SCHEMA))) {
      VectorizedRowBatch batch = ORC_SCHEMA.createRowBatch();
      LongColumnVector idCol = (LongColumnVector) batch.cols[0];
      BytesColumnVector valueCol = (BytesColumnVector) batch.cols[1];
      batch.size = ROW_COUNT;
      for (int i = 0; i < ROW_COUNT; i++) {
        idCol.vector[i] = EXPECTED_IDS[i];
        valueCol.setVal(i, EXPECTED_VALUES[i].getBytes(StandardCharsets.UTF_8));
      }
      writer.addRowBatch(batch);
    }
  }

  private static JobConf buildJobConf(Path orcPath) {
    JobConf job = new JobConf(daemonConf);
    HiveConf.setBoolVar(job, ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(job, ConfVars.PLAN, "//tmp");
    job.set(IOConstants.COLUMNS, "id,value");
    job.set(IOConstants.COLUMNS_TYPES, "bigint,string");
    job.set(ColumnProjectionUtils.ORC_SCHEMA_STRING, ORC_SCHEMA.toString());

    Properties tblProps = new Properties();
    tblProps.setProperty(META_TABLE_NAME, "default.test_orc");
    TableDesc tableDesc = new TableDesc(OrcInputFormat.class, OrcOutputFormat.class, tblProps);

    MapWork mapWork = new MapWork();
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(new VectorizedRowBatchCtx(
        new String[] {"id", "value"},
        new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.stringTypeInfo},
        null, null, 0, 0, null, new String[0], null));
    PartitionDesc partitionDesc = new PartitionDesc();
    partitionDesc.setTableDesc(tableDesc);
    mapWork.addPathToPartitionInfo(orcPath.getParent(), partitionDesc);
    Utilities.setMapWork(job, mapWork);
    return job;
  }
}
