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

package org.apache.hive.jdbc;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapArrowRowInputFormat;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * TestNewGetSplitsFormat.
 */
@Ignore("test unstable HIVE-23524")
public class TestNewGetSplitsFormat extends BaseJdbcWithMiniLlap {

  @BeforeClass public static void beforeTest() throws Exception {
    HiveConf conf = defaultConf();
    conf.setBoolVar(HiveConf.ConfVars.LLAP_OUTPUT_FORMAT_ARROW, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_FILESINK_ARROW_NATIVE_ENABLED, true);
    BaseJdbcWithMiniLlap.beforeTest(conf);
  }

  @Override protected InputFormat<NullWritable, Row> getInputFormat() {
    //For unit testing, no harm in hard-coding allocator ceiling to LONG.MAX_VALUE
    return new LlapArrowRowInputFormat(Long.MAX_VALUE);
  }

  @Override public void testDataTypes() throws Exception {
    TestJdbcWithMiniLlapVectorArrow testJdbcWithMiniLlapVectorArrow = new TestJdbcWithMiniLlapVectorArrow();
    testJdbcWithMiniLlapVectorArrow.testDataTypes();
  }

  @Override
  @Ignore
  public void testMultipleBatchesOfComplexTypes() {
    // ToDo: FixMe
  }

  @Override protected int processQuery(String currentDatabase, String query, int numSplits, RowProcessor rowProcessor)
      throws Exception {
    String url = miniHS2.getJdbcURL();
    String user = System.getProperty("user.name");
    String pwd = user;
    String handleId = UUID.randomUUID().toString();

    InputFormat<NullWritable, Row> inputFormat = getInputFormat();

    // Get splits
    JobConf job = new JobConf(conf);
    job.set(LlapBaseInputFormat.URL_KEY, url);
    job.set(LlapBaseInputFormat.USER_KEY, user);
    job.set(LlapBaseInputFormat.PWD_KEY, pwd);
    job.set(LlapBaseInputFormat.QUERY_KEY, query);
    job.set(LlapBaseInputFormat.HANDLE_ID, handleId);
    job.set(LlapBaseInputFormat.USE_NEW_SPLIT_FORMAT, "true");
    if (currentDatabase != null) {
      job.set(LlapBaseInputFormat.DB_KEY, currentDatabase);
    }

    InputSplit[] splits = inputFormat.getSplits(job, numSplits);

    if (splits.length <= 1) {
      return 0;
    }


    // populate actual splits with schema and planBytes[]
    LlapInputSplit schemaSplit = (LlapInputSplit) splits[0];
    LlapInputSplit planSplit = (LlapInputSplit) splits[1];

    List<LlapInputSplit> actualSplits = new ArrayList<>();

    for (int i = 2; i < splits.length; i++) {
      LlapInputSplit actualSplit = (LlapInputSplit) splits[i];
      actualSplit.setSchema(schemaSplit.getSchema());
      actualSplit.setPlanBytes(planSplit.getPlanBytes());
      actualSplits.add(actualSplit);
    }

    // Fetch rows from splits
    int rowCount = 0;
    for (InputSplit split : actualSplits) {
      System.out.println("Processing split " + split.getLocations());
      RecordReader<NullWritable, Row> reader = inputFormat.getRecordReader(split, job, null);
      Row row = reader.createValue();
      while (reader.next(NullWritable.get(), row)) {
        rowProcessor.process(row);
        ++rowCount;
      }
      //In arrow-mode this will throw exception unless all buffers have been released
      //See org.apache.hadoop.hive.llap.LlapArrowBatchRecordReader
      reader.close();
    }
    LlapBaseInputFormat.close(handleId);

    return rowCount;
  }

}
