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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;

import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.thrift.test.IntString;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class TestHCatInputFormat extends HCatBaseTest {

  private boolean setUpComplete = false;

  /**
   * Create an input sequence file with 100 records; every 10th record is bad.
   * Load this table into Hive.
   */
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    if (setUpComplete) {
      return;
    }

    Path intStringSeq = new Path(TEST_DATA_DIR + "/data/intString.seq");
    LOG.info("Creating data file: " + intStringSeq);
    SequenceFile.Writer seqFileWriter = SequenceFile.createWriter(
      intStringSeq.getFileSystem(hiveConf), hiveConf, intStringSeq,
      NullWritable.class, BytesWritable.class);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TIOStreamTransport transport = new TIOStreamTransport(out);
    TBinaryProtocol protocol = new TBinaryProtocol(transport);

    for (int i = 1; i <= 100; i++) {
      if (i % 10 == 0) {
        seqFileWriter.append(NullWritable.get(), new BytesWritable("bad record".getBytes()));
      } else {
        out.reset();
        IntString intString = new IntString(i, Integer.toString(i), i);
        intString.write(protocol);
        BytesWritable bytesWritable = new BytesWritable(out.toByteArray());
        seqFileWriter.append(NullWritable.get(), bytesWritable);
      }
    }

    seqFileWriter.close();

    // Now let's load this file into a new Hive table.
    Assert.assertEquals(0, driver.run("drop table if exists test_bad_records").getResponseCode());
    Assert.assertEquals(0, driver.run(
      "create table test_bad_records " +
        "row format serde 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer' " +
        "with serdeproperties ( " +
        "  'serialization.class'='org.apache.hadoop.hive.serde2.thrift.test.IntString', " +
        "  'serialization.format'='org.apache.thrift.protocol.TBinaryProtocol') " +
        "stored as" +
        "  inputformat 'org.apache.hadoop.mapred.SequenceFileInputFormat'" +
        "  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'")
      .getResponseCode());
    Assert.assertEquals(0, driver.run("load data local inpath '" + intStringSeq.getParent() +
      "' into table test_bad_records").getResponseCode());

    setUpComplete = true;
  }

  @Test
  public void testBadRecordHandlingPasses() throws Exception {
    Assert.assertTrue(runJob(0.1f));
  }

  @Test
  public void testBadRecordHandlingFails() throws Exception {
    Assert.assertFalse(runJob(0.01f));
  }

  private boolean runJob(float badRecordThreshold) throws Exception {
    Configuration conf = new Configuration();

    conf.setFloat(HCatConstants.HCAT_INPUT_BAD_RECORD_THRESHOLD_KEY, badRecordThreshold);

    Job job = new Job(conf);
    job.setJarByClass(this.getClass());
    job.setMapperClass(MyMapper.class);

    job.setInputFormatClass(HCatInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    HCatInputFormat.setInput(job, "default", "test_bad_records");

    job.setMapOutputKeyClass(HCatRecord.class);
    job.setMapOutputValueClass(HCatRecord.class);

    job.setNumReduceTasks(0);

    Path path = new Path(TEST_DATA_DIR, "test_bad_record_handling_output");
    if (path.getFileSystem(conf).exists(path)) {
      path.getFileSystem(conf).delete(path, true);
    }

    TextOutputFormat.setOutputPath(job, path);

    return job.waitForCompletion(true);
  }

  public static class MyMapper extends Mapper<NullWritable, HCatRecord, NullWritable, Text> {
    @Override
    public void map(NullWritable key, HCatRecord value, Context context)
      throws IOException, InterruptedException {
      LOG.info("HCatRecord: " + value);
      context.write(NullWritable.get(), new Text(value.toString()));
    }
  }
}
