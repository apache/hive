/**
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHCatOutputFormat extends TestCase {

  private static final Logger LOG = LoggerFactory.getLogger(TestHCatOutputFormat.class);
  private HiveMetaStoreClient client;
  private HiveConf hiveConf;

  private static final String dbName = "hcatOutputFormatTestDB";
  private static final String tblName = "hcatOutputFormatTestTable";

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    hiveConf = new HiveConf(this.getClass());

    try {
      client = new HiveMetaStoreClient(hiveConf);

      initTable();
    } catch (Throwable e) {
      LOG.error("Unable to open the metastore", e);
      throw new Exception(e);
    }
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      super.tearDown();
      client.dropTable(dbName, tblName);
      client.dropDatabase(dbName);

      client.close();
    } catch (Throwable e) {
      LOG.error("Unable to close metastore", e);
      throw new Exception(e);
    }
  }

  private void initTable() throws Exception {

    try {
      client.dropTable(dbName, tblName);
    } catch (Exception e) {
    }
    try {
      client.dropDatabase(dbName);
    } catch (Exception e) {
    }
    client.createDatabase(new Database(dbName, "", null, null));
    assertNotNull((client.getDatabase(dbName).getLocationUri()));

    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    fields.add(new FieldSchema("colname", serdeConstants.STRING_TYPE_NAME, ""));

    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(Lists.newArrayList(new FieldSchema("data_column", serdeConstants.STRING_TYPE_NAME, "")));
    tbl.setSd(sd);

    //sd.setLocation("hdfs://tmp");
    sd.setInputFormat(RCFileInputFormat.class.getName());
    sd.setOutputFormat(RCFileOutputFormat.class.getName());
    sd.setParameters(new HashMap<String, String>());
    sd.getParameters().put("test_param_1", "Use this for comments etc");
    //sd.setBucketCols(new ArrayList<String>(2));
    //sd.getBucketCols().add("name");
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.getSerdeInfo().setSerializationLib(
        org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
    tbl.setPartitionKeys(fields);

    Map<String, String> tableParams = new HashMap<String, String>();
    tableParams.put("hcat.testarg", "testArgValue");

    tbl.setParameters(tableParams);

    client.createTable(tbl);
    Path tblPath = new Path(client.getTable(dbName, tblName).getSd().getLocation());
    assertTrue(tblPath.getFileSystem(hiveConf).mkdirs(new Path(tblPath, "colname=p1")));

  }

  public void testSetOutput() throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "test outputformat");

    Map<String, String> partitionValues = new HashMap<String, String>();
    partitionValues.put("colname", "p1");
    //null server url means local mode
    OutputJobInfo info = OutputJobInfo.create(dbName, tblName, partitionValues);

    HCatOutputFormat.setOutput(job, info);
    OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(job.getConfiguration());

    assertNotNull(jobInfo.getTableInfo());
    assertEquals(1, jobInfo.getPartitionValues().size());
    assertEquals("p1", jobInfo.getPartitionValues().get("colname"));
    assertEquals(1, jobInfo.getTableInfo().getDataColumns().getFields().size());
    assertEquals("data_column", jobInfo.getTableInfo().getDataColumns().getFields().get(0).getName());

    publishTest(job);
  }

  public void publishTest(Job job) throws Exception {
    HCatOutputFormat hcof = new HCatOutputFormat();
    TaskAttemptContext tac = ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptContext(
        job.getConfiguration(), ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptID());
    OutputCommitter committer = hcof.getOutputCommitter(tac);
    committer.setupJob(job);
    committer.setupTask(tac);
    committer.commitTask(tac);
    committer.commitJob(job);

    Partition part = client.getPartition(dbName, tblName, Arrays.asList("p1"));
    assertNotNull(part);

    StorerInfo storer = InternalUtil.extractStorerInfo(part.getSd(), part.getParameters());
    assertEquals(storer.getProperties().get("hcat.testarg"), "testArgValue");
    assertTrue(part.getSd().getLocation().indexOf("p1") != -1);
  }
}
