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

package org.apache.hcatalog.hbase;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hcatalog.cli.HCatDriver;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.hbase.snapshot.TableSnapshot;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSnapshots extends SkeletonHBaseTest {
  private static HiveConf hcatConf;
  private static HCatDriver hcatDriver;

  @BeforeClass
  public static void setup() throws Throwable {
    setupSkeletonHBaseTest();
  }

  public void Initialize() throws Exception {
    hcatConf = getHiveConf();
    hcatConf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
      HCatSemanticAnalyzer.class.getName());
    URI fsuri = getFileSystem().getUri();
    Path whPath = new Path(fsuri.getScheme(), fsuri.getAuthority(),
      getTestDir());
    hcatConf.set(HiveConf.ConfVars.HADOOPFS.varname, fsuri.toString());
    hcatConf.set(ConfVars.METASTOREWAREHOUSE.varname, whPath.toString());

    //Add hbase properties

    for (Map.Entry<String, String> el : getHbaseConf()) {
      if (el.getKey().startsWith("hbase.")) {
        hcatConf.set(el.getKey(), el.getValue());
      }
    }

    SessionState.start(new CliSessionState(hcatConf));
    hcatDriver = new HCatDriver();

  }

  @Test
  public void TestSnapshotConversion() throws Exception {
    Initialize();
    String tableName = newTableName("mytableOne");
    String databaseName = newTableName("mydatabase");
    String fullyQualTableName = databaseName + "." + tableName;
    String db_dir = new Path(getTestDir(), "hbasedb").toString();
    String dbquery = "CREATE DATABASE IF NOT EXISTS " + databaseName + " LOCATION '"
      + db_dir + "'";
    String tableQuery = "CREATE TABLE " + fullyQualTableName
      + "(key string, value1 string, value2 string) STORED BY " +
      "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
      + "TBLPROPERTIES ('hbase.columns.mapping'=':key,cf1:q1,cf2:q2')";

    CommandProcessorResponse cmdResponse = hcatDriver.run(dbquery);
    assertEquals(0, cmdResponse.getResponseCode());
    cmdResponse = hcatDriver.run(tableQuery);
    assertEquals(0, cmdResponse.getResponseCode());

    Configuration conf = new Configuration(hcatConf);
    conf.set(HCatConstants.HCAT_KEY_HIVE_CONF,
      HCatUtil.serialize(getHiveConf().getAllProperties()));
    Job job = new Job(conf);
    Properties properties = new Properties();
    properties.setProperty(HBaseConstants.PROPERTY_TABLE_SNAPSHOT_KEY, "dummysnapshot");
    HCatInputFormat.setInput(job, databaseName, tableName).setProperties(properties);
    String modifiedInputInfo = job.getConfiguration().get(HCatConstants.HCAT_KEY_JOB_INFO);
    InputJobInfo inputInfo = (InputJobInfo) HCatUtil.deserialize(modifiedInputInfo);

    Map<String, Long> revMap = new HashMap<String, Long>();
    revMap.put("cf1", 3L);
    revMap.put("cf2", 5L);
    TableSnapshot hbaseSnapshot = new TableSnapshot(fullyQualTableName, revMap, -1);
    HCatTableSnapshot hcatSnapshot = HBaseRevisionManagerUtil.convertSnapshot(hbaseSnapshot, inputInfo.getTableInfo());

    assertEquals(hcatSnapshot.getRevision("value1"), 3);
    assertEquals(hcatSnapshot.getRevision("value2"), 5);

    String dropTable = "DROP TABLE " + fullyQualTableName;
    cmdResponse = hcatDriver.run(dropTable);
    assertEquals(0, cmdResponse.getResponseCode());

    tableName = newTableName("mytableTwo");
    fullyQualTableName = databaseName + "." + tableName;
    tableQuery = "CREATE TABLE " + fullyQualTableName
      + "(key string, value1 string, value2 string) STORED BY " +
      "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
      + "TBLPROPERTIES ('hbase.columns.mapping'=':key,cf1:q1,cf1:q2')";
    cmdResponse = hcatDriver.run(tableQuery);
    assertEquals(0, cmdResponse.getResponseCode());
    revMap.clear();
    revMap.put("cf1", 3L);
    hbaseSnapshot = new TableSnapshot(fullyQualTableName, revMap, -1);
    HCatInputFormat.setInput(job, databaseName, tableName).setProperties(properties);
    modifiedInputInfo = job.getConfiguration().get(HCatConstants.HCAT_KEY_JOB_INFO);
    inputInfo = (InputJobInfo) HCatUtil.deserialize(modifiedInputInfo);
    hcatSnapshot = HBaseRevisionManagerUtil.convertSnapshot(hbaseSnapshot, inputInfo.getTableInfo());
    assertEquals(hcatSnapshot.getRevision("value1"), 3);
    assertEquals(hcatSnapshot.getRevision("value2"), 3);

    dropTable = "DROP TABLE " + fullyQualTableName;
    cmdResponse = hcatDriver.run(dropTable);
    assertEquals(0, cmdResponse.getResponseCode());

    String dropDatabase = "DROP DATABASE IF EXISTS " + databaseName + "CASCADE";
    cmdResponse = hcatDriver.run(dropDatabase);
    assertEquals(0, cmdResponse.getResponseCode());
  }
}
