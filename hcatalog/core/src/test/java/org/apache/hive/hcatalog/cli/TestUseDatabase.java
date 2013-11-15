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
package org.apache.hive.hcatalog.cli;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;

/* Unit test for GitHub Howl issue #3 */
public class TestUseDatabase extends TestCase {

  private Driver hcatDriver;

  @Override
  protected void setUp() throws Exception {

    HiveConf hcatConf = new HiveConf(this.getClass());
    hcatConf.set(ConfVars.PREEXECHOOKS.varname, "");
    hcatConf.set(ConfVars.POSTEXECHOOKS.varname, "");
    hcatConf.set(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

    hcatConf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname, HCatSemanticAnalyzer.class.getName());
    hcatDriver = new Driver(hcatConf);
    SessionState.start(new CliSessionState(hcatConf));
  }

  String query;
  private final String dbName = "testUseDatabase_db";
  private final String tblName = "testUseDatabase_tbl";

  public void testAlterTablePass() throws IOException, CommandNeedRetryException {

    hcatDriver.run("create database " + dbName);
    hcatDriver.run("use " + dbName);
    hcatDriver.run("create table " + tblName + " (a int) partitioned by (b string) stored as RCFILE");

    CommandProcessorResponse response;

    String tmpDir = System.getProperty("test.tmp.dir");
    File dir = new File(tmpDir + "/hive-junit-" + System.nanoTime());
    response = hcatDriver.run("alter table " + tblName + " add partition (b='2') location '" + dir.getAbsolutePath() + "'");
    assertEquals(0, response.getResponseCode());
    assertNull(response.getErrorMessage());

    response = hcatDriver.run("alter table " + tblName + " set fileformat INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' OUTPUTFORMAT " +
        "'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' inputdriver 'mydriver' outputdriver 'yourdriver'");
    assertEquals(0, response.getResponseCode());
    assertNull(response.getErrorMessage());

    hcatDriver.run("drop table " + tblName);
    hcatDriver.run("drop database " + dbName);
  }

}
