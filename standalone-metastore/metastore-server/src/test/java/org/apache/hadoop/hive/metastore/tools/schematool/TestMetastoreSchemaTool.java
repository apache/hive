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
package org.apache.hadoop.hive.metastore.tools.schematool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestMetastoreSchemaTool {

  private String scriptFile = System.getProperty("java.io.tmpdir") + File.separator + "someScript.sql";
  @Mock
  private Configuration conf;
  private MetastoreSchemaTool.CommandBuilder builder;
  private String password = "reallySimplePassword";

  @Before
  public void setup() throws IOException {
    conf = MetastoreConf.newMetastoreConf();
    File file = new File(scriptFile);
    if (!file.exists()) {
      file.createNewFile();
    }
    builder =
        new MetastoreSchemaTool.CommandBuilder(conf, null, null, "testUser", password, scriptFile)
            .setVerbose(false);
  }

  @After
  public void globalAssert() throws IOException {
    new File(scriptFile).delete();
  }

  @Test
  public void shouldReturnStrippedPassword() throws IOException {
    assertFalse(builder.buildToLog().contains(password));
  }

  @Test
  public void shouldReturnActualPassword() throws IOException {
    String[] strings = builder.buildToRun();
    assertTrue(Arrays.asList(strings).contains(password));
  }
}
