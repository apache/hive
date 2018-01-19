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
package org.apache.hive.beeline;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({ HiveSchemaHelper.class, HiveSchemaTool.CommandBuilder.class })
public class TestHiveSchemaTool {

  String scriptFile = System.getProperty("java.io.tmpdir") + File.separator + "someScript.sql";
  @Mock
  private HiveConf hiveConf;
  private HiveSchemaTool.CommandBuilder builder;
  private String pasword = "reallySimplePassword";

  @Before
  public void setup() throws IOException {
    mockStatic(HiveSchemaHelper.class);
    when(HiveSchemaHelper
        .getValidConfVar(eq(MetastoreConf.ConfVars.CONNECTURLKEY), same(hiveConf)))
        .thenReturn("someURL");
    when(HiveSchemaHelper
        .getValidConfVar(eq(MetastoreConf.ConfVars.CONNECTION_DRIVER), same(hiveConf)))
        .thenReturn("someDriver");

    File file = new File(scriptFile);
    if (!file.exists()) {
      file.createNewFile();
    }
    builder = new HiveSchemaTool.CommandBuilder(hiveConf, null, null, "testUser", pasword, scriptFile);
  }

  @After
  public void globalAssert() throws IOException {
    verifyStatic();
    HiveSchemaHelper.getValidConfVar(eq(MetastoreConf.ConfVars.CONNECTURLKEY), same(hiveConf));
    HiveSchemaHelper
        .getValidConfVar(eq(MetastoreConf.ConfVars.CONNECTION_DRIVER), same(hiveConf));

    new File(scriptFile).delete();
  }

  @Test
  public void shouldReturnStrippedPassword() throws IOException {
    assertFalse(builder.buildToLog().contains(pasword));
  }

  @Test
  public void shouldReturnActualPassword() throws IOException {
    String[] strings = builder.buildToRun();
    assertTrue(Arrays.asList(strings).contains(pasword));
  }
}
