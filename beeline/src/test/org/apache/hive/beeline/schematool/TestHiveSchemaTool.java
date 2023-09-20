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
package org.apache.hive.beeline.schematool;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mockStatic;


@RunWith(MockitoJUnitRunner.class)
public class TestHiveSchemaTool {

  String scriptFile = System.getProperty("java.io.tmpdir") + File.separator + "someScript.sql";
  @Mock
  private HiveConf hiveConf;
  private MockedStatic<HiveSchemaHelper> hiveSchemaHelperMockedStatic;
  private HiveSchemaTool.HiveSchemaToolCommandBuilder builder;
  private String pasword = "reallySimplePassword";

  @Before
  public void setup() throws IOException {
    hiveSchemaHelperMockedStatic = mockStatic(HiveSchemaHelper.class);
    hiveSchemaHelperMockedStatic.when(() -> HiveSchemaHelper
        .getValidConfVar(eq(MetastoreConf.ConfVars.CONNECT_URL_KEY), same(hiveConf)))
        .thenReturn("someURL");
    hiveSchemaHelperMockedStatic.when(() -> HiveSchemaHelper
        .getValidConfVar(eq(MetastoreConf.ConfVars.CONNECTION_DRIVER), same(hiveConf)))
        .thenReturn("someDriver");

    File file = new File(scriptFile);
    if (!file.exists()) {
      file.createNewFile();
    }
    builder = new HiveSchemaTool.HiveSchemaToolCommandBuilder(hiveConf, null, null, "testUser", pasword, scriptFile);
  }

  @After
  public void globalAssert() throws IOException {
    HiveSchemaHelper.getValidConfVar(eq(MetastoreConf.ConfVars.CONNECT_URL_KEY), same(hiveConf));
    HiveSchemaHelper
        .getValidConfVar(eq(MetastoreConf.ConfVars.CONNECTION_DRIVER), same(hiveConf));
    hiveSchemaHelperMockedStatic.close();
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
