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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public final class JavaDataModelTest {

  private static final String DATA_MODEL_PROPERTY = "sun.arch.data.model";

  private String previousModelSetting;

  @Before
  public void setUp() throws Exception {
    previousModelSetting = System.getProperty(DATA_MODEL_PROPERTY);
  }

  @After
  public void tearDown() throws Exception {
    if (previousModelSetting != null) {
      System.setProperty(DATA_MODEL_PROPERTY, previousModelSetting);
    } else {
      System.clearProperty(DATA_MODEL_PROPERTY);
    }
  }

  @Test
  public void testGetDoesNotReturnNull() throws Exception {
    JavaDataModel model = JavaDataModel.get();
    assertNotNull(model);
  }

  @Test
  public void testGetModelForSystemWhenSetTo32() throws Exception {
    System.setProperty(DATA_MODEL_PROPERTY, "32");
    assertSame(JavaDataModel.JAVA32, JavaDataModel.getModelForSystem());
  }

  @Test
  public void testGetModelForSystemWhenSetTo64() throws Exception {
    System.setProperty(DATA_MODEL_PROPERTY, "64");
    assertSame(JavaDataModel.JAVA64, JavaDataModel.getModelForSystem());
  }

  @Test
  public void testGetModelForSystemWhenSetToUnknown() throws Exception {
    System.setProperty(DATA_MODEL_PROPERTY, "unknown");
    assertSame(JavaDataModel.JAVA64, JavaDataModel.getModelForSystem());
  }

  @Test
  public void testGetModelForSystemWhenUndefined() throws Exception {
    System.clearProperty(DATA_MODEL_PROPERTY);
    assertSame(JavaDataModel.JAVA64, JavaDataModel.getModelForSystem());
  }
}