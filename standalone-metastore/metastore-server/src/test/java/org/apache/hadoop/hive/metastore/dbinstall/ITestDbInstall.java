/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.dbinstall;

import org.apache.hadoop.hive.metastore.database.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ITestDbInstall {
  private static final String FIRST_VERSION = "1.2.0";

  @Parameterized.Parameter
  public MetastoreDatabaseWrapper metastoreDb;

  @Parameterized.Parameters(name = "metastoreDb = {0}")
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][] {
        { new MetastoreDerby(true) },
        { new MetastoreMysql() },
        { new MetastoreMssql() },
        { new MetastoreOracle() },
        { new MetastorePostgres() }
    });
  }

  @Before
  public void setup() throws Exception {
    metastoreDb.before();
  }

  @After
  public void tearDown() throws Exception {
    metastoreDb.after();
  }

  @Test
  public void install() {
    Assert.assertEquals(0, metastoreDb.createUser());
    Assert.assertEquals(0, metastoreDb.installLatest());
  }

  @Test
  public void upgrade() {
    Assert.assertEquals(0, metastoreDb.createUser());
    Assert.assertEquals(0, metastoreDb.installAVersion(FIRST_VERSION));
    Assert.assertEquals(0, metastoreDb.upgradeToLatest());
    Assert.assertEquals(0, metastoreDb.validateSchema());
  }
}
