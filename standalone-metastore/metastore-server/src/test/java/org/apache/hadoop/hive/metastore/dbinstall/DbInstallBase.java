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

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.dbinstall.rules.DatabaseRule;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreCheckinTest.class)
public abstract class DbInstallBase {
  private static final String FIRST_VERSION = "1.2.0";

  @Test
  public void install() {
    Assert.assertEquals(0, getRule().createUser());
    Assert.assertEquals(0, getRule().installLatest());
  }

  @Test
  public void upgrade() throws HiveMetaException {
    Assert.assertEquals(0, getRule().createUser());
    Assert.assertEquals(0, getRule().installAVersion(FIRST_VERSION));
    Assert.assertEquals(0, getRule().upgradeToLatest());
    Assert.assertEquals(0, getRule().validateSchema());
  }

  protected abstract DatabaseRule getRule();

  protected String[] buildArray(String... strs) {
    return strs;
  }
}
