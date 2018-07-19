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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class TestHiveMetastoreCli {
  private static final String[] CLI_ARGUMENTS = { "9999" };

  @Test
  public void testDefaultCliPortValue() {
    Configuration configuration = MetastoreConf.newMetastoreConf();
    HiveMetaStore.HiveMetastoreCli cli = new HiveMetaStore.HiveMetastoreCli(configuration);
    assert (cli.getPort() == MetastoreConf.getIntVar(configuration, ConfVars.SERVER_PORT));
  }

  @Test
  public void testOverriddenCliPortValue() {
    Configuration configuration = MetastoreConf.newMetastoreConf();
    HiveMetaStore.HiveMetastoreCli cli = new HiveMetaStore.HiveMetastoreCli(configuration);
    cli.parse(TestHiveMetastoreCli.CLI_ARGUMENTS);

    assert (cli.getPort() == 9999);
  }

  @Test
  public void testOverriddenMetastoreServerPortValue() {
    Configuration configuration = MetastoreConf.newMetastoreConf();
    MetastoreConf.setLongVar(configuration, ConfVars.SERVER_PORT, 12345);

    HiveMetaStore.HiveMetastoreCli cli = new HiveMetaStore.HiveMetastoreCli(configuration);

    assert (cli.getPort() == 12345);
  }

  @Test
  public void testCliOverridesConfiguration() {
    Configuration configuration = MetastoreConf.newMetastoreConf();
    MetastoreConf.setLongVar(configuration, ConfVars.SERVER_PORT, 12345);

    HiveMetaStore.HiveMetastoreCli cli = new HiveMetaStore.HiveMetastoreCli(configuration);
    cli.parse(CLI_ARGUMENTS);

    assert (cli.getPort() == 9999);
  }
}
