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

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;

@Category(MetastoreCheckinTest.class)
public class TestRemoteHiveHttpMetaStore extends TestRemoteHiveMetaStore {

  private static final Logger LOG = LoggerFactory.getLogger(TestRemoteHiveHttpMetaStore.class);

  @Override
  public void start() throws Exception {
    MetastoreConf.setVar(conf, ConfVars.THRIFT_TRANSPORT_MODE, "http");
    LOG.info("Attempting to start test remote metastore in http mode");
    super.start();
    LOG.info("Successfully started test remote metastore in http mode");
  }

  @Override
  protected HiveMetaStoreClient createClient() throws Exception {
    MetastoreConf.setVar(conf, ConfVars.METASTORE_CLIENT_THRIFT_TRANSPORT_MODE, "http");
    return super.createClient();
  }
}
