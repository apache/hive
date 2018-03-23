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

package org.apache.hadoop.hive.metastore.minihms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;

/**
 * The AbstractMetaStore implementation which is used when the tests are running against a
 * MetaStore which running in a dedicated thread and accessed through the Thrift interface (See the
 * {@link org.apache.hadoop.hive.metastore.client.MetaStoreFactoryForTests} class).
 */
public class RemoteMetaStoreForTests extends AbstractMetaStoreService {

  public RemoteMetaStoreForTests(Configuration configuration) {
    super(configuration);
  }

  public void start() throws Exception {
    MetastoreConf.setBoolVar(getConfiguration(), MetastoreConf.ConfVars.EXECUTE_SET_UGI, false);
    int port = MetaStoreTestUtils.startMetaStore(HadoopThriftAuthBridge.getBridge(),
        getConfiguration());
    MetastoreConf.setVar(getConfiguration(), MetastoreConf.ConfVars.THRIFT_URIS,
        "thrift://localhost:" + port);
    super.start();
  }
}
