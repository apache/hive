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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import java.util.Objects;

final class IMetaStoreClientFactory extends BasePooledObjectFactory<IMetaStoreClient> {

  private final HiveConf conf;

  @Override
  public IMetaStoreClient create() throws Exception {
    return HiveMetaStoreUtils.getHiveMetastoreClient(conf);
  }

  @Override
  public PooledObject<IMetaStoreClient> wrap(IMetaStoreClient iMetaStoreClient) {
    return new DefaultPooledObject<>(iMetaStoreClient);
  }

  @Override
  public void destroyObject(PooledObject<IMetaStoreClient> p) {
    p.getObject().close();
  }

  @Override
  public boolean validateObject(PooledObject<IMetaStoreClient> p) {
    //Not in use currently, would be good to validate the client at borrowing/returning, but this needs support from
    //MetaStoreClient side
    return super.validateObject(p);
  }

  public IMetaStoreClientFactory(HiveConf conf) {
    this.conf = Objects.requireNonNull(conf);
  }

}
