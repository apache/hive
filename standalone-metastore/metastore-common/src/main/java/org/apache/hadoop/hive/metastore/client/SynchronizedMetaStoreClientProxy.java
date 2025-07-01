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

package org.apache.hadoop.hive.metastore.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

/**
 * A synchronized wrapper for {@link IMetaStoreClient}.
 * The reflection logic originally comes from {@link org.apache.hadoop.hive.metastore.HiveMetaStoreClient}.
 * This should be used by multi-thread applications unless all the underlying layers are thread-safe.
 */
public class SynchronizedMetaStoreClientProxy extends BaseMetaStoreClientProxy implements IMetaStoreClient {
  public SynchronizedMetaStoreClientProxy(Configuration conf, IMetaStoreClient delegate) {
    super(HiveMetaStoreClient.newSynchronizedClient(delegate), conf);
  }
}
