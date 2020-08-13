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

package org.apache.hadoop.hive.ql.metadata;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * Abstract factory that defines an interface for other factories that produce concrete
 * MetaStoreClient objects.
 *
 */
public interface HiveMetaStoreClientFactory {

  /**
   * A method for producing IMetaStoreClient objects.
   *
   * The implementation returned by this method must throw a MetaException if allowEmbedded = true
   * and it does not support embedded mode.
   *
   * @param conf
   *          Hive Configuration.
   * @param hookLoader
   *          Hook for handling events related to tables.
   * @param allowEmbedded
   *          Flag indicating the implementation must run in-process, e.g. for unit testing or
   *          "fast path".
   * @param metaCallTimeMap
   *          A container for storing entry and exit timestamps of IMetaStoreClient method
   *          invocations.
   * @return IMetaStoreClient An implementation of IMetaStoreClient.
   * @throws MetaException
   */
  IMetaStoreClient createMetaStoreClient(HiveConf conf, HiveMetaHookLoader hookLoader,
      boolean allowEmbedded, ConcurrentHashMap<String, Long> metaCallTimeMap) throws MetaException;
}
