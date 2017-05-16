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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A wrapper around {@link ObjectStore}
 * with the ability to control the result of commitTransaction().
 * All other functions simply delegate to an embedded ObjectStore object.
 * Ideally, we should have just extended ObjectStore instead of using
 * delegation.  However, since HiveMetaStore uses a Proxy, this class must
 * not inherit from any other class.
 */
public class DummyRawStoreForRepl extends ObjectStore {
  public DummyRawStoreForRepl() {
    super();
  }

 /**
  * If true, getTable() will simply call delegate getTable() to the
  * underlying ObjectStore.
  * If false, getTable() immediately returns null.
  */
  private static boolean shouldGetTableSucceed = true;
  public static void setGetTableSucceed(boolean flag) {
    shouldGetTableSucceed = flag;
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {
    if (shouldGetTableSucceed) {
      return super.getTable(dbName, tableName);
    } else {
      shouldGetTableSucceed = true;
      return null;
    }
  }
}
