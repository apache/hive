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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;

public final class HMSHandlerContextMap {

  private static final String KEY_RAWSTORE = "raw_store";

  private static final String KEY_TXNSTORE = "txn_store";
  // Thread local configuration is needed as many threads could make changes
  // to the conf using the connection hook
  private static final String KEY_CONFIGURATION = "configuration";
  /**
   * Thread local HMSHandler used during shutdown to notify meta listeners
   */
  private static final String KEY_HMSHANDLER = "hmshandler";
  /**
   * Thread local Map to keep track of modified meta conf keys
   */
  private static final String KEY_MODIFIED_CONF = "modified_config";

  private static final String KEY_THREAD_ID = "thread_id";
  // This will only be set if the metastore is being accessed from a metastore Thrift server,
  // not if it is from the CLI. Also, only if the TTransport being used to connect is an
  // instance of TSocket. This is also not set when kerberos is used.
  private static final String KEY_IPADDRESS = "ip_address";

  private static final AtomicInteger nextSerialNum = new AtomicInteger();
  private static final ThreadLocal<Map<String, Object>> localMap = new ThreadLocal<>();

  private HMSHandlerContextMap() {
  }

  public static RawStore getLocalRawStoreOrNull() {
    return (RawStore) get(KEY_RAWSTORE);
  }

  public static TxnStore getLocalTxnStoreNotNull(Configuration conf) {
    TxnStore txn = (TxnStore) get(KEY_TXNSTORE);
    if (txn == null) {
      txn = TxnUtils.getTxnStore(conf);
      setLocalTxnStore(txn);
    }
    return txn;
  }

  public static HMSHandler getLocalHMSHandlerOrNull() {
    return (HMSHandler) get(KEY_HMSHANDLER);
  }

  public static Configuration getLocalConfigurationOrNull() {
    return (Configuration) get(KEY_CONFIGURATION);
  }

  public static Map<String, String> getLocalModifiedConfigNotNull() {
    Map<String, String> configs = (Map<String, String>) get(KEY_MODIFIED_CONF);
    if (configs == null) {
      configs = new HashMap<>();
      setLocalModifiedConfig(configs);
    }
    return configs;
  }

  public static Integer getLocalThreadIdNotNull() {
    Integer id = (Integer) get(KEY_THREAD_ID);
    if (id == null) {
      id = nextSerialNum.getAndIncrement();
      setLocalThreadId(id);
    }
    return id;
  }

  public static String getLocalIpAddressOrNull() {
    return (String) get(KEY_IPADDRESS);
  }

  private static Object get(String key) {
    Map<String, Object> entries = localMap.get();
    if (entries != null) {
      return entries.get(key);
    }
    return null;
  }

  public static void setLocalRawStore(RawStore rawStore) {
    set(KEY_RAWSTORE, rawStore);
  }

  public static void setLocalTxnStore(TxnStore txnStore) {
    set(KEY_TXNSTORE, txnStore);
  }

  public static void setLocalHMSHandler(HMSHandler hmsHandler) {
    set(KEY_HMSHANDLER, hmsHandler);
  }

  public static void setLocalConfiguration(Configuration conf) {
    set(KEY_CONFIGURATION, conf);
  }

  public static void setLocalModifiedConfig(Map<String, String> modifiedConfig) {
    set(KEY_MODIFIED_CONF, modifiedConfig);
  }

  public static void setLocalThreadId(Integer id) {
    set(KEY_THREAD_ID, id);
  }

  public static void setLocalIpAddress(String ipAddress) {
    set(KEY_IPADDRESS, ipAddress);
  }

  private static void set(String key, Object value) {
    Map<String, Object> entries = localMap.get();
    if (entries == null) {
      entries = new HashMap<>();
      localMap.set(entries);
    }
    entries.put(key, value);
  }

  public static boolean contains(String key) {
    Map<String, Object> entries = localMap.get();
    if (entries != null) {
      return entries.containsKey(key);
    }
    return false;
  }

  public static void clear() {
    localMap.remove();
  }

}
