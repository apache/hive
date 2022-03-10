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
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;

/**
 * When one hms client connects in, we create a handler context for it.
 * We store session information here.
 */
public final class HMSHandlerContext {

  private static final ThreadLocal<HMSHandlerContext> context = ThreadLocal.withInitial(() -> new HMSHandlerContext());

  private RawStore rawStore;

  private TxnStore txnStore;

  // Thread local HMSHandler used during shutdown to notify meta listeners
  private HMSHandler hmsHandler;

  // Thread local configuration is needed as many threads could make changes
  // to the conf using the connection hook
  private Configuration configuration;

  // Thread local Map to keep track of modified meta conf keys
  private Map<String, String> modifiedConfig = new HashMap<>();

  // This will only be set if the metastore is being accessed from a metastore Thrift server,
  // not if it is from the CLI. Also, only if the TTransport being used to connect is an
  // instance of TSocket. This is also not set when kerberos is used.
  private String ipAddress;

  private Map<String, com.codahale.metrics.Timer.Context> timerContexts = new HashMap<>();

  private HMSHandlerContext() {
  }

  public static Optional<RawStore> getRawStore() {
    return Optional.ofNullable(context.get().rawStore);
  }

  public static Optional<HMSHandler> getHMSHandler() {
    return Optional.ofNullable(context.get().hmsHandler);
  }

  public static Optional<String> getIpAddress() {
    return Optional.ofNullable(context.get().ipAddress);
  }

  public static Optional<Configuration> getConfiguration() {
    return Optional.ofNullable(context.get().configuration);
  }

  public static TxnStore getTxnStore(Configuration conf) {
    if (context.get().txnStore == null) {
      setTxnStore(TxnUtils.getTxnStore(conf));
    }
    return context.get().txnStore;
  }

  public static Map<String, String> getModifiedConfig() {
    return context.get().modifiedConfig;
  }

  public static Map<String, com.codahale.metrics.Timer.Context> getTimerContexts() {
    return context.get().timerContexts;
  }

  public static void setRawStore(RawStore rawStore) {
    context.get().rawStore = rawStore;
  }

  public static void setTxnStore(TxnStore txnStore) {
    context.get().txnStore = txnStore;
  }

  public static void setHMSHandler(HMSHandler hmsHandler) {
    context.get().hmsHandler = hmsHandler;
  }

  public static void setConfiguration(Configuration conf) {
    context.get().configuration = conf;
  }

  public static void setIpAddress(String ipAddress) {
    context.get().ipAddress = ipAddress;
  }

  /**
   * Release the context
   */
  public static void clear() {
    context.remove();
  }

}
