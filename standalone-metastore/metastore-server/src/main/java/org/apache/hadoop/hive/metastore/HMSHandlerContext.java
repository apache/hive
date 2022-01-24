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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;

/**
 * When one hms client connects in, we create a handler context for it.
 * We store session information here.
 */
public final class HMSHandlerContext {

  private static final ThreadLocal<HMSHandlerContext> context = new ThreadLocal<>();

  private static final AtomicInteger nextSerialNum = new AtomicInteger();

  private RawStore rawStore;

  private TxnStore txnStore;

  // Thread local HMSHandler used during shutdown to notify meta listeners
  private HMSHandler hmsHandler;

  // Thread local configuration is needed as many threads could make changes
  // to the conf using the connection hook
  private Configuration configuration;

  // Thread local Map to keep track of modified meta conf keys
  private Map<String, String> modifiedConfig = new HashMap<>();

  private Integer threadId = nextSerialNum.incrementAndGet();
  // This will only be set if the metastore is being accessed from a metastore Thrift server,
  // not if it is from the CLI. Also, only if the TTransport being used to connect is an
  // instance of TSocket. This is also not set when kerberos is used.
  private String ipAddress;

  private Map<String, com.codahale.metrics.Timer.Context> timerContexts = new HashMap<>();

  private HMSHandlerContext() {

  }

  public static Optional<RawStore> getRawStore() {
    HMSHandlerContext ctx = context.get();
    return ctx != null ? Optional.ofNullable(ctx.rawStore) : Optional.empty();
  }

  public static Optional<HMSHandler> getHMSHandler() {
    HMSHandlerContext ctx = context.get();
    return ctx != null ? Optional.ofNullable(ctx.hmsHandler) : Optional.empty();
  }

  public static Optional<String> getIpAddress() {
    HMSHandlerContext ctx = context.get();
    return ctx != null ? Optional.ofNullable(ctx.ipAddress) : Optional.empty();
  }

  public static Optional<Configuration> getConfiguration() {
    HMSHandlerContext ctx = context.get();
    return ctx != null ? Optional.ofNullable(ctx.configuration) : Optional.empty();
  }

  public static TxnStore getTxnStore(Configuration conf) {
    if (getContext().txnStore == null) {
      setTxnStore(TxnUtils.getTxnStore(conf));
    }
    return getContext().txnStore;
  }

  public static Map<String, String> getModifiedConfig() {
    return getContext().modifiedConfig;
  }

  public static Integer getThreadId() {
    return getContext().threadId;
  }

  public static Map<String, com.codahale.metrics.Timer.Context> getTimerContexts() {
    return getContext().timerContexts;
  }

  private static HMSHandlerContext getContext() {
    HMSHandlerContext ctx = context.get();
    if (ctx == null) {
      context.set(ctx = new HMSHandlerContext());
    }
    return ctx;
  }

  public static void setRawStore(RawStore rawStore) {
    getContext().rawStore = rawStore;
  }

  public static void setTxnStore(TxnStore txnStore) {
    getContext().txnStore = txnStore;
  }

  public static void setHMSHandler(HMSHandler hmsHandler) {
    getContext().hmsHandler = hmsHandler;
  }

  public static void setConfiguration(Configuration conf) {
    getContext().configuration = conf;
  }

  public static void setIpAddress(String ipAddress) {
    getContext().ipAddress = ipAddress;
  }

  public static void clear() {
    context.remove();
  }

}
