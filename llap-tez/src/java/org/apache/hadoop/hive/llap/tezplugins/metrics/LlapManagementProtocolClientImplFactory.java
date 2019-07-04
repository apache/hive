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

package org.apache.hadoop.hive.llap.tezplugins.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.impl.LlapManagementProtocolClientImpl;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.NetUtils;

import javax.net.SocketFactory;
import java.util.concurrent.TimeUnit;

/**
 * Creates a LlapManagementProtocolClientImpl from a given LlapServiceInstance.
 */
public class LlapManagementProtocolClientImplFactory {
  private final Configuration conf;
  private final RetryPolicy retryPolicy;
  private final SocketFactory socketFactory;

  public LlapManagementProtocolClientImplFactory(Configuration conf, RetryPolicy retryPolicy,
                                                 SocketFactory socketFactory) {
    this.conf = conf;
    this.retryPolicy = retryPolicy;
    this.socketFactory = socketFactory;
  }

  public static LlapManagementProtocolClientImplFactory basicInstance(Configuration conf) {
    return new LlapManagementProtocolClientImplFactory(
        conf,
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(5, 3000L, TimeUnit.MILLISECONDS),
        NetUtils.getDefaultSocketFactory(conf));
  }

  public LlapManagementProtocolClientImpl create(LlapServiceInstance serviceInstance) {
    return new LlapManagementProtocolClientImpl(conf, serviceInstance.getHost(),
        serviceInstance.getManagementPort(), retryPolicy,
        socketFactory);
  }
}
