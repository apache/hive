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
package org.apache.hadoop.hive.llap.daemon.impl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Extra load generator service for LLAP.
 */
public class LlapLoadGeneratorService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(LlapLoadGeneratorService.class);
  private long interval;
  private float threshold;
  private String[] victimsHostName;
  @VisibleForTesting
  Thread[] threads;

  public LlapLoadGeneratorService() {
    super("LlapLoadGeneratorService");
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    threshold = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_UTILIZATION);
    if (threshold < 0 || threshold > 1.0) {
      throw new IllegalArgumentException(HiveConf.ConfVars.HIVE_TEST_LOAD_UTILIZATION.varname + " should " +
        "be between 0.0 and 1.0. The configuration specified [" + threshold + "]");
    }
    victimsHostName = HiveConf.getTrimmedStringsVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_HOSTNAMES);
    interval = HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_INTERVAL, TimeUnit.MILLISECONDS);
    LOG.info("LlapLoadGeneratorService init with {} {} {}", interval, threshold, victimsHostName);
  }

  @Override
  protected void serviceStart() throws UnknownHostException {
    String localHostName = InetAddress.getLocalHost().getHostName();
    LOG.debug("Local hostname is: {}", localHostName);
    for (String hostName : victimsHostName) {
      if (hostName.equalsIgnoreCase(localHostName)) {
        LOG.debug("Starting load generator process on: {}", localHostName);
        threads = new Thread[Runtime.getRuntime().availableProcessors()];
        Random random = new Random();
        for (int i = 0; i < threads.length; i++) {
          threads[i] = new Thread(new Runnable() {
            @Override
            public void run() {
              while (!Thread.interrupted()) {
                if (random.nextFloat() <= threshold) {
                  // Keep it busy
                  long startTime = System.currentTimeMillis();
                  while (System.currentTimeMillis() - startTime < interval) {
                    // active loop, do nothing, just check interrupt status
                    if (Thread.currentThread().isInterrupted()) {
                      break;
                    }
                  }
                } else {
                  // Keep it idle
                  try {
                    Thread.sleep(interval);
                  } catch (InterruptedException e) {
                    // Set the interrupt flag so we will stop the thread
                    Thread.currentThread().interrupt();
                  }
                }
              }
            }
          });
          threads[i].start();
        }
      }
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    for (int i = 0; i < threads.length; i++) {
      threads[i].interrupt();
    }
  }
}
