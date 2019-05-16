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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

/**
 * Extra load generator service for LLAP.
 */
public class LlapLoadGeneratorService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(LlapLoadGeneratorService.class);
  private static final long INTERVAL = 10;
  private float threshold;
  private String[] hostNames;
  private int processors;
  private Thread[] threads;

  public LlapLoadGeneratorService() {
    super("LlapLoadGeneratorService");
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    threshold = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_UTILIZATION);
    hostNames = HiveConf.getTrimmedStringsVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_HOSTNAME);
    LOG.info("LlapLoadGeneratorService init with {} {}", threshold, hostNames);
  }

  protected void serviceStart() throws UnknownHostException {
    String localHostName = InetAddress.getLocalHost().getHostName();
    LOG.debug("Local hostname is: {}", localHostName);
    for (String hostName : hostNames) {
      if (hostName.equalsIgnoreCase(localHostName)) {
        LOG.debug("Starting load generator process on: {}", localHostName);
        processors = Runtime.getRuntime().availableProcessors();
        threads = new Thread[processors];
        Random random = new Random();
        for (int i = 0; i < processors; i++) {
          threads[i] = new Thread(new Runnable() {
            @Override
            public void run() {
              while (!Thread.interrupted()) {
                if (random.nextFloat() <= threshold) {
                  // Keep it busy
                  long startTime = System.currentTimeMillis();
                  while (System.currentTimeMillis() - startTime < INTERVAL) {
                    // active loop, do nothing
                  }
                } else {
                  // Keep it idle
                  try {
                    Thread.sleep(INTERVAL);
                  } catch (InterruptedException e) {
                    // Ignore
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
    for (int i = 0; i < processors; i++) {
      threads[i].interrupt();
    }
  }
}
