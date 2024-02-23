/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.util.concurrent.CyclicBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtilCyclicBarrier {
  private static final Logger LOG = LoggerFactory.getLogger(TestUtilCyclicBarrier.class);
  private static TestUtilCyclicBarrier instance;
  private final CyclicBarrier cyclicBarrier;

  private TestUtilCyclicBarrier(int parties) {
    cyclicBarrier = new CyclicBarrier(parties);
  }

  public static synchronized TestUtilCyclicBarrier getInstance(int parties) {
    if (instance == null) {
      LOG.info("UnitTestConcurrency: Instantiating the cyclic barrier");
      instance = new TestUtilCyclicBarrier(parties);
    }
    return instance;
  }

  public CyclicBarrier getCyclicBarrier() {
    return cyclicBarrier;
  }

  public static synchronized boolean isInstantiated() {
    return instance != null;
  }

  public static synchronized void destroyInstance() {
    if (instance != null) {
      LOG.info("UnitTestConcurrency: Destroying the cyclic barrier");
      instance = null;
    }
  }
}
