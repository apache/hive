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


package org.apache.hadoop.hive.llap.processor;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.api.impl.RequestImpl;
import org.apache.hadoop.hive.llap.loader.Loader;
import org.apache.hadoop.hive.llap.loader.OrcLoader;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

// TODO: write unit tests if this class becomes less primitive.
public class Pool {
  // TODO: for now, pool is of dubious value. There's one processor per request.
  //       So, this provides thread safety that may or may not be needed.
  private final LinkedList<Processor> processors = new LinkedList<Processor>();
  private static final int POOL_LIMIT = 10;
  // There's only one loader, assumed to be thread safe.
  private final Loader loader;
  private final ExecutorService threadPool;

  public Pool(Loader loader, Configuration conf) {
    this.loader = loader;
    int threadCount = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_REQUEST_THREAD_COUNT);
    this.threadPool = Executors.newFixedThreadPool(threadCount,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Llap thread %d").build());
  }

  public void enqueue(RequestImpl request, ChunkConsumer consumer) {
    Processor proc = null;
    synchronized (processors) {
      proc = processors.poll();
    }
    if (proc == null) {
      proc = new Processor(this, loader);
    }
    proc.setRequest(request, consumer);
    threadPool.submit(proc);
  }

  void returnProcessor(Processor proc) {
    synchronized (processors) {
      if (processors.size() < POOL_LIMIT) {
        processors.add(proc);
      }
    }
  }
}
