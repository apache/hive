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

import org.apache.hadoop.hive.llap.api.Llap;
import org.apache.hadoop.hive.llap.api.impl.RequestImpl;
import org.apache.hadoop.hive.llap.loader.Loader;

/**
 * Request processor class. Currently, of dubious value.
 */
public class Processor implements Runnable {
  private final Pool parent;
  private final Loader loader;
  private RequestImpl request;
  private ChunkConsumer consumer;

  public Processor(Pool pool, Loader loader) {
    this.parent = pool;
    this.loader = loader;
  }

  public void setRequest(RequestImpl request, ChunkConsumer consumer) {
    this.request = request;
    this.consumer = consumer;
  }

  @Override
  public void run() {
    try {
      loader.load(request, consumer); // Synchronous load call that return results via consumer.
    } catch (Throwable t) {
      Llap.LOG.error("Load failed", t);
      consumer.setError(t);
    }
    parent.returnProcessor(this);
  }
}
