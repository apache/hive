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

package org.apache.hadoop.hive.llap.api;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.api.impl.RequestImpl;
import org.apache.hadoop.hive.llap.cache.BufferPool;
import org.apache.hadoop.hive.llap.loader.ChunkPool;
import org.apache.hadoop.hive.llap.loader.OrcLoader;
import org.apache.hadoop.hive.llap.processor.Pool;

public class Llap implements RequestFactory, Configurable {
  public static final Log LOG = LogFactory.getLog(Llap.class);

  // TODO: for now, local "server" is hosted here
  private static class LocalServer {
    public final Pool processorPool;
    public final Configuration conf;
    private LocalServer(Configuration conf) throws IOException {
      this.conf = conf;
      ChunkPool<OrcLoader.ChunkKey> chunkPool = new ChunkPool<OrcLoader.ChunkKey>();
      OrcLoader loader = new OrcLoader(new BufferPool(conf, chunkPool), chunkPool, conf);
      this.processorPool = new Pool(loader, conf);
    }
  }
  private static final Object lsLock = new Object();
  private static LocalServer localServer;

  @Override
  public void setConf(Configuration conf) {
    if (localServer != null) return;
    synchronized (lsLock) {
      if (localServer != null) return;
      try {
        localServer = new LocalServer(conf);
      } catch (IOException e) {
        throw new RuntimeException("Cannot initialize local server", e);
      }
    }
  }

  public Request createLocalRequest() {
    assert localServer != null;
    return new RequestImpl(localServer.processorPool);
  }

  @Override
  public Configuration getConf() {
    return localServer == null ? null : localServer.conf;
  }
}
