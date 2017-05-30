/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.llap;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.yarn.api.records.Resource;

public enum LlapDaemonInfo {
  INSTANCE;

  private static final class LlapDaemonInfoHolder {
    public LlapDaemonInfoHolder(int numExecutors, long executorMemory, long cacheSize,
      boolean isDirectCache, boolean isLlapIo, final String pid) {
      this.numExecutors = numExecutors;
      this.executorMemory = executorMemory;
      this.cacheSize = cacheSize;
      this.isDirectCache = isDirectCache;
      this.isLlapIo = isLlapIo;
      this.PID = pid;
    }

    final int numExecutors;
    final long executorMemory;
    final long cacheSize;
    final boolean isDirectCache;
    final boolean isLlapIo;
    final String PID;
  }

  // add more variables as required
  private AtomicReference<LlapDaemonInfoHolder> dataRef =
      new AtomicReference<LlapDaemonInfoHolder>();

  public static void initialize(String appName, Configuration daemonConf) {
    int numExecutors = HiveConf.getIntVar(daemonConf, ConfVars.LLAP_DAEMON_NUM_EXECUTORS);
    long executorMemoryBytes =
        HiveConf.getIntVar(daemonConf, ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB) * 1024l * 1024l;
    long ioMemoryBytes = HiveConf.getSizeVar(daemonConf, ConfVars.LLAP_IO_MEMORY_MAX_SIZE);
    boolean isDirectCache = HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_ALLOCATOR_DIRECT);
    boolean isLlapIo = HiveConf.getBoolVar(daemonConf, HiveConf.ConfVars.LLAP_IO_ENABLED, true);
    String pid = System.getenv("JVM_PID");
    initialize(appName, numExecutors, executorMemoryBytes, ioMemoryBytes, isDirectCache, isLlapIo, pid);
  }

  public static void initialize(String appName, int numExecutors, long executorMemoryBytes,
    long ioMemoryBytes, boolean isDirectCache, boolean isLlapIo, final String pid) {
    INSTANCE.dataRef.set(new LlapDaemonInfoHolder(numExecutors, executorMemoryBytes, ioMemoryBytes,
        isDirectCache, isLlapIo, pid));
  }

  public boolean isLlap() {
    return dataRef.get() != null;
  }

  public int getNumExecutors() {
    return dataRef.get().numExecutors;
  }

  public long getExecutorMemory() {
    return dataRef.get().executorMemory;
  }

  public long getMemoryPerExecutor() {
    final LlapDaemonInfoHolder data = dataRef.get();
    return (getExecutorMemory() - -(data.isDirectCache ? 0 : data.cacheSize)) / getNumExecutors();
  }

  public long getCacheSize() {
    return dataRef.get().cacheSize;
  }
  
  public boolean isDirectCache() {
    return dataRef.get().isDirectCache;
  }

  public boolean isLlapIo() {
    return dataRef.get().isLlapIo;
  }

  public String getPID() {
    return dataRef.get().PID;
  }
}
