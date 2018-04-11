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
package org.apache.hadoop.hive.llap.cache;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;

import sun.misc.Cleaner;

public final class SimpleAllocator implements Allocator, BuddyAllocatorMXBean {
  private final boolean isDirect;
  private static Field cleanerField;
  static {
    try {
      // TODO: To make it work for JDK9 use CleanerUtil from https://issues.apache.org/jira/browse/HADOOP-12760
      final Class<?> dbClazz = Class.forName("java.nio.DirectByteBuffer");
      cleanerField = dbClazz.getDeclaredField("cleaner");
      cleanerField.setAccessible(true);
    } catch (Throwable t) {
      LlapIoImpl.LOG.warn("Cannot initialize DirectByteBuffer cleaner", t);
      cleanerField = null;
    }
  }

  public SimpleAllocator(Configuration conf) {
    isDirect = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_DIRECT);
    if (LlapIoImpl.LOG.isInfoEnabled()) {
      LlapIoImpl.LOG.info("Simple allocator with " + (isDirect ? "direct" : "byte") + " buffers");
    }
  }


  @Override
  @Deprecated
  public void allocateMultiple(MemoryBuffer[] dest, int size) {
    allocateMultiple(dest, size, null);
  }

  @Override
  public void allocateMultiple(MemoryBuffer[] dest, int size, BufferObjectFactory factory) {
    for (int i = 0; i < dest.length; ++i) {
      LlapAllocatorBuffer buf = null;
      if (dest[i] == null) {
      // Note: this is backward compat only. Should be removed with createUnallocated.
        dest[i] = buf = (factory != null)
            ? (LlapAllocatorBuffer)factory.create() : createUnallocated();
      } else {
        buf = (LlapAllocatorBuffer)dest[i];
      }
      ByteBuffer bb = isDirect ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
      buf.initialize(bb, 0, size);
    }
  }

  @Override
  public void deallocate(MemoryBuffer buffer) {
    LlapAllocatorBuffer buf = (LlapAllocatorBuffer)buffer;
    ByteBuffer bb = buf.byteBuffer;
    buf.byteBuffer = null;
    if (!bb.isDirect()) return;
    Field field = cleanerField;
    if (field == null) return;
    try {
      ((Cleaner)field.get(bb)).clean();
    } catch (Throwable t) {
      LlapIoImpl.LOG.warn("Error using DirectByteBuffer cleaner; stopping its use", t);
      cleanerField = null;
    }
  }

  @Override
  public boolean isDirectAlloc() {
    return isDirect;
  }

  @Override
  @Deprecated
  public LlapAllocatorBuffer createUnallocated() {
    return new LlapDataBuffer();
  }

  // BuddyAllocatorMXBean
  @Override
  public boolean getIsDirect() {
    return isDirect;
  }

  @Override
  public int getMinAllocation() {
    return 0;
  }

  @Override
  public int getMaxAllocation() {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getArenaSize() {
    return -1;
  }

  @Override
  public long getMaxCacheSize() {
    return Integer.MAX_VALUE;
  }
}
