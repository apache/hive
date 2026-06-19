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
package org.apache.hadoop.hive.llap.io.api;

import java.lang.reflect.Constructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.coordinator.LlapCoordinator;

@SuppressWarnings("rawtypes")
public class LlapProxy {
  private final static String IO_IMPL_CLASS = "org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl";

  // Llap server depends on Hive execution, so the reverse cannot be true. We create the I/O
  // singleton once (on daemon startup); the said singleton serves as the IO interface.
  private static LlapIo io = null;

  private static boolean isDaemon = false;

  public static void setDaemon(boolean isDaemon) {
    LlapProxy.isDaemon = isDaemon;
  }

  public static boolean isDaemon() {
    return isDaemon;
  }

  public static LlapIo getIo() {
    return io;
  }

  public static void initializeLlapIo(Configuration conf) {
    if (io != null) {
      return; // already initialized
    }
    io = createInstance(IO_IMPL_CLASS, conf);
  }

  private static <T> T createInstance(String className, Configuration conf) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends T> clazz = (Class<? extends T>)Class.forName(className);
      Constructor<? extends T> ctor = clazz.getDeclaredConstructor(Configuration.class);
      ctor.setAccessible(true);
      return ctor.newInstance(conf);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create " + className, e);
    }
  }

  public static void close() {
    if (io != null) {
      io.close();
    }
  }
}
