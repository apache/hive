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
package org.apache.hadoop.hive.llap.io.api;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;


@SuppressWarnings("rawtypes")
public class LlapIoProxy {
  private final static String IMPL_CLASS = "org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl";

  // Llap server depends on Hive execution, so the reverse cannot be true. We create the I/O
  // singleton once (on daemon startup); the said singleton server as the IO interface.
  private static LlapIo io = null;

  public static LlapIo getIo() {
    return io;
  }

  public static void initializeLlapIo(Configuration conf) {

    if (io != null) {
      return; // already initialized
    }

    try {
      io = createIoImpl(conf);
    } catch (IOException e) {
      throw new RuntimeException("Cannot initialize local server", e);
    }
  }

  private static LlapIo createIoImpl(Configuration conf) throws IOException {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends LlapIo> clazz = (Class<? extends LlapIo>)Class.forName(IMPL_CLASS);
      Constructor<? extends LlapIo> ctor = clazz.getDeclaredConstructor(Configuration.class);
      ctor.setAccessible(true);
      return ctor.newInstance(conf);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create impl class", e);
    }
  }
}
