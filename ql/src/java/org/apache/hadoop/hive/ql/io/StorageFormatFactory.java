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

package org.apache.hadoop.hive.ql.io;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.ImmutableMap;

public class StorageFormatFactory {
  private static final Log LOG = LogFactory.getLog(StorageFormatFactory.class);

  private final Map<String, StorageFormatDescriptor> storageFormats;

  public StorageFormatFactory() {
    Map<String, StorageFormatDescriptor> localStorageFormats =
        new HashMap<String, StorageFormatDescriptor>();
    for (StorageFormatDescriptor descriptor : ServiceLoader.load(StorageFormatDescriptor.class)) {
      for (String name : descriptor.getNames()) {
        name = name.trim().toUpperCase();
        StorageFormatDescriptor oldDescriptor  = localStorageFormats.put(name, descriptor);
        if (oldDescriptor != null) {
          String msg = "Storage Format Descriptor conflict at name '" + name + "', " +
              "the descriptor " + descriptor + " is overriding " + oldDescriptor;
          LOG.warn(msg);
        }
      }
    }
    this.storageFormats = ImmutableMap.copyOf(localStorageFormats);
  }

  public @Nullable StorageFormatDescriptor get(String name) {
    name = name.trim().toUpperCase();
    return storageFormats.get(name);
  }
}
