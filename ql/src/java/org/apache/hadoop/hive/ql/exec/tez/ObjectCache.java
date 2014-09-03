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

package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.runtime.api.ObjectRegistry;

import com.google.common.base.Preconditions;

/**
 * ObjectCache. Tez implementation based on the tez object registry.
 *
 */
public class ObjectCache implements org.apache.hadoop.hive.ql.exec.ObjectCache {
  
  private static final Log LOG = LogFactory.getLog(ObjectCache.class.getName());
  
  // ObjectRegistry is available via the Input/Output/ProcessorContext.
  // This is setup as part of the Tez Processor construction, so that it is available whenever an
  // instance of the ObjectCache is created. The assumption is that Tez will initialize the Processor
  // before anything else.
  private volatile static ObjectRegistry staticRegistry;
 
  private final ObjectRegistry registry;
  
  public ObjectCache() {
    Preconditions.checkNotNull(staticRegistry,
        "Object registry not setup yet. This should have been setup by the TezProcessor");
    registry = staticRegistry;
  }

  public static void setupObjectRegistry(ObjectRegistry objectRegistry) {
    staticRegistry = objectRegistry;
  }
  
  @Override
  public void cache(String key, Object value) {
    LOG.info("Adding " + key + " to cache with value " + value);
    registry.cacheForVertex(key, value);
  }

  @Override
  public Object retrieve(String key) {
    Object o = registry.get(key);
    if (o != null) {
      LOG.info("Found " + key + " in cache with value: " + o);
    }
    return o;
  }
}
