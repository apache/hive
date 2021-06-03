/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.avro;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache for objects whose creation only depends on some other set of objects and therefore can be
 * used against other equivalent versions of those objects. Essentially memoizes instance creation.
 *
 * @param <SeedObject> Object that determines the instance. The cache uses this object as a key for
 *          its hash which is why it is imperative to have appropriate equals and hashcode
 *          implementation for this object for the cache to work properly
 * @param <Instance> Instance that will be created from SeedObject.
 */
public abstract class InstanceCache<SeedObject, Instance> {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceCache.class);
  Map<SeedObject, Instance> cache = new ConcurrentHashMap<>();
  
  public InstanceCache() {}

  /**
   * Retrieve (or create if it doesn't exist) the correct Instance for this
   * SeedObject
   */
  public Instance retrieve(SeedObject hv) throws AvroSerdeException {
    return retrieve(hv, null);
  }

  /**
   * Retrieve (or create if it doesn't exist) the correct Instance for this
   * SeedObject using 'seenSchemas' to resolve circular references
   */
  public Instance retrieve(SeedObject hv, Set<SeedObject> seenSchemas)
    throws AvroSerdeException {
    LOG.debug("Checking for hv: {}", hv);

    if(cache.containsKey(hv)) {
      LOG.debug("Returning cache result");
      return cache.get(hv);
    } else {
      LOG.debug("Creating new instance and storing in cache");
      Instance newInstance = makeInstance(hv, seenSchemas);
      Instance cachedInstance = cache.putIfAbsent(hv, newInstance);
      return cachedInstance == null ? newInstance : cachedInstance;
    }
  }

  protected abstract Instance makeInstance(SeedObject hv,
      Set<SeedObject> seenSchemas) throws AvroSerdeException;
}
