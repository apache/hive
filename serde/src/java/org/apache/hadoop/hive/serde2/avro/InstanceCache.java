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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;

/**
 * Cache for objects whose creation only depends on some other set of objects
 * and therefore can be used against other equivalent versions of those
 * objects.  Essentially memoizes instance creation.
 *
 * @param <SeedObject>  Object that determines the instance
 * @param <Instance>  Instance that will be created from SeedObject.
 */
public abstract class InstanceCache<SeedObject, Instance> {
  private static final Log LOG = LogFactory.getLog(InstanceCache.class);
  HashMap<Integer, Instance> cache = new HashMap<Integer, Instance>();
  
  public InstanceCache() {}

  /**
   * Retrieve (or create if it doesn't exist) the correct Instance for this
   * SeedObject
   */
  public Instance retrieve(SeedObject hv) throws AvroSerdeException {
    if(LOG.isDebugEnabled()) LOG.debug("Checking for hv: " + hv.toString());

    if(cache.containsKey(hv.hashCode())) {
      if(LOG.isDebugEnabled()) LOG.debug("Returning cache result.");
      return cache.get(hv.hashCode());
    }

    if(LOG.isDebugEnabled()) LOG.debug("Creating new instance and storing in cache");

    Instance instance = makeInstance(hv);
    cache.put(hv.hashCode(), instance);
    return instance;
  }

  protected abstract Instance makeInstance(SeedObject hv) throws AvroSerdeException;
}
