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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Simple wrapper for persistent Hashmap implementing only the put/get/remove/clear interface. The
 * main memory hash table acts as a cache and all put/get will operate on it first. If the size of
 * the main memory hash table exceeds a certain threshold, new elements will go into the persistent
 * hash table.
 */

public class HashMapWrapper extends AbstractMapJoinTableContainer implements Serializable {

  private static final long serialVersionUID = 1L;
  protected static final Log LOG = LogFactory.getLog(HashMapWrapper.class);

  // default threshold for using main memory based HashMap
  private static final String THESHOLD_NAME = "threshold";
  private static final String LOAD_NAME = "load";
  private static final int THRESHOLD = 1000000;
  private static final float LOADFACTOR = 0.75f;
  private HashMap<MapJoinKey, MapJoinRowContainer> mHash; // main memory HashMap

  /**
   * Constructor.
   *
   * @param threshold
   *          User specified threshold to store new values into persistent storage.
   */
  public HashMapWrapper(int threshold, float loadFactor) {
    super(createConstructorMetaData(threshold, loadFactor));
    mHash = new HashMap<MapJoinKey, MapJoinRowContainer>(threshold, loadFactor);

  }
  
  public HashMapWrapper(Map<String, String> metaData) {
    super(metaData);
    int threshold = Integer.parseInt(metaData.get(THESHOLD_NAME));
    float loadFactor = Float.parseFloat(metaData.get(LOAD_NAME));
    mHash = new HashMap<MapJoinKey, MapJoinRowContainer>(threshold, loadFactor);
  }

  public HashMapWrapper(int threshold) {
    this(threshold, LOADFACTOR);
  }

  public HashMapWrapper() {
    this(THRESHOLD, LOADFACTOR);
  }

  @Override
  public MapJoinRowContainer get(MapJoinKey key) {
    return mHash.get(key);
  }

  @Override
  public void put(MapJoinKey key, MapJoinRowContainer value) {
    mHash.put(key, value);
  }

  @Override
  public int size() {
    return mHash.size();
  }
  @Override
  public Set<Entry<MapJoinKey, MapJoinRowContainer>> entrySet() {
    return mHash.entrySet();
  }
  @Override
  public void clear() {
    mHash.clear();
  }
  private static Map<String, String> createConstructorMetaData(int threshold, float loadFactor) {
    Map<String, String> metaData = new HashMap<String, String>();
    metaData.put(THESHOLD_NAME, String.valueOf(threshold));
    metaData.put(LOAD_NAME, String.valueOf(loadFactor));
    return metaData;
  }

  @Override
  public MapJoinKey getAnyKey() {
    return mHash.isEmpty() ? null : mHash.keySet().iterator().next();
  }
}
