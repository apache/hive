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

import java.io.File;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.hive.ql.util.jdbm.RecordManager;
import org.apache.hadoop.hive.ql.util.jdbm.RecordManagerFactory;
import org.apache.hadoop.hive.ql.util.jdbm.RecordManagerOptions;
import org.apache.hadoop.hive.ql.util.jdbm.htree.HTree;
import org.apache.hadoop.hive.ql.util.jdbm.helper.FastIterator;
import org.apache.hadoop.hive.ql.exec.persistence.MRU;
import org.apache.hadoop.hive.ql.exec.persistence.DCLLItem;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Simple wrapper for persistent Hashmap implementing only the put/get/remove/clear interface.
 * The main memory hash table acts as a cache and all put/get will operate on it first. If the
 * size of the main memory hash table exceeds a certain threshold, new elements will go into
 * the persistent hash table.
 */
public class HashMapWrapper<K,V> {
  
  protected Log LOG = LogFactory.getLog(this.getClass().getName());
  
  // default threshold for using main memory based HashMap
  private static final int THRESHOLD = 25000;
  
  private int threshold;             // threshold to put data into persistent hash table instead
  private HashMap<K,MRUItem> mHash;  // main memory HashMap
  private HTree pHash;               // persistent HashMap
  private RecordManager recman;      // record manager required by HTree
  private File tmpFile;              // temp file holding the persistent data from record manager.
  private MRU<MRUItem> MRUList;      // MRU cache entry
  
  /**
   * Doubly linked list of value items.
   * Note: this is only used along with memory hash table. Persistent hash stores the value directory.
   */
  class MRUItem extends DCLLItem {
    K key;
    V value;
    
    MRUItem(K k, V v) {
      key = k;
      value = v;
    }
  }
  
  /**
   * Constructor.
   * @param threshold User specified threshold to store new values into persistent storage.
   */
  public HashMapWrapper(int threshold) {
    this.threshold = threshold;
    this.pHash = null;
    this.recman = null;
    this.tmpFile = null;
    mHash = new HashMap<K,MRUItem>();
    MRUList = new MRU<MRUItem>();
  }
  
  public HashMapWrapper () {
    this(THRESHOLD);
  }
  
  /**
   * Get the value based on the key. We try to get it from the main memory hash table first.
   * If it is not there we will look up the persistent hash table. This function also guarantees
   * if any item is found given a key, it is available in main memory HashMap. So mutating the 
   * returned value will be reflected (saved) in HashMapWrapper.
   * @param key
   * @return Value corresponding to the key. If the key is not found, return null.
   */
  public V get(K key) throws HiveException {
    V value = null;
    
    // if not the MRU, searching the main memory hash table.
    MRUItem item = mHash.get(key);
    if ( item != null ) {
      value = item.value;
      MRUList.moveToHead(item);
    } else  if ( pHash != null ) {
      try {
        value = (V) pHash.get(key);
        if ( value != null ) { 
          if ( mHash.size() < threshold ) {
            mHash.put(key, new MRUItem(key, value));
 	     	    pHash.remove(key);
          } else if ( threshold > 0 ) { // flush the LRU to disk
            MRUItem tail = MRUList.tail(); // least recently used item
	          pHash.put(tail.key, tail.value);
 	     	    pHash.remove(key);
 	     	    recman.commit();
            
  	        // update mHash -- reuse MRUItem
   	    	  item = mHash.remove(tail.key);
    	      item.key = key;
     	      item.value = value;
      	    mHash.put(key, item);
            
            // update MRU -- reusing MRUItem
      	    tail.key = key;
	          tail.value = value;
  	        MRUList.moveToHead(tail);
          }
        }
      } catch ( Exception e ) {
        LOG.warn(e.toString());
        throw new HiveException(e);
      }
    } 
    return value;
  }
  
  /**
   * Put the key value pair in the hash table. It will first try to 
   * put it into the main memory hash table. If the size exceeds the
   * threshold, it will put it into the persistent hash table.
   * @param key
   * @param value
   * @throws HiveException
   */
  public void put(K key, V value)  throws HiveException {
    int mm_size = mHash.size();
    MRUItem itm = mHash.get(key);
    
    if (mm_size < threshold) {
      if ( itm != null ) {
        // re-use the MRU item -- just overwrite value, key is the same
        itm.value = value;
	      MRUList.moveToHead(itm);
	      if (!mHash.get(key).value.equals(value))
	        LOG.error("HashMapWrapper.put() reuse MRUItem inconsistency [1].");
	      assert(mHash.get(key).value.equals(value));
      } else {
        // check if key already exists in pHash
        try {
          if ( pHash != null && pHash.get(key) != null ) {
            // remove the old item from pHash and insert the new one
            pHash.remove(key);
            pHash.put(key, value);
            recman.commit();
         		return;
          }
        } catch (Exception e) {
          e.printStackTrace();
          throw new HiveException(e);
        }
        itm = new MRUItem(key,value);
        MRUList.put(itm);
	      mHash.put(key, itm);
      }
    } else {
      if ( itm != null ) { // replace existing item
        // re-use the MRU item -- just overwrite value, key is the same
        itm.value = value;
	      MRUList.moveToHead(itm);
	      if (!mHash.get(key).value.equals(value))
	        LOG.error("HashMapWrapper.put() reuse MRUItem inconsistency [2].");
	      assert(mHash.get(key).value.equals(value));
      } else {
        // for items inserted into persistent hash table, we don't put it into MRU
        if (pHash == null) {
          pHash = getPersistentHash();
        }
        try {
          pHash.put(key, value);
          recman.commit();
        } catch (Exception e) {
          LOG.warn(e.toString());
          throw new HiveException(e);
        }
      }
    }
  }
  
  /**
   * Get the persistent hash table.
   * @return persistent hash table
   * @throws HiveException
   */
  private HTree getPersistentHash() throws HiveException {
    try {
      // Create a temporary file for the page manager to hold persistent data. 
    	if ( tmpFile != null ) {
    	  tmpFile.delete();
      }
      tmpFile = File.createTempFile("HashMapWrapper", ".tmp", new File("/tmp"));
      LOG.info("HashMapWrapper created temp file " + tmpFile.getAbsolutePath());
      // Delete the temp file if the JVM terminate normally through Hadoop job kill command.
      // Caveat: it won't be deleted if JVM is killed by 'kill -9'.
      tmpFile.deleteOnExit(); 
      
      Properties props = new Properties();
      props.setProperty(RecordManagerOptions.CACHE_TYPE, RecordManagerOptions.NO_CACHE);
    	props.setProperty(RecordManagerOptions.DISABLE_TRANSACTIONS, "true" );
      
      recman = RecordManagerFactory.createRecordManager(tmpFile, props );
      pHash = HTree.createInstance(recman);
    } catch (Exception e) {
      LOG.warn(e.toString());
      throw new HiveException(e);
    } 
    return pHash;
  }
  
  /**
   * Clean up the hash table. All elements in the main memory hash table will be removed, and
   * the persistent hash table will be destroyed (temporary file will be deleted).
   */
  public void clear() throws HiveException {
    if ( mHash != null ) {
      mHash.clear();
      MRUList.clear();
    }
    close();
  }
  
  /**
   * Remove one key-value pairs from the hash table based on the given key. If the pairs are
   * removed from the main memory hash table, pairs in the persistent hash table will not be
   * moved to the main memory hash table. Future inserted elements will go into the main memory
   * hash table though.
   * @param key
   * @throws HiveException
   */
  public void remove(Object key) throws HiveException {
    MRUItem entry = mHash.remove(key);
    if ( entry != null ) {
      MRUList.remove(entry);
    } else if ( pHash != null ) {
      try {
        pHash.remove(key);
      } catch (Exception e) {
        LOG.warn(e.toString());
        throw new HiveException(e);
      }
    }
  }
  
  /**
   * Get a list of all keys in the hash map.
   * @return
   */
  public Set<K> keySet() {
    HashSet<K> ret = null;
    if ( mHash != null ) {
      ret = new HashSet<K>();
      ret.addAll(mHash.keySet());
    }
    if ( pHash != null ) {
      try {
        FastIterator fitr = pHash.keys();
	      if ( fitr != null ) {
 	        K k;
  	      while ( (k = (K) fitr.next()) != null )
   	        ret.add(k);
    	  }
   	  } catch (Exception e) {
        e.printStackTrace();
   	  }
    }
    return ret;
  }
  
  /**
   * Get the main memory cache capacity. 
   * @return the maximum number of items can be put into main memory HashMap cache.
   */
  public int cacheSize() {
    return threshold;
  }
  
  /**
   * Close the persistent hash table and clean it up.
   * @throws HiveException
   */
  public void close() throws HiveException {
    
    if ( pHash != null ) {
      try {
        if ( recman != null )
          recman.close();
      }  catch (Exception e) {
        throw new HiveException(e);
      }
      // delete the temporary file
      tmpFile.delete();
      tmpFile = null;
      pHash   = null;
      recman  = null;
    }
  }
}
