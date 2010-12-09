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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;


/**
 * Simple wrapper for persistent Hashmap implementing only the put/get/remove/clear interface. The
 * main memory hash table acts as a cache and all put/get will operate on it first. If the size of
 * the main memory hash table exceeds a certain threshold, new elements will go into the persistent
 * hash table.
 */

public class HashMapWrapper<K, V> implements Serializable {

  private static final long serialVersionUID = 1L;
  protected Log LOG = LogFactory.getLog(this.getClass().getName());

  // default threshold for using main memory based HashMap

  private static final int THRESHOLD = 1000000;
  private static final float LOADFACTOR = 0.75f;
  private static final float MEMORYUSAGE = 1;

  private float maxMemoryUsage;
  private HashMap<K, V> mHash; // main memory HashMap
  protected transient LogHelper console;

  private File dumpFile;
  public static MemoryMXBean memoryMXBean;
  private long maxMemory;
  private long currentMemory;
  private NumberFormat num;

  /**
   * Constructor.
   *
   * @param threshold
   *          User specified threshold to store new values into persistent storage.
   */
  public HashMapWrapper(int threshold, float loadFactor, float memoryUsage) {
    maxMemoryUsage = memoryUsage;
    mHash = new HashMap<K, V>(threshold, loadFactor);
    memoryMXBean = ManagementFactory.getMemoryMXBean();
    maxMemory = memoryMXBean.getHeapMemoryUsage().getMax();
    LOG.info("maximum memory: " + maxMemory);
    num = NumberFormat.getInstance();
    num.setMinimumFractionDigits(2);
  }

  public HashMapWrapper(int threshold) {
    this(threshold, LOADFACTOR, MEMORYUSAGE);
  }

  public HashMapWrapper() {
    this(THRESHOLD, LOADFACTOR, MEMORYUSAGE);
  }

  public V get(K key) {
    return mHash.get(key);
  }

  public boolean put(K key, V value) throws HiveException {
    // isAbort();
    mHash.put(key, value);
    return false;
  }


  public void remove(K key) {
    mHash.remove(key);
  }

  /**
   * Flush the main memory hash table into the persistent cache file
   *
   * @return persistent cache file
   */
  public long flushMemoryCacheToPersistent(File file) throws IOException {
    ObjectOutputStream outputStream = null;
    outputStream = new ObjectOutputStream(new FileOutputStream(file));
    outputStream.writeObject(mHash);
    outputStream.flush();
    outputStream.close();

    return file.length();
  }

  public void initilizePersistentHash(String fileName) throws IOException, ClassNotFoundException {
    ObjectInputStream inputStream = null;
    inputStream = new ObjectInputStream(new FileInputStream(fileName));
    HashMap<K, V> hashtable = (HashMap<K, V>) inputStream.readObject();
    this.setMHash(hashtable);

    inputStream.close();
  }

  public int size() {
    return mHash.size();
  }

  public Set<K> keySet() {
    return mHash.keySet();
  }


  /**
   * Close the persistent hash table and clean it up.
   *
   * @throws HiveException
   */
  public void close() throws HiveException {
    mHash.clear();
  }

  public void clear() throws HiveException {
    mHash.clear();
  }

  public int getKeySize() {
    return mHash.size();
  }

  public boolean isAbort(long numRows,LogHelper console) {
    System.gc();
    System.gc();
    int size = mHash.size();
    long usedMemory = memoryMXBean.getHeapMemoryUsage().getUsed();
    double rate = (double) usedMemory / (double) maxMemory;
    console.printInfo(Utilities.now() + "\tProcessing rows:\t" + numRows + "\tHashtable size:\t"
        + size + "\tMemory usage:\t" + usedMemory + "\trate:\t" + num.format(rate));
    if (rate > (double) maxMemoryUsage) {
      return true;
    }
    return false;
  }

  public void setLOG(Log log) {
    LOG = log;
  }

  public HashMap<K, V> getMHash() {
    return mHash;
  }

  public void setMHash(HashMap<K, V> hash) {
    mHash = hash;
  }

  public LogHelper getConsole() {
    return console;
  }

  public void setConsole(LogHelper console) {
    this.console = console;
  }

  public File getDumpFile() {
    return dumpFile;
  }

  public void setDumpFile(File dumpFile) {
    this.dumpFile = dumpFile;
  }

  public static MemoryMXBean getMemoryMXBean() {
    return memoryMXBean;
  }

  public static void setMemoryMXBean(MemoryMXBean memoryMXBean) {
    HashMapWrapper.memoryMXBean = memoryMXBean;
  }

  public long getMaxMemory() {
    return maxMemory;
  }

  public void setMaxMemory(long maxMemory) {
    this.maxMemory = maxMemory;
  }

  public long getCurrentMemory() {
    return currentMemory;
  }

  public void setCurrentMemory(long currentMemory) {
    this.currentMemory = currentMemory;
  }

  public NumberFormat getNum() {
    return num;
  }

  public void setNum(NumberFormat num) {
    this.num = num;
  }

  public static int getTHRESHOLD() {
    return THRESHOLD;
  }

}
