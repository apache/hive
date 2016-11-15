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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.JoinUtil.JoinResult;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinBytesTableContainer.KeyValueHelper;
import org.apache.hadoop.hive.ql.exec.vector.VectorHashKeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinRowBytesContainer;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.WriteBuffers;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryFactory;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.BloomFilter;
import org.apache.hive.common.util.HashCodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

/**
 * Hash table container that can have many partitions -- each partition has its own hashmap,
 * as well as row container for small table and big table.
 *
 * The purpose is to distribute rows into multiple partitions so that when the entire small table
 * cannot fit into memory, we are still able to perform hash join, by processing them recursively.
 *
 * Partitions that can fit in memory will be processed first, and then every spilled partition will
 * be restored and processed one by one.
 */
public class HybridHashTableContainer
      implements MapJoinTableContainer, MapJoinTableContainerDirectAccess {
  private static final Logger LOG = LoggerFactory.getLogger(HybridHashTableContainer.class);

  private final HashPartition[] hashPartitions; // an array of partitions holding the triplets
  private int totalInMemRowCount = 0;           // total number of small table rows in memory
  private long memoryThreshold;                 // the max memory limit that can be allocated
  private long memoryUsed;                      // the actual memory used
  private final long tableRowSize;              // row size of the small table
  private boolean isSpilled;                    // whether there's any spilled partition
  private int toSpillPartitionId;               // the partition into which to spill the big table row;
                                                // This may change after every setMapJoinKey call
  private int numPartitionsSpilled;             // number of spilled partitions
  private boolean lastPartitionInMem;           // only one (last one) partition is left in memory
  private final int memoryCheckFrequency;       // how often (# of rows apart) to check if memory is full
  private final HybridHashTableConf nwayConf;         // configuration for n-way join
  private int writeBufferSize;                  // write buffer size for BytesBytesMultiHashMap

  /** The OI used to deserialize values. We never deserialize keys. */
  private LazyBinaryStructObjectInspector internalValueOi;
  private boolean[] sortableSortOrders;
  private byte[] nullMarkers;
  private byte[] notNullMarkers;
  private MapJoinBytesTableContainer.KeyValueHelper writeHelper;
  private final MapJoinBytesTableContainer.DirectKeyValueWriter directWriteHelper;
  /*
   * this is not a real bloom filter, but is a cheap version of the 1-memory
   * access bloom filters
   *
   * In several cases, we'll have map-join spills because the value columns are
   * a few hundred columns of Text each, while there are very few keys in total
   * (a few thousand).
   *
   * This is a cheap exit option to prevent spilling the big-table in such a
   * scenario.
   */
  private transient BloomFilter bloom1 = null;
  private final int BLOOM_FILTER_MAX_SIZE = 300000000;

  private final List<Object> EMPTY_LIST = new ArrayList<Object>(0);

  private final String spillLocalDirs;

  /**
   * This class encapsulates the triplet together since they are closely related to each other
   * The triplet: hashmap (either in memory or on disk), small table container, big table container
   */
  public static class HashPartition {
    BytesBytesMultiHashMap hashMap;         // In memory hashMap
    KeyValueContainer sidefileKVContainer;  // Stores small table key/value pairs
    ObjectContainer matchfileObjContainer;  // Stores big table rows
    VectorMapJoinRowBytesContainer matchfileRowBytesContainer;
                                            // Stores big table rows as bytes for native vector map join.
    Path hashMapLocalPath;                  // Local file system path for spilled hashMap
    boolean hashMapOnDisk;                  // Status of hashMap. true: on disk, false: in memory
    boolean hashMapSpilledOnCreation;       // When there's no enough memory, cannot create hashMap
    int initialCapacity;                    // Used to create an empty BytesBytesMultiHashMap
    float loadFactor;                       // Same as above
    int wbSize;                             // Same as above
    int rowsOnDisk;                         // How many rows saved to the on-disk hashmap (if on disk)
    private final String spillLocalDirs;

    /* It may happen that there's not enough memory to instantiate a hashmap for the partition.
     * In that case, we don't create the hashmap, but pretend the hashmap is directly "spilled".
     */
    public HashPartition(int initialCapacity, float loadFactor, int wbSize, long maxProbeSize,
                         boolean createHashMap, String spillLocalDirs) {
      if (createHashMap) {
        // Probe space should be at least equal to the size of our designated wbSize
        maxProbeSize = Math.max(maxProbeSize, wbSize);
        hashMap = new BytesBytesMultiHashMap(initialCapacity, loadFactor, wbSize, maxProbeSize);
      } else {
        hashMapSpilledOnCreation = true;
        hashMapOnDisk = true;
      }
      this.spillLocalDirs = spillLocalDirs;
      this.initialCapacity = initialCapacity;
      this.loadFactor = loadFactor;
      this.wbSize = wbSize;
    }

    /* Get the in memory hashmap */
    public BytesBytesMultiHashMap getHashMapFromMemory() {
      return hashMap;
    }

    /* Restore the hashmap from disk by deserializing it.
     * Currently Kryo is used for this purpose.
     */
    public BytesBytesMultiHashMap getHashMapFromDisk(int rowCount)
        throws IOException, ClassNotFoundException {
      if (hashMapSpilledOnCreation) {
        return new BytesBytesMultiHashMap(rowCount, loadFactor, wbSize, -1);
      } else {
        InputStream inputStream = Files.newInputStream(hashMapLocalPath);
        com.esotericsoftware.kryo.io.Input input = new com.esotericsoftware.kryo.io.Input(inputStream);
        Kryo kryo = SerializationUtilities.borrowKryo();
        BytesBytesMultiHashMap restoredHashMap = null;
        try {
          restoredHashMap = kryo.readObject(input, BytesBytesMultiHashMap.class);
        } finally {
          SerializationUtilities.releaseKryo(kryo);
        }

        if (rowCount > 0) {
          restoredHashMap.expandAndRehashToTarget(rowCount);
        }

        // some bookkeeping
        rowsOnDisk = 0;
        hashMapOnDisk = false;

        input.close();
        inputStream.close();
        Files.delete(hashMapLocalPath);
        return restoredHashMap;
      }
    }

    /* Get the small table key/value container */
    public KeyValueContainer getSidefileKVContainer() {
      if (sidefileKVContainer == null) {
        sidefileKVContainer = new KeyValueContainer(spillLocalDirs);
      }
      return sidefileKVContainer;
    }

    /* Get the big table row container */
    public ObjectContainer getMatchfileObjContainer() {
      if (matchfileObjContainer == null) {
        matchfileObjContainer = new ObjectContainer(spillLocalDirs);
      }
      return matchfileObjContainer;
    }

    /* Get the big table row bytes container for native vector map join */
    public VectorMapJoinRowBytesContainer getMatchfileRowBytesContainer() {
      if (matchfileRowBytesContainer == null) {
        matchfileRowBytesContainer = new VectorMapJoinRowBytesContainer(spillLocalDirs);
      }
      return matchfileRowBytesContainer;
    }

    /* Check if hashmap is on disk or in memory */
    public boolean isHashMapOnDisk() {
      return hashMapOnDisk;
    }

    public void clear() {
      if (hashMap != null) {
        hashMap.clear();
        hashMap = null;
      }

      if (hashMapLocalPath != null) {
        try {
          Files.delete(hashMapLocalPath);
        } catch (Throwable ignored) {
        }
        hashMapLocalPath = null;
        rowsOnDisk = 0;
        hashMapOnDisk = false;
      }

      if (sidefileKVContainer != null) {
        sidefileKVContainer.clear();
        sidefileKVContainer = null;
      }

      if (matchfileObjContainer != null) {
        matchfileObjContainer.clear();
        matchfileObjContainer = null;
      }

      if (matchfileRowBytesContainer != null) {
        matchfileRowBytesContainer.clear();
        matchfileRowBytesContainer = null;
      }
    }

    public int size() {
      if (isHashMapOnDisk()) {
        // Rows are in a combination of the on-disk hashmap and the sidefile
        return rowsOnDisk + (sidefileKVContainer != null ? sidefileKVContainer.size() : 0);
      } else {
        // All rows should be in the in-memory hashmap
        return hashMap.size();
      }
    }
  }

  public HybridHashTableContainer(Configuration hconf, long keyCount, long memoryAvailable,
                                  long estimatedTableSize, HybridHashTableConf nwayConf)
      throws SerDeException, IOException {
    this(HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVEHASHTABLEKEYCOUNTADJUSTMENT),
        HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLETHRESHOLD),
        HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVEHASHTABLELOADFACTOR),
        HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHYBRIDGRACEHASHJOINMEMCHECKFREQ),
        HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHYBRIDGRACEHASHJOINMINWBSIZE),
        HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLEWBSIZE),
        HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHYBRIDGRACEHASHJOINMINNUMPARTITIONS),
        HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVEMAPJOINOPTIMIZEDTABLEPROBEPERCENT),
        HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVEHYBRIDGRACEHASHJOINBLOOMFILTER),
        estimatedTableSize, keyCount, memoryAvailable, nwayConf,
        HiveUtils.getLocalDirList(hconf));
  }

  private HybridHashTableContainer(float keyCountAdj, int threshold, float loadFactor,
      int memCheckFreq, int minWbSize, int maxWbSize, int minNumParts, float probePercent,
      boolean useBloomFilter, long estimatedTableSize, long keyCount, long memoryAvailable,
      HybridHashTableConf nwayConf, String spillLocalDirs)
      throws SerDeException, IOException {
    directWriteHelper = new MapJoinBytesTableContainer.DirectKeyValueWriter();

    int newKeyCount = HashMapWrapper.calculateTableSize(
        keyCountAdj, threshold, loadFactor, keyCount);

    memoryThreshold = memoryAvailable;
    tableRowSize = estimatedTableSize / (keyCount != 0 ? keyCount : 1);
    memoryCheckFrequency = memCheckFreq;
    this.spillLocalDirs = spillLocalDirs;

    this.nwayConf = nwayConf;
    int numPartitions;
    if (nwayConf == null) { // binary join
      numPartitions = calcNumPartitions(memoryThreshold, estimatedTableSize, minNumParts, minWbSize);
      writeBufferSize = (int)(estimatedTableSize / numPartitions);
    } else {                // n-way join
      // It has been calculated in HashTableLoader earlier, so just need to retrieve that number
      numPartitions = nwayConf.getNumberOfPartitions();
      if (nwayConf.getLoadedContainerList().size() == 0) {  // n-way: first small table
        writeBufferSize = (int)(estimatedTableSize / numPartitions);
      } else {                                              // n-way: all later small tables
        while (memoryThreshold < numPartitions * minWbSize) {
          // Spill previously loaded tables to make more room
          long memFreed = nwayConf.spill();
          if (memFreed == 0) {
            LOG.warn("Available memory is not enough to create HybridHashTableContainers" +
                " consistently!");
            break;
          } else {
            LOG.info("Total available memory was: " + memoryThreshold);
            memoryThreshold += memFreed;
          }
        }
        writeBufferSize = (int)(memoryThreshold / numPartitions);
      }
    }
    LOG.info("Total available memory is: " + memoryThreshold);

    // Round to power of 2 here, as is required by WriteBuffers
    writeBufferSize = Integer.bitCount(writeBufferSize) == 1 ?
        writeBufferSize : Integer.highestOneBit(writeBufferSize);

    // Cap WriteBufferSize to avoid large preallocations
    // We also want to limit the size of writeBuffer, because we normally have 16 partitions, that
    // makes spilling prediction (isMemoryFull) to be too defensive which results in unnecessary spilling
    writeBufferSize = writeBufferSize < minWbSize ? minWbSize : Math.min(maxWbSize / numPartitions, writeBufferSize);
    LOG.info("Write buffer size: " + writeBufferSize);
    memoryUsed = 0;

    if (useBloomFilter) {
      if (newKeyCount <= BLOOM_FILTER_MAX_SIZE) {
        this.bloom1 = new BloomFilter(newKeyCount);
      } else {
        // To avoid having a huge BloomFilter we need to scale up False Positive Probability
        double fpp = calcFPP(newKeyCount);
        assert fpp < 1 : "Too many keys! BloomFilter False Positive Probability is 1!";
        if (fpp >= 0.5) {
          LOG.warn("BloomFilter FPP is greater than 0.5!");
        }
        LOG.info("BloomFilter is using FPP: " + fpp);
        this.bloom1 = new BloomFilter(newKeyCount, fpp);
      }
      LOG.info(String.format("Using a bloom-1 filter %d keys of size %d bytes",
        newKeyCount, bloom1.sizeInBytes()));
      memoryUsed = bloom1.sizeInBytes();
    }

    hashPartitions = new HashPartition[numPartitions];
    int numPartitionsSpilledOnCreation = 0;
    int initialCapacity = Math.max(newKeyCount / numPartitions, threshold / numPartitions);
    // maxCapacity should be calculated based on a percentage of memoryThreshold, which is to divide
    // row size using long size
    float probePercentage = (float) 8 / (tableRowSize + 8); // long_size / tableRowSize + long_size
    if (probePercentage == 1) {
      probePercentage = probePercent;
    }
    int maxCapacity = (int)(memoryThreshold * probePercentage);
    for (int i = 0; i < numPartitions; i++) {
      if (this.nwayConf == null ||                          // binary join
          nwayConf.getLoadedContainerList().size() == 0) {  // n-way join, first (biggest) small table
        if (i == 0) { // We unconditionally create a hashmap for the first hash partition
          hashPartitions[i] = new HashPartition(initialCapacity, loadFactor, writeBufferSize,
              maxCapacity, true, spillLocalDirs);
          LOG.info("Each new partition will require memory: " + hashPartitions[0].hashMap.memorySize());
        } else {
          // To check whether we have enough memory to allocate for another hash partition,
          // we need to get the size of the first hash partition to get an idea.
          hashPartitions[i] = new HashPartition(initialCapacity, loadFactor, writeBufferSize,
              maxCapacity, memoryUsed + hashPartitions[0].hashMap.memorySize() < memoryThreshold,
              spillLocalDirs);
        }
      } else {                                              // n-way join, all later small tables
        // For all later small tables, follow the same pattern of the previously loaded tables.
        if (this.nwayConf.doSpillOnCreation(i)) {
          hashPartitions[i] = new HashPartition(initialCapacity, loadFactor, writeBufferSize,
              maxCapacity, false, spillLocalDirs);
        } else {
          hashPartitions[i] = new HashPartition(initialCapacity, loadFactor, writeBufferSize,
              maxCapacity, true, spillLocalDirs);
        }
      }

      if (isHashMapSpilledOnCreation(i)) {
        numPartitionsSpilledOnCreation++;
        numPartitionsSpilled++;
        this.setSpill(true);
        if (this.nwayConf != null && this.nwayConf.getNextSpillPartition() == numPartitions - 1) {
          this.nwayConf.setNextSpillPartition(i - 1);
        }
        LOG.info("Hash partition " + i + " is spilled on creation.");
      } else {
        memoryUsed += hashPartitions[i].hashMap.memorySize();
        LOG.info("Hash partition " + i + " is created in memory. Total memory usage so far: " + memoryUsed);
      }
    }

    if (writeBufferSize * (numPartitions - numPartitionsSpilledOnCreation) > memoryThreshold) {
      LOG.error("There is not enough memory to allocate " +
          (numPartitions - numPartitionsSpilledOnCreation) + " hash partitions.");
    }
    assert numPartitionsSpilledOnCreation != numPartitions : "All partitions are directly spilled!" +
        " It is not supported now.";
    LOG.info("Number of partitions created: " + numPartitions);
    LOG.info("Number of partitions spilled directly to disk on creation: "
        + numPartitionsSpilledOnCreation);

    // Append this container to the loaded list
    if (this.nwayConf != null) {
      this.nwayConf.getLoadedContainerList().add(this);
    }
  }

  /**
   * Calculate the proper False Positive Probability so that the BloomFilter won't grow too big
   * @param keyCount number of keys
   * @return FPP
   */
  private double calcFPP(int keyCount) {
    int n = keyCount;
    double p = 0.05;

    // Calculation below is consistent with BloomFilter.optimalNumOfBits().
    // Also, we are capping the BloomFilter size below 100 MB (800000000/8)
    while ((-n * Math.log(p) / (Math.log(2) * Math.log(2))) > 800000000) {
      p += 0.05;
    }
    return p;
  }

  public MapJoinBytesTableContainer.KeyValueHelper getWriteHelper() {
    return writeHelper;
  }

  public HashPartition[] getHashPartitions() {
    return hashPartitions;
  }

  public long getMemoryThreshold() {
    return memoryThreshold;
  }

  /**
   * Get the current memory usage by recalculating it.
   * @return current memory usage
   */
  private long refreshMemoryUsed() {
    long memUsed = bloom1 != null ? bloom1.sizeInBytes() : 0;
    for (HashPartition hp : hashPartitions) {
      if (hp.hashMap != null) {
        memUsed += hp.hashMap.memorySize();
      } else {
        // also include the still-in-memory sidefile, before it has been truely spilled
        if (hp.sidefileKVContainer != null) {
          memUsed += hp.sidefileKVContainer.numRowsInReadBuffer() * tableRowSize;
        }
      }
    }
    return memoryUsed = memUsed;
  }

  public LazyBinaryStructObjectInspector getInternalValueOi() {
    return internalValueOi;
  }

  public boolean[] getSortableSortOrders() {
    return sortableSortOrders;
  }

  public byte[] getNullMarkers() {
    return nullMarkers;
  }

  public byte[] getNotNullMarkers() {
    return notNullMarkers;
  }

  /* For a given row, put it into proper partition based on its hash value.
   * When memory threshold is reached, the biggest hash table in memory will be spilled to disk.
   * If the hash table of a specific partition is already on disk, all later rows will be put into
   * a row container for later use.
   */
  @SuppressWarnings("deprecation")
  @Override
  public MapJoinKey putRow(Writable currentKey, Writable currentValue)
      throws SerDeException, HiveException, IOException {
    writeHelper.setKeyValue(currentKey, currentValue);
    return internalPutRow(writeHelper, currentKey, currentValue);
  }

  private MapJoinKey internalPutRow(KeyValueHelper keyValueHelper,
          Writable currentKey, Writable currentValue) throws SerDeException, IOException {

    boolean putToSidefile = false; // by default we put row into partition in memory

    // Next, put row into corresponding hash partition
    int keyHash = keyValueHelper.getHashFromKey();
    int partitionId = keyHash & (hashPartitions.length - 1);
    HashPartition hashPartition = hashPartitions[partitionId];

    if (bloom1 != null) {
      bloom1.addLong(keyHash);
    }

    if (isOnDisk(partitionId) || isHashMapSpilledOnCreation(partitionId)) { // destination on disk
      putToSidefile = true;
    } else {  // destination in memory
      if (!lastPartitionInMem &&        // If this is the only partition in memory, proceed without check
          (hashPartition.size() == 0 || // Destination partition being empty indicates a write buffer
                                        // will be allocated, thus need to check if memory is full
           (totalInMemRowCount & (this.memoryCheckFrequency - 1)) == 0)) {  // check periodically
        if (isMemoryFull()) {
          if ((numPartitionsSpilled == hashPartitions.length - 1) ) {
            LOG.warn("This LAST partition in memory won't be spilled!");
            lastPartitionInMem = true;
          } else {
            if (nwayConf == null) { // binary join
              int biggest = biggestPartition();
              spillPartition(biggest);
              this.setSpill(true);
              if (partitionId == biggest) { // destination hash partition has just be spilled
                putToSidefile = true;
              }
            } else {                // n-way join
              LOG.info("N-way spilling: spill tail partition from previously loaded small tables");
              int biggest = nwayConf.getNextSpillPartition();
              memoryThreshold += nwayConf.spill();
              if (biggest != 0 && partitionId == biggest) { // destination hash partition has just be spilled
                putToSidefile = true;
              }
              LOG.info("Memory threshold has been increased to: " + memoryThreshold);
            }
            numPartitionsSpilled++;
          }
        }
      }
    }

    // Now we know where to put row
    if (putToSidefile) {
      KeyValueContainer kvContainer = hashPartition.getSidefileKVContainer();
      kvContainer.add((HiveKey) currentKey, (BytesWritable) currentValue);
    } else {
      hashPartition.hashMap.put(keyValueHelper, keyHash); // Pass along hashcode to avoid recalculation
      totalInMemRowCount++;
    }

    return null; // there's no key to return
  }

  /**
   * Check if the hash table of a specified partition is on disk (or "spilled" on creation)
   * @param partitionId partition number
   * @return true if on disk, false if in memory
   */
  public boolean isOnDisk(int partitionId) {
    return hashPartitions[partitionId].hashMapOnDisk;
  }

  /**
   * Check if the hash table of a specified partition has been "spilled" to disk when it was created.
   * In fact, in other words, check if a hashmap does exist or not.
   * @param partitionId hashMap ID
   * @return true if it was not created at all, false if there is a hash table existing there
   */
  public boolean isHashMapSpilledOnCreation(int partitionId) {
    return hashPartitions[partitionId].hashMapSpilledOnCreation;
  }

  /**
   * Check if the memory threshold is about to be reached.
   * Since all the write buffer will be lazily allocated in BytesBytesMultiHashMap, we need to
   * consider those as well.
   * We also need to count in the next 1024 rows to be loaded.
   * @return true if memory is full, false if not
   */
  private boolean isMemoryFull() {
    int numPartitionsInMem = 0;

    for (HashPartition hp : hashPartitions) {
      if (!hp.isHashMapOnDisk()) {
        numPartitionsInMem++;
      }
    }

    return refreshMemoryUsed() + this.memoryCheckFrequency * getTableRowSize() +
        writeBufferSize * numPartitionsInMem >= memoryThreshold;
  }

  /**
   * Find the partition with biggest hashtable in memory at this moment
   * @return the biggest partition number
   */
  private int biggestPartition() {
    int res = -1;
    int maxSize = 0;

    // If a partition has been spilled to disk, its size will be 0, i.e. it won't be picked
    for (int i = 0; i < hashPartitions.length; i++) {
      int size;
      if (isOnDisk(i)) {
        continue;
      } else {
        size = hashPartitions[i].hashMap.getNumValues();
      }
      if (size > maxSize) {
        maxSize = size;
        res = i;
      }
    }

    // It can happen that although there're some partitions in memory, but their sizes are all 0.
    // In that case we just pick one and spill.
    if (res == -1) {
      for (int i = 0; i < hashPartitions.length; i++) {
        if (!isOnDisk(i)) {
          return i;
        }
      }
    }

    return res;
  }

  /**
   * Move the hashtable of a specified partition from memory into local file system
   * @param partitionId the hashtable to be moved
   * @return amount of memory freed
   */
  public long spillPartition(int partitionId) throws IOException {
    HashPartition partition = hashPartitions[partitionId];
    int inMemRowCount = partition.hashMap.getNumValues();
    if (inMemRowCount == 0) {
      LOG.warn("Trying to spill an empty hash partition! It may be due to " +
          "hive.auto.convert.join.noconditionaltask.size being set too low.");
    }

    File file = FileUtils.createLocalDirsTempFile(
        spillLocalDirs, "partition-" + partitionId + "-", null, false);
    OutputStream outputStream = new FileOutputStream(file, false);

    com.esotericsoftware.kryo.io.Output output =
        new com.esotericsoftware.kryo.io.Output(outputStream);
    Kryo kryo = SerializationUtilities.borrowKryo();
    try {
      LOG.info("Trying to spill hash partition " + partitionId + " ...");
      kryo.writeObject(output, partition.hashMap);  // use Kryo to serialize hashmap
      output.close();
      outputStream.close();
    } finally {
      SerializationUtilities.releaseKryo(kryo);
    }

    partition.hashMapLocalPath = file.toPath();
    partition.hashMapOnDisk = true;

    LOG.info("Spilling hash partition " + partitionId + " (Rows: " + inMemRowCount +
        ", Mem size: " + partition.hashMap.memorySize() + "): " + file);
    LOG.info("Memory usage before spilling: " + memoryUsed);

    long memFreed = partition.hashMap.memorySize();
    memoryUsed -= memFreed;
    LOG.info("Memory usage after spilling: " + memoryUsed);

    partition.rowsOnDisk = inMemRowCount;
    totalInMemRowCount -= inMemRowCount;
    partition.hashMap.clear();
    partition.hashMap = null;
    return memFreed;
  }

  /**
   * Calculate how many partitions are needed.
   * For n-way join, we only do this calculation once in the HashTableLoader, for the biggest small
   * table. Other small tables will use the same number. They may need to adjust (usually reduce)
   * their individual write buffer size in order not to exceed memory threshold.
   * @param memoryThreshold memory threshold for the given table
   * @param dataSize total data size for the table
   * @param minNumParts minimum required number of partitions
   * @param minWbSize minimum required write buffer size
   * @return number of partitions needed
   */
  public static int calcNumPartitions(long memoryThreshold, long dataSize, int minNumParts,
      int minWbSize) throws IOException {
    int numPartitions = minNumParts;

    if (memoryThreshold < minNumParts * minWbSize) {
      LOG.warn("Available memory is not enough to create a HybridHashTableContainer!");
    }

    if (memoryThreshold / 2 < dataSize) { // The divided-by-2 logic is consistent to MapJoinOperator.reloadHashTable
      while (dataSize / numPartitions > memoryThreshold / 2) {
        numPartitions *= 2;
      }
    }

    LOG.info("Total available memory: " + memoryThreshold);
    LOG.info("Estimated small table size: " + dataSize);
    LOG.info("Number of hash partitions to be created: " + numPartitions);
    return numPartitions;
  }

  /* Get number of partitions */
  public int getNumPartitions() {
    return hashPartitions.length;
  }

  /* Get total number of rows from all in memory partitions */
  public int getTotalInMemRowCount() {
    return totalInMemRowCount;
  }

  /* Set total number of rows from all in memory partitions */
  public void setTotalInMemRowCount(int totalInMemRowCount) {
    this.totalInMemRowCount = totalInMemRowCount;
  }

  /* Get row size of small table */
  public long getTableRowSize() {
    return tableRowSize;
  }

  @Override
  public boolean hasSpill() {
    return isSpilled;
  }

  public void setSpill(boolean isSpilled) {
    this.isSpilled = isSpilled;
  }

  /**
   * Gets the partition Id into which to spill the big table row
   * @return partition Id
   */
  public int getToSpillPartitionId() {
    return toSpillPartitionId;
  }

  @Override
  public void clear() {
    for (int i = 0; i < hashPartitions.length; i++) {
      HashPartition hp = hashPartitions[i];
      if (hp != null) {
        LOG.info("Going to clear hash partition " + i);
        hp.clear();
      }
    }
    memoryUsed = 0;
  }

  @Override
  public MapJoinKey getAnyKey() {
    return null; // This table has no keys.
  }

  @Override
  public ReusableGetAdaptor createGetter(MapJoinKey keyTypeFromLoader) {
    if (keyTypeFromLoader != null) {
      throw new AssertionError("No key expected from loader but got " + keyTypeFromLoader);
    }
    return new GetAdaptor();
  }

  @Override
  public void seal() {
    for (HashPartition hp : hashPartitions) {
      // Only seal those partitions that haven't been spilled and cleared,
      // because once a hashMap is cleared, it will become unusable
      if (hp.hashMap != null && hp.hashMap.size() != 0) {
        hp.hashMap.seal();
      }
    }
  }


  // Direct access interfaces.

  @Override
  public void put(Writable currentKey, Writable currentValue) throws SerDeException, IOException {
    directWriteHelper.setKeyValue(currentKey, currentValue);
    internalPutRow(directWriteHelper, currentKey, currentValue);
  }

  /** Implementation of ReusableGetAdaptor that has Output for key serialization; row
   * container is also created once and reused for every row. */
  private class GetAdaptor implements ReusableGetAdaptor, ReusableGetAdaptorDirectAccess {

    private Object[] currentKey;
    private boolean[] nulls;
    private List<ObjectInspector> vectorKeyOIs;

    private final ReusableRowContainer currentValue;
    private final Output output;

    public GetAdaptor() {
      currentValue = new ReusableRowContainer();
      output = new Output();
    }

    @Override
    public JoinUtil.JoinResult setFromVector(VectorHashKeyWrapper kw,
        VectorExpressionWriter[] keyOutputWriters, VectorHashKeyWrapperBatch keyWrapperBatch)
        throws HiveException {
      if (nulls == null) {
        nulls = new boolean[keyOutputWriters.length];
        currentKey = new Object[keyOutputWriters.length];
        vectorKeyOIs = new ArrayList<ObjectInspector>();
        for (int i = 0; i < keyOutputWriters.length; i++) {
          vectorKeyOIs.add(keyOutputWriters[i].getObjectInspector());
        }
      } else {
        assert nulls.length == keyOutputWriters.length;
      }
      for (int i = 0; i < keyOutputWriters.length; i++) {
        currentKey[i] = keyWrapperBatch.getWritableKeyValue(kw, i, keyOutputWriters[i]);
        nulls[i] = currentKey[i] == null;
      }
      return currentValue.setFromOutput(
          MapJoinKey.serializeRow(output, currentKey, vectorKeyOIs,
                  sortableSortOrders, nullMarkers, notNullMarkers));
    }

    @Override
    public JoinUtil.JoinResult setFromRow(Object row, List<ExprNodeEvaluator> fields,
        List<ObjectInspector> ois) throws HiveException {
      if (nulls == null) {
        nulls = new boolean[fields.size()];
        currentKey = new Object[fields.size()];
      }
      for (int keyIndex = 0; keyIndex < fields.size(); ++keyIndex) {
        currentKey[keyIndex] = fields.get(keyIndex).evaluate(row);
        nulls[keyIndex] = currentKey[keyIndex] == null;
      }
      return currentValue.setFromOutput(
          MapJoinKey.serializeRow(output, currentKey, ois,
                  sortableSortOrders, nullMarkers, notNullMarkers));
    }

    @Override
    public JoinUtil.JoinResult setFromOther(ReusableGetAdaptor other) throws HiveException {
      assert other instanceof GetAdaptor;
      GetAdaptor other2 = (GetAdaptor)other;
      nulls = other2.nulls;
      currentKey = other2.currentKey;
      return currentValue.setFromOutput(other2.output);
    }

    @Override
    public boolean hasAnyNulls(int fieldCount, boolean[] nullsafes) {
      if (nulls == null || nulls.length == 0) return false;
      for (int i = 0; i < nulls.length; i++) {
        if (nulls[i] && (nullsafes == null || !nullsafes[i])) {
          return true;
        }
      }
      return false;
    }

    @Override
    public MapJoinRowContainer getCurrentRows() {
      return !currentValue.hasRows() ? null : currentValue;
    }

    @Override
    public Object[] getCurrentKey() {
      return currentKey;
    }

    // Direct access interfaces.

    @Override
    public JoinUtil.JoinResult setDirect(byte[] bytes, int offset, int length,
        BytesBytesMultiHashMap.Result hashMapResult) {
      return currentValue.setDirect(bytes, offset, length, hashMapResult);
    }

    @Override
    public int directSpillPartitionId() {
      return currentValue.directSpillPartitionId();
    }
  }

  /** Row container that gets and deserializes the rows on demand from bytes provided. */
  private class ReusableRowContainer
    implements MapJoinRowContainer, AbstractRowContainer.RowIterator<List<Object>> {
    private byte aliasFilter;
    private final BytesBytesMultiHashMap.Result hashMapResult;

    /**
     * Sometimes, when container is empty in multi-table mapjoin, we need to add a dummy row.
     * This container does not normally support adding rows; this is for the dummy row.
     */
    private List<Object> dummyRow = null;

    private final ByteArrayRef uselessIndirection; // LBStruct needs ByteArrayRef
    private final LazyBinaryStruct valueStruct;
    private final boolean needsComplexObjectFixup;
    private final ArrayList<Object> complexObjectArrayBuffer;

    private int partitionId; // Current hashMap in use

    public ReusableRowContainer() {
      if (internalValueOi != null) {
        valueStruct = (LazyBinaryStruct)
            LazyBinaryFactory.createLazyBinaryObject(internalValueOi);
        needsComplexObjectFixup = MapJoinBytesTableContainer.hasComplexObjects(internalValueOi);
        if (needsComplexObjectFixup) {
          complexObjectArrayBuffer =
              new ArrayList<Object>(
                  Collections.nCopies(internalValueOi.getAllStructFieldRefs().size(), null));
        } else {
          complexObjectArrayBuffer = null;
        }
      } else {
        valueStruct = null; // No rows?
        needsComplexObjectFixup =  false;
        complexObjectArrayBuffer = null;
      }
      uselessIndirection = new ByteArrayRef();
      hashMapResult = new BytesBytesMultiHashMap.Result();
      clearRows();
    }

    /* Determine if there is a match between big table row and the corresponding hashtable
     * Three states can be returned:
     * MATCH: a match is found
     * NOMATCH: no match is found from the specified partition
     * SPILL: the specified partition has been spilled to disk and is not available;
     *        the evaluation for this big table row will be postponed.
     */
    public JoinUtil.JoinResult setFromOutput(Output output) throws HiveException {
      int keyHash = HashCodeUtil.murmurHash(output.getData(), 0, output.getLength());

      if (bloom1 != null && !bloom1.testLong(keyHash)) {
        /*
         * if the keyHash is missing in the bloom filter, then the value cannot
         * exist in any of the spilled partition - return NOMATCH
         */
        dummyRow = null;
        aliasFilter = (byte) 0xff;
        hashMapResult.forget();
        return JoinResult.NOMATCH;
      }

      partitionId = keyHash & (hashPartitions.length - 1);

      // If the target hash table is on disk, spill this row to disk as well to be processed later
      if (isOnDisk(partitionId)) {
        toSpillPartitionId = partitionId;
        hashMapResult.forget();
        return JoinUtil.JoinResult.SPILL;
      }
      else {
        aliasFilter = hashPartitions[partitionId].hashMap.getValueResult(output.getData(), 0,
            output.getLength(), hashMapResult);
        dummyRow = null;
        if (hashMapResult.hasRows()) {
          return JoinUtil.JoinResult.MATCH;
        } else {
          aliasFilter = (byte) 0xff;
          return JoinUtil.JoinResult.NOMATCH;
        }
      }
    }

    @Override
    public boolean hasRows() {
      return hashMapResult.hasRows() || (dummyRow != null);
    }

    @Override
    public boolean isSingleRow() {
      if (!hashMapResult.hasRows()) {
        return (dummyRow != null);
      }
      return hashMapResult.isSingleRow();
    }

    // Implementation of row container
    @Override
    public AbstractRowContainer.RowIterator<List<Object>> rowIter() throws HiveException {
      return this;
    }

    @Override
    public int rowCount() throws HiveException {
      // For performance reasons we do not want to chase the values to the end to determine
      // the count.  Use hasRows and isSingleRow instead.
      throw new UnsupportedOperationException("Getting the row count not supported");
    }

    @Override
    public void clearRows() {
      // Doesn't clear underlying hashtable
      hashMapResult.forget();
      dummyRow = null;
      aliasFilter = (byte) 0xff;
    }

    @Override
    public byte getAliasFilter() throws HiveException {
      return aliasFilter;
    }

    @Override
    public MapJoinRowContainer copy() throws HiveException {
      return this; // Independent of hashtable and can be modified, no need to copy.
    }

    // Implementation of row iterator
    @Override
    public List<Object> first() throws HiveException {

      // A little strange that we forget the dummy row on read.
      if (dummyRow != null) {
        List<Object> result = dummyRow;
        dummyRow = null;
        return result;
      }

      WriteBuffers.ByteSegmentRef byteSegmentRef = hashMapResult.first();
      if (byteSegmentRef == null) {
        return null;
      } else {
        return unpack(byteSegmentRef);
      }

    }

    @Override
    public List<Object> next() throws HiveException {

      WriteBuffers.ByteSegmentRef byteSegmentRef = hashMapResult.next();
      if (byteSegmentRef == null) {
        return null;
      } else {
        return unpack(byteSegmentRef);
      }

    }

    private List<Object> unpack(WriteBuffers.ByteSegmentRef ref) throws HiveException {
      if (ref.getLength() == 0) {
        return EMPTY_LIST; // shortcut, 0 length means no fields
      }
      uselessIndirection.setData(ref.getBytes());
      valueStruct.init(uselessIndirection, (int)ref.getOffset(), ref.getLength());
      List<Object> result;
      if (!needsComplexObjectFixup) {
        // Good performance for common case where small table has no complex objects.
        result = valueStruct.getFieldsAsList();
      } else {
        // Convert the complex LazyBinary objects to standard (Java) objects so downstream
        // operators like FileSinkOperator can serialize complex objects in the form they expect
        // (i.e. Java objects).
        result = MapJoinBytesTableContainer.getComplexFieldsAsList(
            valueStruct, complexObjectArrayBuffer, internalValueOi);
      }
      return result;
    }

    @Override
    public void addRow(List<Object> t) {
      if (dummyRow != null || hashMapResult.hasRows()) {
        throw new RuntimeException("Cannot add rows when not empty");
      }
      dummyRow = t;
    }

    // Various unsupported methods.
    @Override
    public void addRow(Object[] value) {
      throw new RuntimeException(this.getClass().getCanonicalName() + " cannot add arrays");
    }
    @Override
    public void write(MapJoinObjectSerDeContext valueContext, ObjectOutputStream out) {
      throw new RuntimeException(this.getClass().getCanonicalName() + " cannot be serialized");
    }

    // Direct access.

    public JoinUtil.JoinResult setDirect(byte[] bytes, int offset, int length,
        BytesBytesMultiHashMap.Result hashMapResult) {

      int keyHash = HashCodeUtil.murmurHash(bytes, offset, length);
      partitionId = keyHash & (hashPartitions.length - 1);

      if (bloom1 != null && !bloom1.testLong(keyHash)) {
        /*
         * if the keyHash is missing in the bloom filter, then the value cannot exist in any of the
         * spilled partition - return NOMATCH
         */
        dummyRow = null;
        aliasFilter = (byte) 0xff;
        hashMapResult.forget();
        return JoinResult.NOMATCH;
      }

      // If the target hash table is on disk, spill this row to disk as well to be processed later
      if (isOnDisk(partitionId)) {
        return JoinUtil.JoinResult.SPILL;
      }
      else {
        aliasFilter = hashPartitions[partitionId].hashMap.getValueResult(bytes, offset, length,
            hashMapResult);
        dummyRow = null;
        if (hashMapResult.hasRows()) {
          return JoinUtil.JoinResult.MATCH;
        } else {
          aliasFilter = (byte) 0xff;
          return JoinUtil.JoinResult.NOMATCH;
        }
      }
    }

    public int directSpillPartitionId() {
      return partitionId;
    }
  }

  @Override
  public void dumpMetrics() {
    for (int i = 0; i < hashPartitions.length; i++) {
      HashPartition hp = hashPartitions[i];
      if (hp.hashMap != null) {
        hp.hashMap.debugDumpMetrics();
      }
    }
  }

  public void dumpStats() {
    int numPartitionsInMem = 0;
    int numPartitionsOnDisk = 0;

    for (HashPartition hp : hashPartitions) {
      if (hp.isHashMapOnDisk()) {
        numPartitionsOnDisk++;
      } else {
        numPartitionsInMem++;
      }
    }

    LOG.info("In memory partitions have been processed successfully: " +
        numPartitionsInMem + " partitions in memory have been processed; " +
        numPartitionsOnDisk + " partitions have been spilled to disk and will be processed next.");
  }

  @Override
  public int size() {
    int totalSize = 0;
    for (HashPartition hashPartition : hashPartitions) {
      totalSize += hashPartition.size();
    }
    return totalSize;
  }

  @Override
  public void setSerde(MapJoinObjectSerDeContext keyCtx, MapJoinObjectSerDeContext valCtx)
      throws SerDeException {
    AbstractSerDe keySerde = keyCtx.getSerDe(), valSerde = valCtx.getSerDe();

    if (writeHelper == null) {
      LOG.info("Initializing container with " + keySerde.getClass().getName() + " and "
          + valSerde.getClass().getName());

      // We assume this hashtable is loaded only when tez is enabled
      LazyBinaryStructObjectInspector valSoi =
          (LazyBinaryStructObjectInspector) valSerde.getObjectInspector();
      writeHelper = new MapJoinBytesTableContainer.LazyBinaryKvWriter(keySerde, valSoi,
          valCtx.hasFilterTag());
      if (internalValueOi == null) {
        internalValueOi = valSoi;
      }
      if (sortableSortOrders == null) {
        sortableSortOrders = ((BinarySortableSerDe) keySerde).getSortOrders();
      }
      if (nullMarkers == null) {
        nullMarkers = ((BinarySortableSerDe) keySerde).getNullMarkers();
      }
      if (notNullMarkers == null) {
        notNullMarkers = ((BinarySortableSerDe) keySerde).getNotNullMarkers();
      }
    }
  }
}
