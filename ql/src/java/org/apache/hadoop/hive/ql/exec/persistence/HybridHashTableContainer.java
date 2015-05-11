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


import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinBytesTableContainer.KeyValueHelper;
import org.apache.hadoop.hive.ql.exec.vector.VectorHashKeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinRowBytesContainer;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.SerDe;
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
  private static final Log LOG = LogFactory.getLog(HybridHashTableContainer.class);

  private final HashPartition[] hashPartitions; // an array of partitions holding the triplets
  private int totalInMemRowCount = 0;           // total number of small table rows in memory
  private long memoryThreshold;                 // the max memory limit that can be allocated
  private long memoryUsed;                      // the actual memory used
  private int writeBufferSize;                  // write buffer size for this HybridHashTableContainer
  private final long tableRowSize;              // row size of the small table
  private boolean isSpilled;                    // whether there's any spilled partition
  private int toSpillPartitionId;               // the partition into which to spill the big table row;
                                                // This may change after every setMapJoinKey call
  private int numPartitionsSpilled;             // number of spilled partitions
  private boolean lastPartitionInMem;           // only one (last one) partition is left in memory
  private final int memoryCheckFrequency;       // how often (# of rows apart) to check if memory is full
  private HybridHashTableConf nwayConf;         // configuration for n-way join

  /** The OI used to deserialize values. We never deserialize keys. */
  private LazyBinaryStructObjectInspector internalValueOi;
  private boolean[] sortableSortOrders;
  private MapJoinBytesTableContainer.KeyValueHelper writeHelper;
  private MapJoinBytesTableContainer.DirectKeyValueWriter directWriteHelper;

  private final List<Object> EMPTY_LIST = new ArrayList<Object>(0);

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
    int threshold;                          // Used to create an empty BytesBytesMultiHashMap
    float loadFactor;                       // Same as above
    int wbSize;                             // Same as above

    /* It may happen that there's not enough memory to instantiate a hashmap for the partition.
     * In that case, we don't create the hashmap, but pretend the hashmap is directly "spilled".
     */
    public HashPartition(int threshold, float loadFactor, int wbSize, long memUsage,
                         boolean createHashMap) {
      if (createHashMap) {
        hashMap = new BytesBytesMultiHashMap(threshold, loadFactor, wbSize, memUsage);
      } else {
        hashMapSpilledOnCreation = true;
        hashMapOnDisk = true;
      }
      this.threshold = threshold;
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
    public BytesBytesMultiHashMap getHashMapFromDisk(int initialCapacity)
        throws IOException, ClassNotFoundException {
      if (hashMapSpilledOnCreation) {
        return new BytesBytesMultiHashMap(Math.max(threshold, initialCapacity) , loadFactor, wbSize, -1);
      } else {
        InputStream inputStream = Files.newInputStream(hashMapLocalPath);
        com.esotericsoftware.kryo.io.Input input = new com.esotericsoftware.kryo.io.Input(inputStream);
        Kryo kryo = Utilities.runtimeSerializationKryo.get();
        BytesBytesMultiHashMap restoredHashMap = kryo.readObject(input, BytesBytesMultiHashMap.class);

        if (initialCapacity > 0) {
          restoredHashMap.expandAndRehashToTarget(initialCapacity);
        }

        input.close();
        inputStream.close();
        Files.delete(hashMapLocalPath);
        return restoredHashMap;
      }
    }

    /* Get the small table key/value container */
    public KeyValueContainer getSidefileKVContainer() {
      if (sidefileKVContainer == null) {
        sidefileKVContainer = new KeyValueContainer();
      }
      return sidefileKVContainer;
    }

    /* Get the big table row container */
    public ObjectContainer getMatchfileObjContainer() {
      if (matchfileObjContainer == null) {
        matchfileObjContainer = new ObjectContainer();
      }
      return matchfileObjContainer;
    }

    /* Get the big table row bytes container for native vector map join */
    public VectorMapJoinRowBytesContainer getMatchfileRowBytesContainer() {
      if (matchfileRowBytesContainer == null) {
        matchfileRowBytesContainer = new VectorMapJoinRowBytesContainer();
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
  }

  public HybridHashTableContainer(Configuration hconf, long keyCount, long memoryAvailable,
                                  long estimatedTableSize, HybridHashTableConf nwayConf)
      throws SerDeException, IOException {
    this(HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVEHASHTABLEKEYCOUNTADJUSTMENT),
         HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLETHRESHOLD),
         HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVEHASHTABLELOADFACTOR),
         HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHYBRIDGRACEHASHJOINMEMCHECKFREQ),
         HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHYBRIDGRACEHASHJOINMINWBSIZE),
         HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHYBRIDGRACEHASHJOINMINNUMPARTITIONS),
         estimatedTableSize, keyCount, memoryAvailable, nwayConf);
  }

  private HybridHashTableContainer(float keyCountAdj, int threshold, float loadFactor,
                                   int memCheckFreq, int minWbSize, int minNumParts,
                                   long estimatedTableSize, long keyCount,
                                   long memoryAvailable, HybridHashTableConf nwayConf)
      throws SerDeException, IOException {
    directWriteHelper = new MapJoinBytesTableContainer.DirectKeyValueWriter();

    int newKeyCount = HashMapWrapper.calculateTableSize(
        keyCountAdj, threshold, loadFactor, keyCount);

    memoryThreshold = memoryAvailable;
    tableRowSize = estimatedTableSize / (keyCount != 0 ? keyCount : 1);
    memoryCheckFrequency = memCheckFreq;

    this.nwayConf = nwayConf;
    int numPartitions;
    if (nwayConf == null) { // binary join
      numPartitions = calcNumPartitions(memoryThreshold, estimatedTableSize, minNumParts, minWbSize,
          nwayConf);
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
            LOG.info("Total available memory is: " + memoryThreshold);
          }
        }
        writeBufferSize = (int)(memoryThreshold / numPartitions);
      }
    }
    writeBufferSize = writeBufferSize < minWbSize ? minWbSize : writeBufferSize;
    LOG.info("Write buffer size: " + writeBufferSize);
    hashPartitions = new HashPartition[numPartitions];
    int numPartitionsSpilledOnCreation = 0;
    memoryUsed = 0;
    int initialCapacity = Math.max(newKeyCount / numPartitions, threshold / numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      if (this.nwayConf == null ||                          // binary join
          nwayConf.getLoadedContainerList().size() == 0) {  // n-way join, first (biggest) small table
        if (i == 0) { // We unconditionally create a hashmap for the first hash partition
          hashPartitions[i] = new HashPartition(initialCapacity, loadFactor, writeBufferSize, memoryThreshold, true);
        } else {
          hashPartitions[i] = new HashPartition(initialCapacity, loadFactor, writeBufferSize, memoryThreshold,
              memoryUsed + writeBufferSize < memoryThreshold);
        }
      } else {                      // n-way join
        // For all later small tables, follow the same pattern of the previously loaded tables.
        if (this.nwayConf.doSpillOnCreation(i)) {
          hashPartitions[i] = new HashPartition(threshold, loadFactor, writeBufferSize, memoryThreshold, false);
        } else {
          hashPartitions[i] = new HashPartition(threshold, loadFactor, writeBufferSize, memoryThreshold, true);
        }
      }

      if (isHashMapSpilledOnCreation(i)) {
        numPartitionsSpilledOnCreation++;
        numPartitionsSpilled++;
        this.setSpill(true);
        if (this.nwayConf != null && this.nwayConf.getNextSpillPartition() == numPartitions - 1) {
          this.nwayConf.setNextSpillPartition(i - 1);
        }
      } else {
        memoryUsed += hashPartitions[i].hashMap.memorySize();
      }
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
  public long refreshMemoryUsed() {
    long memUsed = 0;
    for (HashPartition hp : hashPartitions) {
      if (hp.hashMap != null) {
        memUsed += hp.hashMap.memorySize();
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

  /* For a given row, put it into proper partition based on its hash value.
   * When memory threshold is reached, the biggest hash table in memory will be spilled to disk.
   * If the hash table of a specific partition is already on disk, all later rows will be put into
   * a row container for later use.
   */
  @SuppressWarnings("deprecation")
  @Override
  public MapJoinKey putRow(MapJoinObjectSerDeContext keyContext, Writable currentKey,
      MapJoinObjectSerDeContext valueContext, Writable currentValue)
      throws SerDeException, HiveException, IOException {
    SerDe keySerde = keyContext.getSerDe(), valSerde = valueContext.getSerDe();

    if (writeHelper == null) {
      LOG.info("Initializing container with "
          + keySerde.getClass().getName() + " and " + valSerde.getClass().getName());

      // We assume this hashtable is loaded only when tez is enabled
      LazyBinaryStructObjectInspector valSoi =
          (LazyBinaryStructObjectInspector) valSerde.getObjectInspector();
      writeHelper = new MapJoinBytesTableContainer.LazyBinaryKvWriter(keySerde, valSoi,
                                                                      valueContext.hasFilterTag());
      if (internalValueOi == null) {
        internalValueOi = valSoi;
      }
      if (sortableSortOrders == null) {
        sortableSortOrders = ((BinarySortableSerDe) keySerde).getSortOrders();
      }
    }
    writeHelper.setKeyValue(currentKey, currentValue);
    return internalPutRow(writeHelper, currentKey, currentValue);
  }

  private MapJoinKey internalPutRow(KeyValueHelper keyValueHelper,
          Writable currentKey, Writable currentValue) throws SerDeException, IOException {

    // Next, put row into corresponding hash partition
    int keyHash = keyValueHelper.getHashFromKey();
    int partitionId = keyHash & (hashPartitions.length - 1);
    HashPartition hashPartition = hashPartitions[partitionId];

    if (isOnDisk(partitionId) || isHashMapSpilledOnCreation(partitionId)) {
      KeyValueContainer kvContainer = hashPartition.getSidefileKVContainer();
      kvContainer.add((HiveKey) currentKey, (BytesWritable) currentValue);
    } else {
      hashPartition.hashMap.put(keyValueHelper, keyHash); // Pass along hashcode to avoid recalculation
      totalInMemRowCount++;

      if ((totalInMemRowCount & (this.memoryCheckFrequency - 1)) == 0 &&  // check periodically
          !lastPartitionInMem) { // If this is the only partition in memory, proceed without check
        if (isMemoryFull()) {
          if ((numPartitionsSpilled == hashPartitions.length - 1) ) {
            LOG.warn("This LAST partition in memory won't be spilled!");
            lastPartitionInMem = true;
          } else {
            if (nwayConf == null) { // binary join
              int biggest = biggestPartition();
              spillPartition(biggest);
              this.setSpill(true);
            } else {                // n-way join
              LOG.info("N-way spilling: spill tail partition from previously loaded small tables");
              memoryThreshold += nwayConf.spill();
              LOG.info("Memory threshold has been increased to: " + memoryThreshold);
            }
            numPartitionsSpilled++;
          }
        }
      }
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
   * Check if the memory threshold is reached
   * @return true if memory is full, false if not
   */
  private boolean isMemoryFull() {
    return refreshMemoryUsed() >= memoryThreshold;
  }

  /**
   * Find the partition with biggest hashtable in memory at this moment
   * @return the biggest partition number
   */
  private int biggestPartition() {
    int res = 0;
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

    Path path = Files.createTempFile("partition-" + partitionId + "-", null);
    OutputStream outputStream = Files.newOutputStream(path);

    com.esotericsoftware.kryo.io.Output output = new com.esotericsoftware.kryo.io.Output(outputStream);
    Kryo kryo = Utilities.runtimeSerializationKryo.get();
    kryo.writeObject(output, partition.hashMap);  // use Kryo to serialize hashmap
    output.close();
    outputStream.close();

    partition.hashMapLocalPath = path;
    partition.hashMapOnDisk = true;

    LOG.info("Spilling hash partition " + partitionId + " (Rows: " + inMemRowCount +
        ", Mem size: " + partition.hashMap.memorySize() + "): " + path);
    LOG.info("Memory usage before spilling: " + memoryUsed);

    long memFreed = partition.hashMap.memorySize();
    memoryUsed -= memFreed;
    LOG.info("Memory usage after spilling: " + memoryUsed);

    totalInMemRowCount -= inMemRowCount;
    partition.hashMap.clear();
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
   * @param nwayConf the n-way join configuration
   * @return number of partitions needed
   */
  public static int calcNumPartitions(long memoryThreshold, long dataSize, int minNumParts,
      int minWbSize, HybridHashTableConf nwayConf) throws IOException {
    int numPartitions = minNumParts;

    if (memoryThreshold < minNumParts * minWbSize) {
      LOG.warn("Available memory is not enough to create a HybridHashTableContainer!");
    }
    if (memoryThreshold < dataSize) {
      while (dataSize / numPartitions > memoryThreshold) {
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
    for (HashPartition hp : hashPartitions) {
      if (hp != null) {
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
          MapJoinKey.serializeRow(output, currentKey, vectorKeyOIs, sortableSortOrders));
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
          MapJoinKey.serializeRow(output, currentKey, ois, sortableSortOrders));
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
    private BytesBytesMultiHashMap.Result hashMapResult;

    /**
     * Sometimes, when container is empty in multi-table mapjoin, we need to add a dummy row.
     * This container does not normally support adding rows; this is for the dummy row.
     */
    private List<Object> dummyRow = null;

    private final ByteArrayRef uselessIndirection; // LBStruct needs ByteArrayRef
    private final LazyBinaryStruct valueStruct;

    private int partitionId; // Current hashMap in use

    public ReusableRowContainer() {
      if (internalValueOi != null) {
        valueStruct = (LazyBinaryStruct)
            LazyBinaryFactory.createLazyBinaryObject(internalValueOi);
      } else {
        valueStruct = null; // No rows?
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
      int keyHash = WriteBuffers.murmurHash(output.getData(), 0, output.getLength());
      partitionId = keyHash & (hashPartitions.length - 1);

      // If the target hash table is on disk, spill this row to disk as well to be processed later
      if (isOnDisk(partitionId)) {
        toSpillPartitionId = partitionId;
        hashMapResult.forget();
        return JoinUtil.JoinResult.SPILL;
      }
      else {
        aliasFilter = hashPartitions[partitionId].hashMap.getValueResult(output.getData(), 0, output.getLength(), hashMapResult);
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
        return uppack(byteSegmentRef);
      }

    }

    @Override
    public List<Object> next() throws HiveException {

      WriteBuffers.ByteSegmentRef byteSegmentRef = hashMapResult.next();
      if (byteSegmentRef == null) {
        return null;
      } else {
        return uppack(byteSegmentRef);
      }

    }

    private List<Object> uppack(WriteBuffers.ByteSegmentRef ref) throws HiveException {
      if (ref.getLength() == 0) {
        return EMPTY_LIST; // shortcut, 0 length means no fields
      }
      uselessIndirection.setData(ref.getBytes());
      valueStruct.init(uselessIndirection, (int)ref.getOffset(), ref.getLength());
      return valueStruct.getFieldsAsList(); // TODO: should we unset bytes after that?
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

      int keyHash = WriteBuffers.murmurHash(bytes, offset, length);
      partitionId = keyHash & (hashPartitions.length - 1);

      // If the target hash table is on disk, spill this row to disk as well to be processed later
      if (isOnDisk(partitionId)) {
        return JoinUtil.JoinResult.SPILL;
      }
      else {
        aliasFilter = hashPartitions[partitionId].hashMap.getValueResult(bytes, offset, length, hashMapResult);
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
}
