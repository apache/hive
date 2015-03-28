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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.HashTableLoaderFactory;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionHandler;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinRowContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer.ReusableGetAdaptor;
import org.apache.hadoop.hive.ql.exec.persistence.UnwrapRowContainer;
import org.apache.hadoop.hive.ql.exec.persistence.BytesBytesMultiHashMap;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinBytesTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.HybridHashTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.KeyValueContainer;
import org.apache.hadoop.hive.ql.exec.persistence.ObjectContainer;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import static org.apache.hadoop.hive.ql.exec.persistence.HybridHashTableContainer.HashPartition;
import static org.apache.hadoop.hive.ql.exec.persistence.MapJoinBytesTableContainer.KeyValueHelper;

/**
 * Map side Join operator implementation.
 */
public class MapJoinOperator extends AbstractMapJoinOperator<MapJoinDesc> implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(MapJoinOperator.class.getName());
  private static final String CLASS_NAME = MapJoinOperator.class.getName();
  private final PerfLogger perfLogger = PerfLogger.getPerfLogger();

  private transient String cacheKey;
  private transient ObjectCache cache;

  protected HashTableLoader loader;

  protected transient MapJoinTableContainer[] mapJoinTables;
  private transient MapJoinTableContainerSerDe[] mapJoinTableSerdes;
  private transient boolean hashTblInitedOnce;
  private transient ReusableGetAdaptor[] hashMapRowGetters;

  private UnwrapRowContainer[] unwrapContainer;
  private transient Configuration hconf;
  private transient boolean useHybridGraceHashJoin; // whether Hybrid Grace Hash Join is enabled
  private transient boolean hybridMapJoinLeftover;  // whether there's spilled data to be processed
  private transient MapJoinBytesTableContainer currentSmallTable; // reloaded hashmap from disk
  private transient int tag;        // big table alias
  private transient int smallTable; // small table alias

  public MapJoinOperator() {
  }

  public MapJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mjop) {
    super(mjop);
  }

  /*
   * We need the base (operator.java) implementation of start/endGroup.
   * The parent class has functionality in those that map join can't use.
   * Note: The mapjoin can be run in the reducer only on Tez.
   */
  @Override
  public void endGroup() throws HiveException {
    defaultEndGroup();
  }

  @Override
  public void startGroup() throws HiveException {
    defaultStartGroup();
  }

  @Override
  protected Collection<Future<?>> initializeOp(Configuration hconf) throws HiveException {
    this.hconf = hconf;
    unwrapContainer = new UnwrapRowContainer[conf.getTagLength()];

    Collection<Future<?>> result = super.initializeOp(hconf);
    if (result == null) {
      result = new HashSet<Future<?>>();
    }

    int tagLen = conf.getTagLength();

    // On Tez only: The hash map might already be cached in the container we run
    // the task in. On MR: The cache is a no-op.
    cacheKey = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVEQUERYID)
      + "__HASH_MAP_"+this.getOperatorId()+"_container";

    cache = ObjectCacheFactory.getCache(hconf);
    loader = HashTableLoaderFactory.getLoader(hconf);

    hashMapRowGetters = null;

    mapJoinTables = new MapJoinTableContainer[tagLen];
    mapJoinTableSerdes = new MapJoinTableContainerSerDe[tagLen];
    hashTblInitedOnce = false;
    useHybridGraceHashJoin =
        HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVEUSEHYBRIDGRACEHASHJOIN);

    generateMapMetaData();

    final ExecMapperContext mapContext = getExecContext();
    final MapredContext mrContext = MapredContext.get();

    if (!conf.isBucketMapJoin() && !useHybridGraceHashJoin) {
      /*
       * The issue with caching in case of bucket map join is that different tasks
       * process different buckets and if the container is reused to join a different bucket,
       * join results can be incorrect. The cache is keyed on operator id and for bucket map join
       * the operator does not change but data needed is different. For a proper fix, this
       * requires changes in the Tez API with regard to finding bucket id and
       * also ability to schedule tasks to re-use containers that have cached the specific bucket.
       */
      if (isLogInfoEnabled) {
        LOG.info("This is not bucket map join, so cache");
      }

      Future<Pair<MapJoinTableContainer[], MapJoinTableContainerSerDe[]>> future =
          cache.retrieveAsync(
              cacheKey,
              new Callable<Pair<MapJoinTableContainer[], MapJoinTableContainerSerDe[]>>() {
                @Override
                public Pair<MapJoinTableContainer[], MapJoinTableContainerSerDe[]> call()
                    throws HiveException {
                  return loadHashTable(mapContext, mrContext);
                }
              });
      result.add(future);
    } else if (mapContext == null || mapContext.getLocalWork() == null
        || mapContext.getLocalWork().getInputFileChangeSensitive() == false) {
      loadHashTable(mapContext, mrContext);
      hashTblInitedOnce = true;
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected final void completeInitializationOp(Object[] os) throws HiveException {
    if (os.length != 0) {
      Pair<MapJoinTableContainer[], MapJoinTableContainerSerDe[]> pair =
          (Pair<MapJoinTableContainer[], MapJoinTableContainerSerDe[]>) os[0];
      mapJoinTables = pair.getLeft();
      mapJoinTableSerdes = pair.getRight();
      hashTblInitedOnce = true;
    }

    alias = (byte) conf.getPosBigTable();
    if (hashMapRowGetters == null) {
      hashMapRowGetters = new ReusableGetAdaptor[mapJoinTables.length];
      MapJoinKey refKey = getRefKey(alias);
      for (byte pos = 0; pos < order.length; pos++) {
        if (pos != alias) {
          hashMapRowGetters[pos] = mapJoinTables[pos].createGetter(refKey);
        }
      }
    }

    if (this.getExecContext() != null) {
      // reset exec context so that initialization of the map operator happens
      // properly
      this.getExecContext().setLastInputPath(null);
      this.getExecContext().setCurrentInputPath(null);
    }
  }

  @Override
  protected List<ObjectInspector> getValueObjectInspectors(
      byte alias, List<ObjectInspector>[] aliasToObjectInspectors) {
    int[] valueIndex = conf.getValueIndex(alias);
    if (valueIndex == null) {
      return super.getValueObjectInspectors(alias, aliasToObjectInspectors);
    }
    unwrapContainer[alias] = new UnwrapRowContainer(alias, valueIndex, hasFilter(alias));

    List<ObjectInspector> inspectors = aliasToObjectInspectors[alias];

    int bigPos = conf.getPosBigTable();
    List<ObjectInspector> valueOI = new ArrayList<ObjectInspector>();
    for (int i = 0; i < valueIndex.length; i++) {
      if (valueIndex[i] >= 0 && !joinKeysObjectInspectors[bigPos].isEmpty()) {
        valueOI.add(joinKeysObjectInspectors[bigPos].get(valueIndex[i]));
      } else {
        valueOI.add(inspectors.get(i));
      }
    }
    return valueOI;
  }

  public void generateMapMetaData() throws HiveException {
    // generate the meta data for key
    // index for key is -1

    try {
      TableDesc keyTableDesc = conf.getKeyTblDesc();
      SerDe keySerializer = (SerDe) ReflectionUtils.newInstance(keyTableDesc.getDeserializerClass(),
 null);
      SerDeUtils.initializeSerDe(keySerializer, null, keyTableDesc.getProperties(), null);
      MapJoinObjectSerDeContext keyContext = new MapJoinObjectSerDeContext(keySerializer, false);
      for (int pos = 0; pos < order.length; pos++) {
        if (pos == posBigTable) {
          continue;
        }
        TableDesc valueTableDesc;
        if (conf.getNoOuterJoin()) {
          valueTableDesc = conf.getValueTblDescs().get(pos);
        } else {
          valueTableDesc = conf.getValueFilteredTblDescs().get(pos);
        }
        SerDe valueSerDe =
            (SerDe) ReflectionUtils.newInstance(valueTableDesc.getDeserializerClass(), null);
        SerDeUtils.initializeSerDe(valueSerDe, null, valueTableDesc.getProperties(), null);
        MapJoinObjectSerDeContext valueContext =
            new MapJoinObjectSerDeContext(valueSerDe, hasFilter(pos));
        mapJoinTableSerdes[pos] = new MapJoinTableContainerSerDe(keyContext, valueContext);
      }
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }

  private Pair<MapJoinTableContainer[], MapJoinTableContainerSerDe[]> loadHashTable(
      ExecMapperContext mapContext, MapredContext mrContext) throws HiveException {

    if (this.hashTblInitedOnce
        && ((mapContext == null) || (mapContext.getLocalWork() == null) || (mapContext
            .getLocalWork().getInputFileChangeSensitive() == false))) {
      // no need to reload
      return new ImmutablePair<MapJoinTableContainer[], MapJoinTableContainerSerDe[]>(
          mapJoinTables, mapJoinTableSerdes);
    }

    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.LOAD_HASHTABLE);
    loader.init(mapContext, mrContext, hconf, this);
    long memUsage = (long)(MapJoinMemoryExhaustionHandler.getMaxHeapSize()
        * conf.getHashTableMemoryUsage());
    loader.load(mapJoinTables, mapJoinTableSerdes, memUsage);

    hashTblInitedOnce = true;

    Pair<MapJoinTableContainer[], MapJoinTableContainerSerDe[]> pair
      = new ImmutablePair<MapJoinTableContainer[],
      MapJoinTableContainerSerDe[]> (mapJoinTables, mapJoinTableSerdes);

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.LOAD_HASHTABLE);

    return pair;
  }

  // Load the hash table
  @Override
  public void cleanUpInputFileChangedOp() throws HiveException {
    loadHashTable(getExecContext(), MapredContext.get());
  }

  protected JoinUtil.JoinResult setMapJoinKey(
      ReusableGetAdaptor dest, Object row, byte alias) throws HiveException {
    return dest.setFromRow(row, joinKeys[alias], joinKeysObjectInspectors[alias]);
  }

  protected MapJoinKey getRefKey(byte alias) {
    // We assume that since we are joining on the same key, all tables would have either
    // optimized or non-optimized key; hence, we can pass any key in any table as reference.
    // We do it so that MJKB could determine whether it can use optimized keys.
    for (byte pos = 0; pos < order.length; pos++) {
      if (pos == alias) continue;
      MapJoinKey refKey = mapJoinTables[pos].getAnyKey();
      if (refKey != null) return refKey;
    }
    return null; // All join tables have 0 keys, doesn't matter what we generate.
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    this.tag = tag;

    // As we're calling processOp again to process the leftover triplets, we know the "row" is
    // coming from the on-disk matchfile. We need to recreate hashMapRowGetter against new hashtable
    if (hybridMapJoinLeftover) {
      assert hashMapRowGetters != null;
      if (hashMapRowGetters[smallTable] == null) {
        MapJoinKey refKey = getRefKey((byte) tag);
        hashMapRowGetters[smallTable] = currentSmallTable.createGetter(refKey);
      }
    }

    try {
      // compute keys and values as StandardObjects
      ReusableGetAdaptor firstSetKey = null;
      int fieldCount = joinKeys[alias].size();
      boolean joinNeeded = false;
      for (byte pos = 0; pos < order.length; pos++) {
        if (pos != alias) {
          smallTable = pos; // record small table alias
          JoinUtil.JoinResult joinResult;
          ReusableGetAdaptor adaptor;
          if (firstSetKey == null) {
            adaptor = firstSetKey = hashMapRowGetters[pos];
            joinResult = setMapJoinKey(firstSetKey, row, alias);
          } else {
            // Keys for all tables are the same, so only the first has to deserialize them.
            adaptor = hashMapRowGetters[pos];
            joinResult = adaptor.setFromOther(firstSetKey);
          }
          MapJoinRowContainer rowContainer = adaptor.getCurrentRows();
          if (rowContainer != null && unwrapContainer[pos] != null) {
            Object[] currentKey = firstSetKey.getCurrentKey();
            rowContainer = unwrapContainer[pos].setInternal(rowContainer, currentKey);
          }
          // there is no join-value or join-key has all null elements
          if (rowContainer == null || firstSetKey.hasAnyNulls(fieldCount, nullsafes)) {
            if (!noOuterJoin) {
              // For Hybrid Grace Hash Join, during the 1st round processing,
              // we only keep the LEFT side if the row is not spilled
              if (!useHybridGraceHashJoin || hybridMapJoinLeftover ||
                  (!hybridMapJoinLeftover && joinResult != JoinUtil.JoinResult.SPILL)) {
                joinNeeded = true;
                storage[pos] = dummyObjVectors[pos];
              }
            } else {
              storage[pos] = emptyList;
            }
          } else {
            joinNeeded = true;
            storage[pos] = rowContainer.copy();
            aliasFilterTags[pos] = rowContainer.getAliasFilter();
          }
          // Spill the big table rows into appropriate partition
          if (joinResult == JoinUtil.JoinResult.SPILL) {
            spillBigTableRow(mapJoinTables[pos], row);
          }
        }
      }
      if (joinNeeded) {
        List<Object> value = getFilteredValue(alias, row);
        // Add the value to the ArrayList
        storage[alias].addRow(value);
        // generate the output records
        checkAndGenObject();
      }
      // done with the row
      storage[tag].clearRows();
      for (byte pos = 0; pos < order.length; pos++) {
        if (pos != tag) {
          storage[pos] = null;
        }
      }
    } catch (Exception e) {
      String msg = "Unexpected exception: " + e.getMessage();
      LOG.error(msg, e);
      throw new HiveException(msg, e);
    }
  }

  /**
   * Postpone processing the big table row temporarily by spilling it to a row container
   * @param hybridHtContainer Hybrid hashtable container
   * @param row big table row
   */
  protected void spillBigTableRow(MapJoinTableContainer hybridHtContainer, Object row) throws HiveException {
    HybridHashTableContainer ht = (HybridHashTableContainer) hybridHtContainer;
    int partitionId = ht.getToSpillPartitionId();
    HashPartition hp = ht.getHashPartitions()[partitionId];
    ObjectContainer bigTable = hp.getMatchfileObjContainer();
    bigTable.add(row);
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    for (MapJoinTableContainer tableContainer : mapJoinTables) {
      if (tableContainer != null) {
        tableContainer.dumpMetrics();

        if (tableContainer instanceof HybridHashTableContainer) {
          HybridHashTableContainer hybridHtContainer = (HybridHashTableContainer) tableContainer;
          hybridHtContainer.dumpStats();

          HashPartition[] hashPartitions = hybridHtContainer.getHashPartitions();
          // Clear all in memory partitions first
          for (int i = 0; i < hashPartitions.length; i++) {
            if (!hashPartitions[i].isHashMapOnDisk()) {
              hybridHtContainer.setTotalInMemRowCount(
                  hybridHtContainer.getTotalInMemRowCount() -
                      hashPartitions[i].getHashMapFromMemory().getNumValues());
              hashPartitions[i].getHashMapFromMemory().clear();
            }
          }
          assert hybridHtContainer.getTotalInMemRowCount() == 0;

          for (int i = 0; i < hashPartitions.length; i++) {
            if (hashPartitions[i].isHashMapOnDisk()) {
              // Recursively process on-disk triplets (hash partition, sidefile, matchfile)
              try {
                hybridMapJoinLeftover = true;
                hashMapRowGetters[smallTable] = null;
                continueProcess(hashPartitions[i], hybridHtContainer);
              } catch (IOException e) {
                e.printStackTrace();
              } catch (ClassNotFoundException e) {
                e.printStackTrace();
              } catch (SerDeException e) {
                e.printStackTrace();
              }
            }
            hybridMapJoinLeftover = false;
            currentSmallTable = null;
          }
        }
      }
    }
    if ((this.getExecContext() != null) && (this.getExecContext().getLocalWork() != null)
        && (this.getExecContext().getLocalWork().getInputFileChangeSensitive())
        && mapJoinTables != null) {
      for (MapJoinTableContainer tableContainer : mapJoinTables) {
        if (tableContainer != null) {
          tableContainer.clear();
        }
      }
    }
    cache.release(cacheKey);
    super.closeOp(abort);
  }

  /**
   * Continue processing each pair of spilled hashtable and big table row container
   * @param partition hash partition to process
   * @param hybridHtContainer Hybrid hashtable container
   * @throws HiveException
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws SerDeException
   */
  private void continueProcess(HashPartition partition, HybridHashTableContainer hybridHtContainer)
      throws HiveException, IOException, ClassNotFoundException, SerDeException {
    reloadHashTable(partition, hybridHtContainer);
    reProcessBigTable(partition);
  }

  /**
   * Reload hashtable from the hash partition.
   * It can have two steps:
   * 1) Deserialize a serialized hash table, and
   * 2) Merge every key/value pair from small table container into the hash table
   * @param partition hash partition to process
   * @param hybridHtContainer Hybrid hashtable container
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws HiveException
   * @throws SerDeException
   */
  private void reloadHashTable(HashPartition partition,
                               HybridHashTableContainer hybridHtContainer)
      throws IOException, ClassNotFoundException, HiveException, SerDeException {
    // Deserialize the on-disk hash table
    // We're sure this part is smaller than memory limit
    BytesBytesMultiHashMap restoredHashMap = partition.getHashMapFromDisk();
    int rowCount = restoredHashMap.getNumValues();
    LOG.info("Hybrid Grace Hash Join: Deserializing spilled hash partition...");
    LOG.info("Hybrid Grace Hash Join: Number of rows restored from hashmap: " + rowCount);

    // Merge the sidefile into the newly created hash table
    // This is where the spilling may happen again
    KeyValueContainer kvContainer = partition.getSidefileKVContainer();
    rowCount += kvContainer.size();
    LOG.info("Hybrid Grace Hash Join: Number of rows restored from KeyValueContainer: " +
        kvContainer.size());

    // If based on the new key count, keyCount is smaller than a threshold,
    // then just load the entire restored hashmap into memory.
    // The size of deserialized partition shouldn't exceed half of memory limit
    if (rowCount * hybridHtContainer.getTableRowSize() >= hybridHtContainer.getMemoryThreshold() / 2) {
      throw new RuntimeException("Hybrid Grace Hash Join: Hash table cannot be reloaded since it" +
          " will be greater than memory limit. Recursive spilling is currently not supported");
    }

    KeyValueHelper writeHelper = hybridHtContainer.getWriteHelper();
    while (kvContainer.hasNext()) {
      ObjectPair<HiveKey, BytesWritable> pair = kvContainer.next();
      Writable key = pair.getFirst();
      Writable val = pair.getSecond();
      writeHelper.setKeyValue(key, val);
      restoredHashMap.put(writeHelper, -1);
    }

    hybridHtContainer.setTotalInMemRowCount(hybridHtContainer.getTotalInMemRowCount()
        + restoredHashMap.getNumValues() + kvContainer.size());
    kvContainer.clear();

    // Since there's only one hashmap to deal with, it's OK to create a MapJoinBytesTableContainer
    currentSmallTable = new MapJoinBytesTableContainer(restoredHashMap);
    currentSmallTable.setInternalValueOi(hybridHtContainer.getInternalValueOi());
    currentSmallTable.setSortableSortOrders(hybridHtContainer.getSortableSortOrders());
  }

  /**
   * Iterate over the big table row container and feed process() with leftover rows
   * @param partition the hash partition being brought back to memory at the moment
   * @throws HiveException
   */
  protected void reProcessBigTable(HashPartition partition) throws HiveException {
    ObjectContainer bigTable = partition.getMatchfileObjContainer();
    while (bigTable.hasNext()) {
      Object row = bigTable.next();
      process(row, tag);
    }
    bigTable.clear();
  }

  /**
   * Implements the getName function for the Node Interface.
   *
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "MAPJOIN";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.MAPJOIN;
  }
}
