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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.HashTableLoaderFactory;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionHandler;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinRowContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer.ReusableGetAdaptor;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.persistence.UnwrapRowContainer;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Map side Join operator implementation.
 */
public class MapJoinOperator extends AbstractMapJoinOperator<MapJoinDesc> implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(MapJoinOperator.class.getName());
  private static final String CLASS_NAME = MapJoinOperator.class.getName();
  private final PerfLogger perfLogger = PerfLogger.getPerfLogger();

  private transient String tableKey;
  private transient String serdeKey;
  private transient ObjectCache cache;

  protected HashTableLoader loader;

  protected transient MapJoinTableContainer[] mapJoinTables;
  private transient MapJoinTableContainerSerDe[] mapJoinTableSerdes;
  private transient boolean hashTblInitedOnce;
  private transient ReusableGetAdaptor[] hashMapRowGetters;

  private UnwrapRowContainer[] unwrapContainer;

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
  protected void initializeOp(Configuration hconf) throws HiveException {
    unwrapContainer = new UnwrapRowContainer[conf.getTagLength()];
    super.initializeOp(hconf);

    int tagLen = conf.getTagLength();

    // On Tez only: The hash map might already be cached in the container we run
    // the task in. On MR: The cache is a no-op.
    tableKey = "__HASH_MAP_"+this.getOperatorId()+"_container";
    serdeKey = "__HASH_MAP_"+this.getOperatorId()+"_serde";

    cache = ObjectCacheFactory.getCache(hconf);
    loader = HashTableLoaderFactory.getLoader(hconf);

    hashMapRowGetters = null;

    mapJoinTables = (MapJoinTableContainer[]) cache.retrieve(tableKey);
    mapJoinTableSerdes = (MapJoinTableContainerSerDe[]) cache.retrieve(serdeKey);
    hashTblInitedOnce = true;
    LOG.info("Try to retrieve from cache");

    if (mapJoinTables == null || mapJoinTableSerdes == null) {
      LOG.info("Did not find tables in cache");
      mapJoinTables = new MapJoinTableContainer[tagLen];
      mapJoinTableSerdes = new MapJoinTableContainerSerDe[tagLen];
      hashTblInitedOnce = false;
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

  public void generateMapMetaData() throws HiveException, SerDeException {
    // generate the meta data for key
    // index for key is -1

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
      SerDe valueSerDe = (SerDe) ReflectionUtils.newInstance(valueTableDesc.getDeserializerClass(),
          null);
      SerDeUtils.initializeSerDe(valueSerDe, null, valueTableDesc.getProperties(), null);
      MapJoinObjectSerDeContext valueContext = new MapJoinObjectSerDeContext(valueSerDe, hasFilter(pos));
      mapJoinTableSerdes[pos] = new MapJoinTableContainerSerDe(keyContext, valueContext);
    }
  }

  private void loadHashTable() throws HiveException {

    if ((this.getExecContext() == null)
        || (this.getExecContext().getLocalWork() == null)
        || (this.getExecContext().getLocalWork().getInputFileChangeSensitive() == false)
    ) {
      /*
       * This early-exit criteria is not applicable if the local work is sensitive to input file changes.
       * But the check does no apply if there is no local work, or if this is a reducer vertex (execContext is null).
       */
      if (hashTblInitedOnce) {
        return;
      } else {
        hashTblInitedOnce = true;
      }
    }
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.LOAD_HASHTABLE);
    loader.init(getExecContext(), hconf, this);
    long memUsage = (long)(MapJoinMemoryExhaustionHandler.getMaxHeapSize()
        * conf.getHashTableMemoryUsage());
    loader.load(mapJoinTables, mapJoinTableSerdes, memUsage);
    if (!conf.isBucketMapJoin()) {
      /*
       * The issue with caching in case of bucket map join is that different tasks
       * process different buckets and if the container is reused to join a different bucket,
       * join results can be incorrect. The cache is keyed on operator id and for bucket map join
       * the operator does not change but data needed is different. For a proper fix, this
       * requires changes in the Tez API with regard to finding bucket id and
       * also ability to schedule tasks to re-use containers that have cached the specific bucket.
       */
      LOG.info("This is not bucket map join, so cache");
      cache.cache(tableKey, mapJoinTables);
      cache.cache(serdeKey, mapJoinTableSerdes);
    }
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.LOAD_HASHTABLE);
  }

  // Load the hash table
  @Override
  public void cleanUpInputFileChangedOp() throws HiveException {
    try {
      if (firstRow) {
        // generate the map metadata
        generateMapMetaData();
        firstRow = false;
      }
      loadHashTable();
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }

  protected void setMapJoinKey(
      ReusableGetAdaptor dest, Object row, byte alias) throws HiveException {
    dest.setFromRow(row, joinKeys[alias], joinKeysObjectInspectors[alias]);
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
  public void processOp(Object row, int tag) throws HiveException {
    try {
      if (firstRow) {
        generateMapMetaData();
        loadHashTable();
        firstRow = false;
      }

      alias = (byte) tag;
      if (hashMapRowGetters == null) {
        hashMapRowGetters = new ReusableGetAdaptor[mapJoinTables.length];
        MapJoinKey refKey = getRefKey(alias);
        for (byte pos = 0; pos < order.length; pos++) {
          if (pos != alias) {
            hashMapRowGetters[pos] = mapJoinTables[pos].createGetter(refKey);
          }
        }
      }

      // compute keys and values as StandardObjects
      ReusableGetAdaptor firstSetKey = null;
      int fieldCount = joinKeys[alias].size();
      boolean joinNeeded = false;
      for (byte pos = 0; pos < order.length; pos++) {
        if (pos != alias) {
          ReusableGetAdaptor adaptor;
          if (firstSetKey == null) {
            adaptor = firstSetKey = hashMapRowGetters[pos];
            setMapJoinKey(firstSetKey, row, alias);
          } else {
            // Keys for all tables are the same, so only the first has to deserialize them.
            adaptor = hashMapRowGetters[pos];
            adaptor.setFromOther(firstSetKey);
          }
          MapJoinRowContainer rowContainer = adaptor.getCurrentRows();
          if (rowContainer != null && unwrapContainer[pos] != null) {
            Object[] currentKey = firstSetKey.getCurrentKey();
            rowContainer = unwrapContainer[pos].setInternal(rowContainer, currentKey);
          }
          // there is no join-value or join-key has all null elements
          if (rowContainer == null || firstSetKey.hasAnyNulls(fieldCount, nullsafes)) {
            if (!noOuterJoin) {
              joinNeeded = true;
              storage[pos] = dummyObjVectors[pos];
            } else {
              storage[pos] = emptyList;
            }
          } else {
            joinNeeded = true;
            storage[pos] = rowContainer.copy();
            aliasFilterTags[pos] = rowContainer.getAliasFilter();
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

  @Override
  public void closeOp(boolean abort) throws HiveException {
    for (MapJoinTableContainer tableContainer : mapJoinTables) {
      if (tableContainer != null) {
        tableContainer.dumpMetrics();
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
    super.closeOp(abort);
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
