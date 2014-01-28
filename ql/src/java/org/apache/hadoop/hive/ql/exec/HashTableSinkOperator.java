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

import java.io.BufferedOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionHandler;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinRowContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.HashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.util.ReflectionUtils;


public class HashTableSinkOperator extends TerminalOperator<HashTableSinkDesc> implements
    Serializable {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(HashTableSinkOperator.class.getName());

  /**
   * The expressions for join inputs's join keys.
   */
  private transient List<ExprNodeEvaluator>[] joinKeys;
  /**
   * The ObjectInspectors for the join inputs's join keys.
   */
  private transient List<ObjectInspector>[] joinKeysObjectInspectors;

  private transient int posBigTableAlias = -1; // one of the tables that is not in memory

  /**
   * The filters for join
   */
  private transient List<ExprNodeEvaluator>[] joinFilters;  

  private transient int[][] filterMaps;

  /**
   * The expressions for join outputs.
   */
  private transient List<ExprNodeEvaluator>[] joinValues;
  /**
   * The ObjectInspectors for the join inputs.
   */
  private transient List<ObjectInspector>[] joinValuesObjectInspectors;
  /**
   * The ObjectInspectors for join filters.
   */
  private transient List<ObjectInspector>[] joinFilterObjectInspectors;

  private transient Byte[] order; // order in which the results should
  private Configuration hconf;
  private transient Byte alias;

  private transient MapJoinTableContainer[] mapJoinTables;
  private transient MapJoinTableContainerSerDe[] mapJoinTableSerdes;  

  private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
  private static final MapJoinRowContainer EMPTY_ROW_CONTAINER = new MapJoinRowContainer();
  static {
    EMPTY_ROW_CONTAINER.add(EMPTY_OBJECT_ARRAY);
  }
  
  private transient boolean noOuterJoin;

  private long rowNumber = 0;
  private transient LogHelper console;
  private long hashTableScale;
  private MapJoinMemoryExhaustionHandler memoryExhaustionHandler;
  
  public HashTableSinkOperator() {
  }

  public HashTableSinkOperator(MapJoinOperator mjop) {
    this.conf = new HashTableSinkDesc(mjop.getConf());
  }


  @Override
  @SuppressWarnings("unchecked")
  protected void initializeOp(Configuration hconf) throws HiveException {
    boolean isSilent = HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVESESSIONSILENT);
    console = new LogHelper(LOG, isSilent);
    memoryExhaustionHandler = new MapJoinMemoryExhaustionHandler(console, conf.getHashtableMemoryUsage());

    // for small tables only; so get the big table position first
    posBigTableAlias = conf.getPosBigTable();

    order = conf.getTagOrder();

    // initialize some variables, which used to be initialized in CommonJoinOperator
    this.hconf = hconf;

    noOuterJoin = conf.isNoOuterJoin();
    filterMaps = conf.getFilterMap();

    int tagLen = conf.getTagLength();

    // process join keys
    joinKeys = new List[tagLen];
    JoinUtil.populateJoinKeyValue(joinKeys, conf.getKeys(), posBigTableAlias);
    joinKeysObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinKeys,
        inputObjInspectors, posBigTableAlias, tagLen);

    // process join values
    joinValues = new List[tagLen];
    JoinUtil.populateJoinKeyValue(joinValues, conf.getExprs(), posBigTableAlias);
    joinValuesObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinValues,
        inputObjInspectors, posBigTableAlias, tagLen);

    // process join filters
    joinFilters = new List[tagLen];
    JoinUtil.populateJoinKeyValue(joinFilters, conf.getFilters(), posBigTableAlias);
    joinFilterObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinFilters,
        inputObjInspectors, posBigTableAlias, tagLen);

    if (!noOuterJoin) {
      List<ObjectInspector>[] rowContainerObjectInspectors = new List[tagLen];
      for (Byte alias : order) {
        if (alias == posBigTableAlias) {
          continue;
        }
        List<ObjectInspector> rcOIs = joinValuesObjectInspectors[alias];
        if (filterMaps != null && filterMaps[alias] != null) {
          // for each alias, add object inspector for filter tag as the last element
          rcOIs = new ArrayList<ObjectInspector>(rcOIs);
          rcOIs.add(PrimitiveObjectInspectorFactory.writableShortObjectInspector);
        }
        rowContainerObjectInspectors[alias] = rcOIs;
      }
    }
    mapJoinTables = new MapJoinTableContainer[tagLen];
    mapJoinTableSerdes = new MapJoinTableContainerSerDe[tagLen];
    int hashTableThreshold = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLETHRESHOLD);
    float hashTableLoadFactor = HiveConf.getFloatVar(hconf,
        HiveConf.ConfVars.HIVEHASHTABLELOADFACTOR);
    hashTableScale = HiveConf.getLongVar(hconf, HiveConf.ConfVars.HIVEHASHTABLESCALE);
    if (hashTableScale <= 0) {
      hashTableScale = 1;
    }
    try {
      TableDesc keyTableDesc = conf.getKeyTblDesc();
      SerDe keySerde = (SerDe) ReflectionUtils.newInstance(keyTableDesc.getDeserializerClass(),
          null);
      keySerde.initialize(null, keyTableDesc.getProperties());
      MapJoinObjectSerDeContext keyContext = new MapJoinObjectSerDeContext(keySerde, false);
      for (Byte pos : order) {
        if (pos == posBigTableAlias) {
          continue;
        }
        mapJoinTables[pos] = new HashMapWrapper(hashTableThreshold, hashTableLoadFactor);        
        TableDesc valueTableDesc = conf.getValueTblFilteredDescs().get(pos);
        SerDe valueSerDe = (SerDe) ReflectionUtils.newInstance(valueTableDesc.getDeserializerClass(), null);
        valueSerDe.initialize(null, valueTableDesc.getProperties());
        mapJoinTableSerdes[pos] = new MapJoinTableContainerSerDe(keyContext, new MapJoinObjectSerDeContext(
            valueSerDe, hasFilter(pos)));
      }
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }



  private static List<ObjectInspector>[] getStandardObjectInspectors(
      List<ObjectInspector>[] aliasToObjectInspectors, int maxTag) {
    @SuppressWarnings("unchecked")
    List<ObjectInspector>[] result = new List[maxTag];
    for (byte alias = 0; alias < aliasToObjectInspectors.length; alias++) {
      List<ObjectInspector> oiList = aliasToObjectInspectors[alias];
      if (oiList == null) {
        continue;
      }
      ArrayList<ObjectInspector> fieldOIList = new ArrayList<ObjectInspector>(oiList.size());
      for (int i = 0; i < oiList.size(); i++) {
        fieldOIList.add(ObjectInspectorUtils.getStandardObjectInspector(oiList.get(i),
            ObjectInspectorCopyOption.WRITABLE));
      }
      result[alias] = fieldOIList;
    }
    return result;

  }

  /*
   * This operator only process small tables Read the key/value pairs Load them into hashtable
   */
  @Override
  public void processOp(Object row, int tag) throws HiveException {
    alias = (byte)tag;
    // compute keys and values as StandardObjects
    MapJoinKey key = JoinUtil.computeMapJoinKeys(null, row, joinKeys[alias],
        joinKeysObjectInspectors[alias]);
    Object[] value = EMPTY_OBJECT_ARRAY;
    if((hasFilter(alias) && filterMaps[alias].length > 0) || joinValues[alias].size() > 0) {
      value = JoinUtil.computeMapJoinValues(row, joinValues[alias],
        joinValuesObjectInspectors[alias], joinFilters[alias], joinFilterObjectInspectors[alias],
        filterMaps == null ? null : filterMaps[alias]);
    }
    MapJoinTableContainer tableContainer = mapJoinTables[alias];
    MapJoinRowContainer rowContainer = tableContainer.get(key);
    if (rowContainer == null) {
      if(value.length != 0) {
        rowContainer = new MapJoinRowContainer();
        rowContainer.add(value);
      } else {
        rowContainer = EMPTY_ROW_CONTAINER;
      }
      rowNumber++;
      if (rowNumber > hashTableScale && rowNumber % hashTableScale == 0) {
        memoryExhaustionHandler.checkMemoryStatus(tableContainer.size(), rowNumber);
      }
      tableContainer.put(key, rowContainer);
    } else if (rowContainer == EMPTY_ROW_CONTAINER) {
      rowContainer = rowContainer.copy();
      rowContainer.add(value);
      tableContainer.put(key, rowContainer);
    } else {
      rowContainer.add(value);
    }
  }
  private boolean hasFilter(int alias) {
    return filterMaps != null && filterMaps[alias] != null;
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    try {
      if (mapJoinTables != null) {
        // get tmp path
        Path tmpPath = this.getExecContext().getLocalWork().getTmpPath();
        LOG.info("Temp URI for side table: " + tmpPath);
        for (byte tag = 0; tag < mapJoinTables.length; tag++) {
          // get the key and value
          MapJoinTableContainer tableContainer = mapJoinTables[tag];
          if (tableContainer == null) {
            continue;
          }
          // get current input file name
          String bigBucketFileName = getExecContext().getCurrentBigBucketFile();
          String fileName = getExecContext().getLocalWork().getBucketFileName(bigBucketFileName);
          // get the tmp URI path; it will be a hdfs path if not local mode
          String dumpFilePrefix = conf.getDumpFilePrefix();
          Path path = Utilities.generatePath(tmpPath, dumpFilePrefix, tag, fileName);
          console.printInfo(Utilities.now() + "\tDump the side-table into file: " + path);
          // get the hashtable file and path
          FileSystem fs = path.getFileSystem(hconf);
          ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(fs.create(path), 4096));
          try {
            mapJoinTableSerdes[tag].persist(out, tableContainer);
          } finally {
            out.close();
          }
          tableContainer.clear();
          console.printInfo(Utilities.now() + "\tUpload 1 File to: " + path);
        }
      }
      super.closeOp(abort);
    } catch (Exception e) {
      LOG.error("Error generating side-table", e);
    }
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
    return "HASHTABLESINK";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.HASHTABLESINK;
  }
}
