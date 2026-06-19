/*
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
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinEagerRowContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKeyObject;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinPersistableTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinRowContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.HashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.util.ReflectionUtils;

public class HashTableSinkOperator extends TerminalOperator<HashTableSinkDesc> implements
    Serializable {
  private static final long serialVersionUID = 1L;
  protected static final Logger LOG = LoggerFactory.getLogger(HashTableSinkOperator.class.getName());

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
  protected Configuration hconf;

  protected transient MapJoinPersistableTableContainer[] mapJoinTables;
  protected transient MapJoinTableContainerSerDe[] mapJoinTableSerdes;

  private final Object[] emptyObjectArray = new Object[0];
  private final MapJoinEagerRowContainer emptyRowContainer = new MapJoinEagerRowContainer();

  private long rowNumber = 0;
  protected transient LogHelper console;
  private long hashTableScale;
  private MemoryExhaustionChecker memoryExhaustionChecker;

  /** Kryo ctor. */
  protected HashTableSinkOperator() {
    super();
  }

  public HashTableSinkOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public HashTableSinkOperator(CompilationOpContext ctx, MapJoinOperator mjop) {
    this(ctx);
    this.conf = new HashTableSinkDesc(mjop.getConf());
  }


  @Override
  @SuppressWarnings("unchecked")
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    boolean isSilent = HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVE_SESSION_SILENT);
    console = new LogHelper(LOG, isSilent);
    memoryExhaustionChecker = MemoryExhaustionCheckerFactory.getChecker(console, hconf, conf);
    emptyRowContainer.addRow(emptyObjectArray);

    // for small tables only; so get the big table position first
    posBigTableAlias = conf.getPosBigTable();

    order = conf.getTagOrder();

    // initialize some variables, which used to be initialized in CommonJoinOperator
    this.hconf = hconf;

    filterMaps = conf.getFilterMap();

    int tagLen = conf.getTagLength();

    // process join keys
    joinKeys = new List[tagLen];
    JoinUtil.populateJoinKeyValue(joinKeys, conf.getKeys(), posBigTableAlias, hconf);
    joinKeysObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinKeys,
        inputObjInspectors, posBigTableAlias, tagLen);

    // process join values
    joinValues = new List[tagLen];
    JoinUtil.populateJoinKeyValue(joinValues, conf.getExprs(), posBigTableAlias, hconf);
    joinValuesObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinValues,
        inputObjInspectors, posBigTableAlias, tagLen);

    // process join filters
    joinFilters = new List[tagLen];
    JoinUtil.populateJoinKeyValue(joinFilters, conf.getFilters(), posBigTableAlias, hconf);
    joinFilterObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinFilters,
        inputObjInspectors, posBigTableAlias, tagLen);

    if (!conf.isNoOuterJoin()) {
      for (Byte alias : order) {
        if (alias == posBigTableAlias || joinValues[alias] == null) {
          continue;
        }
        List<ObjectInspector> rcOIs = joinValuesObjectInspectors[alias];
        if (filterMaps != null && filterMaps[alias] != null) {
          // for each alias, add object inspector for filter tag as the last element
          rcOIs = new ArrayList<ObjectInspector>(rcOIs);
          rcOIs.add(PrimitiveObjectInspectorFactory.writableShortObjectInspector);
        }
      }
    }
    mapJoinTables = new MapJoinPersistableTableContainer[tagLen];
    mapJoinTableSerdes = new MapJoinTableContainerSerDe[tagLen];
    hashTableScale = HiveConf.getLongVar(hconf, HiveConf.ConfVars.HIVE_HASHTABLE_SCALE);
    if (hashTableScale <= 0) {
      hashTableScale = 1;
    }
    try {
      TableDesc keyTableDesc = conf.getKeyTblDesc();
      AbstractSerDe keySerDe = (AbstractSerDe) ReflectionUtils.newInstance(keyTableDesc.getSerDeClass(),
          null);
      keySerDe.initialize(null, keyTableDesc.getProperties(), null);
      MapJoinObjectSerDeContext keyContext = new MapJoinObjectSerDeContext(keySerDe, false);
      for (Byte pos : order) {
        if (pos == posBigTableAlias) {
          continue;
        }
        mapJoinTables[pos] = new HashMapWrapper(hconf, -1);
        TableDesc valueTableDesc = conf.getValueTblFilteredDescs().get(pos);
        AbstractSerDe valueSerDe = (AbstractSerDe) ReflectionUtils.newInstance(valueTableDesc.getSerDeClass(), null);
        valueSerDe.initialize(null, valueTableDesc.getProperties(), null);
        mapJoinTableSerdes[pos] = new MapJoinTableContainerSerDe(keyContext, new MapJoinObjectSerDeContext(
            valueSerDe, hasFilter(pos)));
      }
    } catch (SerDeException e) {
      throw new HiveException(e);
    }

  }

  public MapJoinTableContainer[] getMapJoinTables() {
    return mapJoinTables;
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
  public void process(Object row, int tag) throws HiveException {
    byte alias = (byte)tag;
    // compute keys and values as StandardObjects. Use non-optimized key (MR).
    Object[] currentKey = new Object[joinKeys[alias].size()];
    for (int keyIndex = 0; keyIndex < joinKeys[alias].size(); ++keyIndex) {
      currentKey[keyIndex] = joinKeys[alias].get(keyIndex).evaluate(row);
    }
    MapJoinKeyObject key = new MapJoinKeyObject();
    key.readFromRow(currentKey, joinKeysObjectInspectors[alias]);

    Object[] value = emptyObjectArray;
    if((hasFilter(alias) && filterMaps[alias].length > 0) || joinValues[alias].size() > 0) {
      value = JoinUtil.computeMapJoinValues(row, joinValues[alias],
        joinValuesObjectInspectors[alias], joinFilters[alias], joinFilterObjectInspectors[alias],
        filterMaps == null ? null : filterMaps[alias]);
    }
    MapJoinPersistableTableContainer tableContainer = mapJoinTables[alias];
    MapJoinRowContainer rowContainer = tableContainer.get(key);
    if (rowContainer == null) {
      if(value.length != 0) {
        rowContainer = new MapJoinEagerRowContainer();
        rowContainer.addRow(value);
      } else {
        rowContainer = emptyRowContainer;
      }
      rowNumber++;
      memoryExhaustionChecker.checkMemoryOverhead(rowNumber, hashTableScale, tableContainer.size());
      tableContainer.put(key, rowContainer);
    } else if (rowContainer == emptyRowContainer) {
      rowContainer = rowContainer.copy();
      rowContainer.addRow(value);
      tableContainer.put(key, rowContainer);
    } else {
      rowContainer.addRow(value);
    }
  }
  private boolean hasFilter(int alias) {
    return filterMaps != null && filterMaps[alias] != null;
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    try {
      if (mapJoinTables == null) {
        LOG.debug("mapJoinTables is null");
      } else {
        flushToFile();
      }
      super.closeOp(abort);
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  protected void flushToFile() throws IOException, HiveException {
    // get tmp file URI
    Path tmpURI = getExecContext().getLocalWork().getTmpPath();
    LOG.info("Temp URI for side table: {}", tmpURI);
    for (byte tag = 0; tag < mapJoinTables.length; tag++) {
      // get the key and value
      MapJoinPersistableTableContainer tableContainer = mapJoinTables[tag];
      if (tableContainer == null) {
        continue;
      }
      // get current input file name
      String bigBucketFileName = getExecContext().getCurrentBigBucketFile();
      String fileName = getExecContext().getLocalWork().getBucketFileName(bigBucketFileName);
      // get the tmp URI path; it will be a hdfs path if not local mode
      // TODO [MM gap?]: this doesn't work, however this is MR only.
      //      The path for writer and reader mismatch:
      //      Dump the side-table for tag ... -local-10004/HashTable-Stage-1/MapJoin-a-00-(ds%3D2008-04-08)mm_2.hashtable
      //      Load back 1 hashtable file      -local-10004/HashTable-Stage-1/MapJoin-a-00-srcsortbucket3outof4.txt.hashtable
      //      Hive3 probably won't support MR so do we really care? 
      String dumpFilePrefix = conf.getDumpFilePrefix();
      Path path = Utilities.generatePath(tmpURI, dumpFilePrefix, tag, fileName);
      console.printInfo(Utilities.now() + "\tDump the side-table for tag: " + tag +
          " with group count: " + tableContainer.size() + " into file: " + path);
      // get the hashtable file and path
      FileSystem fs = path.getFileSystem(hconf);
      ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(fs.create(path)));
      try {
        mapJoinTableSerdes[tag].persist(out, tableContainer);
      } finally {
        out.close();
      }
      tableContainer.clear();
      FileStatus status = fs.getFileStatus(path);
      console.printInfo(Utilities.now() + "\tUploaded 1 File to: " + path +
          " (" + status.getLen() + " bytes)");
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
