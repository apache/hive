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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.persistence.AbstractMapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectValue;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinRowContainer;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.HashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.util.ReflectionUtils;


public class HashTableSinkOperator extends TerminalOperator<HashTableSinkDesc> implements
    Serializable {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(HashTableSinkOperator.class.getName());

  protected static MapJoinMetaData metadata = new MapJoinMetaData();
  // from abstract map join operator
  /**
   * The expressions for join inputs's join keys.
   */
  protected transient List<ExprNodeEvaluator>[] joinKeys;
  /**
   * The ObjectInspectors for the join inputs's join keys.
   */
  protected transient List<ObjectInspector>[] joinKeysObjectInspectors;
  /**
   * The standard ObjectInspectors for the join inputs's join keys.
   */
  protected transient List<ObjectInspector>[] joinKeysStandardObjectInspectors;

  protected transient int posBigTableAlias = -1; // one of the tables that is not in memory
  transient int mapJoinRowsKey; // rows for a given key

  protected transient RowContainer<ArrayList<Object>> emptyList = null;

  transient int numMapRowsRead;
  protected transient int totalSz; // total size of the composite object
  transient boolean firstRow;
  /**
   * The filters for join
   */
  protected transient List<ExprNodeEvaluator>[] joinFilters;

  protected transient int[][] filterMaps;

  protected transient int numAliases; // number of aliases
  /**
   * The expressions for join outputs.
   */
  protected transient List<ExprNodeEvaluator>[] joinValues;
  /**
   * The ObjectInspectors for the join inputs.
   */
  protected transient List<ObjectInspector>[] joinValuesObjectInspectors;
  /**
   * The ObjectInspectors for join filters.
   */
  protected transient List<ObjectInspector>[] joinFilterObjectInspectors;
  /**
   * The standard ObjectInspectors for the join inputs.
   */
  protected transient List<ObjectInspector>[] joinValuesStandardObjectInspectors;

  protected transient List<ObjectInspector>[] rowContainerStandardObjectInspectors;

  protected transient Byte[] order; // order in which the results should
  Configuration hconf;
  protected transient Byte alias;
  protected transient TableDesc[] spillTableDesc; // spill tables are

  protected transient HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue>[] mapJoinTables;
  protected transient boolean noOuterJoin;

  private long rowNumber = 0;
  protected transient LogHelper console;
  private long hashTableScale;
  private boolean isAbort = false;

  public static class HashTableSinkObjectCtx {
    ObjectInspector standardOI;
    SerDe serde;
    TableDesc tblDesc;
    Configuration conf;
    boolean hasFilter;

    /**
     * @param standardOI
     * @param serde
     */
    public HashTableSinkObjectCtx(ObjectInspector standardOI, SerDe serde, TableDesc tblDesc,
        boolean hasFilter, Configuration conf) {
      this.standardOI = standardOI;
      this.serde = serde;
      this.tblDesc = tblDesc;
      this.hasFilter = hasFilter;
      this.conf = conf;
    }

    /**
     * @return the standardOI
     */
    public ObjectInspector getStandardOI() {
      return standardOI;
    }

    /**
     * @return the serde
     */
    public SerDe getSerDe() {
      return serde;
    }

    public TableDesc getTblDesc() {
      return tblDesc;
    }

    public boolean hasFilterTag() {
      return hasFilter;
    }

    public Configuration getConf() {
      return conf;
    }

  }

  public static MapJoinMetaData getMetadata() {
    return metadata;
  }

  private static final transient String[] FATAL_ERR_MSG = {
      null, // counter value 0 means no error
      "Mapside join exceeds available memory. "
          + "Please try removing the mapjoin hint."};
  private final int metadataKeyTag = -1;
  transient int[] metadataValueTag;


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
    numMapRowsRead = 0;
    firstRow = true;

    // for small tables only; so get the big table position first
    posBigTableAlias = conf.getPosBigTable();

    order = conf.getTagOrder();

    // initialize some variables, which used to be initialized in CommonJoinOperator
    numAliases = conf.getExprs().size();
    this.hconf = hconf;
    totalSz = 0;

    noOuterJoin = conf.isNoOuterJoin();
    filterMaps = conf.getFilterMap();

    int tagLen = conf.getTagLength();

    // process join keys
    joinKeys = new List[tagLen];
    JoinUtil.populateJoinKeyValue(joinKeys, conf.getKeys(), posBigTableAlias);
    joinKeysObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinKeys,
        inputObjInspectors, posBigTableAlias, tagLen);
    joinKeysStandardObjectInspectors = JoinUtil.getStandardObjectInspectors(
        joinKeysObjectInspectors, posBigTableAlias, tagLen);

    // process join values
    joinValues = new List[tagLen];
    JoinUtil.populateJoinKeyValue(joinValues, conf.getExprs(), posBigTableAlias);
    joinValuesObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinValues,
        inputObjInspectors, posBigTableAlias, tagLen);
    joinValuesStandardObjectInspectors = JoinUtil.getStandardObjectInspectors(
        joinValuesObjectInspectors, posBigTableAlias, tagLen);

    // process join filters
    joinFilters = new List[tagLen];
    JoinUtil.populateJoinKeyValue(joinFilters, conf.getFilters(), posBigTableAlias);
    joinFilterObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinFilters,
        inputObjInspectors, posBigTableAlias, tagLen);

    if (noOuterJoin) {
      rowContainerStandardObjectInspectors = joinValuesStandardObjectInspectors;
    } else {
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
      rowContainerStandardObjectInspectors = getStandardObjectInspectors(
          rowContainerObjectInspectors, tagLen);
    }

    metadataValueTag = new int[numAliases];
    for (int pos = 0; pos < numAliases; pos++) {
      metadataValueTag[pos] = -1;
    }
    mapJoinTables = new HashMapWrapper[tagLen];

    int hashTableThreshold = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLETHRESHOLD);
    float hashTableLoadFactor = HiveConf.getFloatVar(hconf,
        HiveConf.ConfVars.HIVEHASHTABLELOADFACTOR);
    float hashTableMaxMemoryUsage = this.getConf().getHashtableMemoryUsage();

    hashTableScale = HiveConf.getLongVar(hconf, HiveConf.ConfVars.HIVEHASHTABLESCALE);
    if (hashTableScale <= 0) {
      hashTableScale = 1;
    }

    // initialize the hash tables for other tables
    for (Byte pos : order) {
      if (pos == posBigTableAlias) {
        continue;
      }

      HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue> hashTable = new HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue>(
          hashTableThreshold, hashTableLoadFactor, hashTableMaxMemoryUsage);

      mapJoinTables[pos] = hashTable;
    }
  }



  protected static List<ObjectInspector>[] getStandardObjectInspectors(
      List<ObjectInspector>[] aliasToObjectInspectors, int maxTag) {
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

  private void setKeyMetaData() throws SerDeException {
    TableDesc keyTableDesc = conf.getKeyTblDesc();
    SerDe keySerializer = (SerDe) ReflectionUtils.newInstance(keyTableDesc.getDeserializerClass(),
        null);
    keySerializer.initialize(null, keyTableDesc.getProperties());

    metadata.put(Integer.valueOf(metadataKeyTag), new HashTableSinkObjectCtx(
        ObjectInspectorUtils.getStandardObjectInspector(keySerializer.getObjectInspector(),
            ObjectInspectorCopyOption.WRITABLE), keySerializer, keyTableDesc, false, hconf));
  }

  private boolean hasFilter(int alias) {
    return filterMaps != null && filterMaps[alias] != null;
  }
  /*
   * This operator only process small tables Read the key/value pairs Load them into hashtable
   */
  @Override
  public void processOp(Object row, int tag) throws HiveException {
    // let the mapJoinOp process these small tables
    try {
      if (firstRow) {
        // generate the map metadata
        setKeyMetaData();
        firstRow = false;
      }
      alias = (byte)tag;

      // compute keys and values as StandardObjects
      AbstractMapJoinKey keyMap = JoinUtil.computeMapJoinKeys(row, joinKeys[alias],
          joinKeysObjectInspectors[alias]);

      Object[] value = JoinUtil.computeMapJoinValues(row, joinValues[alias],
          joinValuesObjectInspectors[alias], joinFilters[alias], joinFilterObjectInspectors[alias],
          filterMaps == null ? null : filterMaps[alias]);

      HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue> hashTable = mapJoinTables[alias];

      MapJoinObjectValue o = hashTable.get(keyMap);
      MapJoinRowContainer<Object[]> res = null;

      boolean needNewKey = true;
      if (o == null) {
        res = new MapJoinRowContainer<Object[]>();
        res.add(value);

        if (metadataValueTag[tag] == -1) {
          metadataValueTag[tag] = order[tag];
          setValueMetaData(tag);
        }

        // Construct externalizable objects for key and value
        if (needNewKey) {
          MapJoinObjectValue valueObj = new MapJoinObjectValue(
              metadataValueTag[tag], res);

          rowNumber++;
          if (rowNumber > hashTableScale && rowNumber % hashTableScale == 0) {
            isAbort = hashTable.isAbort(rowNumber, console);
            if (isAbort) {
              throw new HiveException("RunOutOfMeomoryUsage");
            }
          }
          hashTable.put(keyMap, valueObj);
        }

      } else {
        res = o.getObj();
        res.add(value);
      }


    } catch (SerDeException e) {
      throw new HiveException(e);
    }

  }

  private void setValueMetaData(int tag) throws SerDeException {
    TableDesc valueTableDesc = conf.getValueTblFilteredDescs().get(tag);
    SerDe valueSerDe = (SerDe) ReflectionUtils.newInstance(valueTableDesc.getDeserializerClass(),
        null);

    valueSerDe.initialize(null, valueTableDesc.getProperties());

    List<ObjectInspector> newFields = rowContainerStandardObjectInspectors[alias];
    int length = newFields.size();
    List<String> newNames = new ArrayList<String>(length);
    for (int i = 0; i < length; i++) {
      String tmp = new String("tmp_" + i);
      newNames.add(tmp);
    }
    StandardStructObjectInspector standardOI = ObjectInspectorFactory
        .getStandardStructObjectInspector(newNames, newFields);

    int alias = Integer.valueOf(metadataValueTag[tag]);
    metadata.put(Integer.valueOf(metadataValueTag[tag]), new HashTableSinkObjectCtx(
        standardOI, valueSerDe, valueTableDesc, hasFilter(alias), hconf));
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    try {
      if (mapJoinTables != null) {
        // get tmp file URI
        String tmpURI = this.getExecContext().getLocalWork().getTmpFileURI();
        LOG.info("Get TMP URI: " + tmpURI);
        long fileLength;
        for (byte tag = 0; tag < mapJoinTables.length; tag++) {
          // get the key and value
          HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue> hashTable = mapJoinTables[tag];
          if (hashTable == null) {
            continue;
          }

          // get current input file name
          String bigBucketFileName = getExecContext().getCurrentBigBucketFile();

          String fileName = getExecContext().getLocalWork().getBucketFileName(bigBucketFileName);

          // get the tmp URI path; it will be a hdfs path if not local mode
          String dumpFilePrefix = conf.getDumpFilePrefix();
          String tmpURIPath = Utilities.generatePath(tmpURI, dumpFilePrefix, tag, fileName);
          hashTable.isAbort(rowNumber, console);
          console.printInfo(Utilities.now() + "\tDump the hashtable into file: " + tmpURIPath);
          // get the hashtable file and path
          Path path = new Path(tmpURIPath);
          FileSystem fs = path.getFileSystem(hconf);
          File file = new File(path.toUri().getPath());
          fs.create(path);
          fileLength = hashTable.flushMemoryCacheToPersistent(file);
          console.printInfo(Utilities.now() + "\tUpload 1 File to: " + tmpURIPath + " File size: "
              + fileLength);

          hashTable.close();
        }
      }

      super.closeOp(abort);
    } catch (Exception e) {
      LOG.error("Generate Hashtable error", e);
      e.printStackTrace();
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
