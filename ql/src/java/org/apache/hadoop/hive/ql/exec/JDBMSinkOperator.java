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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectValue;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.JDBMSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.util.JoinUtil;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.util.ReflectionUtils;


public class JDBMSinkOperator extends TerminalOperator<JDBMSinkDesc>
implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(JDBMSinkOperator.class
      .getName());

  //from abstract map join operator
  /**
   * The expressions for join inputs's join keys.
   */
  protected transient Map<Byte, List<ExprNodeEvaluator>> joinKeys;
  /**
   * The ObjectInspectors for the join inputs's join keys.
   */
  protected transient Map<Byte, List<ObjectInspector>> joinKeysObjectInspectors;
  /**
   * The standard ObjectInspectors for the join inputs's join keys.
   */
  protected transient Map<Byte, List<ObjectInspector>> joinKeysStandardObjectInspectors;

  protected transient int posBigTableTag = -1; // one of the tables that is not in memory
  protected transient int posBigTableAlias = -1; // one of the tables that is not in memory
  transient int mapJoinRowsKey; // rows for a given key

  protected transient RowContainer<ArrayList<Object>> emptyList = null;

  transient int numMapRowsRead;
  protected transient int totalSz; // total size of the composite object
  transient boolean firstRow;
  private boolean smallTablesOnly;
  /**
   * The filters for join
   */
  protected transient Map<Byte, List<ExprNodeEvaluator>> joinFilters;

  protected transient int numAliases; // number of aliases
  /**
   * The expressions for join outputs.
   */
  protected transient Map<Byte, List<ExprNodeEvaluator>> joinValues;
  /**
   * The ObjectInspectors for the join inputs.
   */
  protected transient Map<Byte, List<ObjectInspector>> joinValuesObjectInspectors;
  /**
   * The ObjectInspectors for join filters.
   */
  protected transient Map<Byte, List<ObjectInspector>> joinFilterObjectInspectors;
  /**
   * The standard ObjectInspectors for the join inputs.
   */
  protected transient Map<Byte, List<ObjectInspector>> joinValuesStandardObjectInspectors;

  protected transient
  Map<Byte, List<ObjectInspector>> rowContainerStandardObjectInspectors;

  protected transient Byte[] order; // order in which the results should
  Configuration hconf;
  protected transient Byte alias;
  protected transient Map<Byte, TableDesc> spillTableDesc; // spill tables are

  protected transient Map<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>> mapJoinTables;
  protected transient boolean noOuterJoin;

  public static class JDBMSinkObjectCtx {
    ObjectInspector standardOI;
    SerDe serde;
    TableDesc tblDesc;
    Configuration conf;

    /**
     * @param standardOI
     * @param serde
     */
    public JDBMSinkObjectCtx(ObjectInspector standardOI, SerDe serde,
        TableDesc tblDesc, Configuration conf) {
      this.standardOI = standardOI;
      this.serde = serde;
      this.tblDesc = tblDesc;
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

    public Configuration getConf() {
      return conf;
    }

  }

  private static final transient String[] FATAL_ERR_MSG = {
      null, // counter value 0 means no error
      "Mapside join size exceeds hive.mapjoin.maxsize. "
          + "Please increase that or remove the mapjoin hint."
      };
  transient int metadataKeyTag;
  transient int[] metadataValueTag;
  transient int maxMapJoinSize;


  public JDBMSinkOperator(){
    //super();
  }

  public JDBMSinkOperator(MapJoinOperator mjop){
    this.conf = new JDBMSinkDesc(mjop.getConf());
  }


  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    maxMapJoinSize = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEMAXMAPJOINSIZE);

    numMapRowsRead = 0;
    firstRow = true;

   //for small tables only; so get the big table position first
    posBigTableTag = conf.getPosBigTable();

    order = conf.getTagOrder();

    posBigTableAlias=order[posBigTableTag];

    //initialize some variables, which used to be initialized in CommonJoinOperator
    numAliases = conf.getExprs().size();
    this.hconf = hconf;
    totalSz = 0;

    noOuterJoin = conf.isNoOuterJoin();

    //process join keys
    joinKeys = new HashMap<Byte, List<ExprNodeEvaluator>>();
    JoinUtil.populateJoinKeyValue(joinKeys, conf.getKeys(),order,posBigTableAlias);
    joinKeysObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinKeys,inputObjInspectors,posBigTableAlias);
    joinKeysStandardObjectInspectors = JoinUtil.getStandardObjectInspectors(joinKeysObjectInspectors,posBigTableAlias);

    //process join values
    joinValues = new HashMap<Byte, List<ExprNodeEvaluator>>();
    JoinUtil.populateJoinKeyValue(joinValues, conf.getExprs(),order,posBigTableAlias);
    joinValuesObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinValues,inputObjInspectors,posBigTableAlias);
    joinValuesStandardObjectInspectors = JoinUtil.getStandardObjectInspectors(joinValuesObjectInspectors,posBigTableAlias);

    //process join filters
    joinFilters = new HashMap<Byte, List<ExprNodeEvaluator>>();
    JoinUtil.populateJoinKeyValue(joinFilters, conf.getFilters(),order,posBigTableAlias);
    joinFilterObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinValues,inputObjInspectors,posBigTableAlias);




    if (noOuterJoin) {
      rowContainerStandardObjectInspectors = joinValuesStandardObjectInspectors;
    } else {
      Map<Byte, List<ObjectInspector>> rowContainerObjectInspectors =
        new HashMap<Byte, List<ObjectInspector>>();
      for (Byte alias : order) {
        if(alias == posBigTableAlias){
          continue;
        }
        ArrayList<ObjectInspector> rcOIs = new ArrayList<ObjectInspector>();
        rcOIs.addAll(joinValuesObjectInspectors.get(alias));
        // for each alias, add object inspector for boolean as the last element
        rcOIs.add(
            PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
        rowContainerObjectInspectors.put(alias, rcOIs);
      }
      rowContainerStandardObjectInspectors =
        getStandardObjectInspectors(rowContainerObjectInspectors);
    }

    metadataValueTag = new int[numAliases];
    for (int pos = 0; pos < numAliases; pos++) {
      metadataValueTag[pos] = -1;
    }

    mapJoinTables = new HashMap<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>>();

    // initialize the hash tables for other tables
    for (Byte pos:order) {
      if (pos == posBigTableTag) {
        continue;
      }

      int cacheSize = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVEMAPJOINCACHEROWS);
      HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue> hashTable = new HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>(
          cacheSize);

      mapJoinTables.put(pos, hashTable);
    }
  }



  protected static HashMap<Byte, List<ObjectInspector>> getStandardObjectInspectors(
      Map<Byte, List<ObjectInspector>> aliasToObjectInspectors) {
    HashMap<Byte, List<ObjectInspector>> result = new HashMap<Byte, List<ObjectInspector>>();
    for (Entry<Byte, List<ObjectInspector>> oiEntry : aliasToObjectInspectors
        .entrySet()) {
      Byte alias = oiEntry.getKey();
      List<ObjectInspector> oiList = oiEntry.getValue();
      ArrayList<ObjectInspector> fieldOIList = new ArrayList<ObjectInspector>(
          oiList.size());
      for (int i = 0; i < oiList.size(); i++) {
        fieldOIList.add(ObjectInspectorUtils.getStandardObjectInspector(oiList
            .get(i), ObjectInspectorCopyOption.WRITABLE));
      }
      result.put(alias, fieldOIList);
    }
    return result;

  }

  /*
   * This operator only process small tables
   * Read the key/value pairs
   * Load them into hashtable
   */
  @Override
  public void processOp(Object row, int tag) throws HiveException{
      //let the mapJoinOp process these small tables
    try{
      alias = order[tag];
      //alias = (byte)tag;

      // compute keys and values as StandardObjects
      ArrayList<Object> key = JoinUtil.computeKeys(row, joinKeys.get(alias),
          joinKeysObjectInspectors.get(alias));

      ArrayList<Object> value = JoinUtil.computeValues(row, joinValues.get(alias),
          joinValuesObjectInspectors.get(alias),joinFilters.get(alias),
          joinFilterObjectInspectors.get(alias), noOuterJoin);


      if (firstRow) {
        metadataKeyTag = -1;

        TableDesc keyTableDesc = conf.getKeyTblDesc();
        SerDe keySerializer = (SerDe) ReflectionUtils.newInstance(
            keyTableDesc.getDeserializerClass(), null);
        keySerializer.initialize(null, keyTableDesc.getProperties());

        MapJoinMetaData.clear();
        MapJoinMetaData.put(Integer.valueOf(metadataKeyTag),
            new JDBMSinkObjectCtx(
            ObjectInspectorUtils
            .getStandardObjectInspector(keySerializer
            .getObjectInspector(),
            ObjectInspectorCopyOption.WRITABLE), keySerializer,
            keyTableDesc, hconf));

        firstRow = false;
      }

      numMapRowsRead++;

      if ((numMapRowsRead > maxMapJoinSize)&& (counterNameToEnum != null)) {
        // update counter
        LOG
            .warn("Too many rows in map join tables. Fatal error counter will be incremented!!");
        incrCounter(fatalErrorCntr, 1);
        fatalError = true;
        return;
      }

      HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue> hashTable = mapJoinTables.get((byte) tag);
      MapJoinObjectKey keyMap = new MapJoinObjectKey(metadataKeyTag, key);
      MapJoinObjectValue o = hashTable.get(keyMap);
      RowContainer res = null;

      boolean needNewKey = true;
      if (o == null) {
        int bucketSize = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEMAPJOINBUCKETCACHESIZE);
        res = JoinUtil.getRowContainer(hconf,
            rowContainerStandardObjectInspectors.get((byte)tag),
            order[tag], bucketSize,spillTableDesc,conf,noOuterJoin);

        res.add(value);
      } else {
        res = o.getObj();
        res.add(value);

        if (hashTable.cacheSize() > 0) {
          o.setObj(res);
          needNewKey = false;
        }
      }

      if (metadataValueTag[tag] == -1) {
        metadataValueTag[tag] = order[tag];

        TableDesc valueTableDesc = conf.getValueTblDescs().get(tag);
        SerDe valueSerDe = (SerDe) ReflectionUtils.newInstance(valueTableDesc
            .getDeserializerClass(), null);
        valueSerDe.initialize(null, valueTableDesc.getProperties());

        MapJoinMetaData.put(Integer.valueOf(metadataValueTag[tag]),
            new JDBMSinkObjectCtx(ObjectInspectorUtils
            .getStandardObjectInspector(valueSerDe.getObjectInspector(),
            ObjectInspectorCopyOption.WRITABLE), valueSerDe,
            valueTableDesc, hconf));
      }

      // Construct externalizable objects for key and value
      if (needNewKey) {
        MapJoinObjectKey keyObj = new MapJoinObjectKey(metadataKeyTag, key);
        MapJoinObjectValue valueObj = new MapJoinObjectValue(
            metadataValueTag[tag], res);

        //valueObj.setConf(hconf);
        valueObj.setConf(hconf);
        // This may potentially increase the size of the hashmap on the mapper
        if (res.size() > mapJoinRowsKey) {
          if (res.size() % 100 == 0) {
            LOG.warn("Number of values for a given key " + keyObj + " are "
                + res.size());
            LOG.warn("used memory " + Runtime.getRuntime().totalMemory());
          }
        }
        hashTable.put(keyObj, valueObj);
      }
    }catch (SerDeException e) {
      e.printStackTrace();
      throw new HiveException(e);
    }

  }


  @Override
  /*
   * Flush the hashtable into jdbm file
   * Load this jdbm file into HDFS only
   */
  public void closeOp(boolean abort) throws HiveException{
    try{
      if(mapJoinTables != null) {
        //get tmp file URI
        String tmpURI = this.getExecContext().getLocalWork().getTmpFileURI();
        LOG.info("Get TMP URI: "+tmpURI);

        for (Map.Entry<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>> hashTables : mapJoinTables.entrySet()) {
          //get the key and value
          Byte tag = hashTables.getKey();
          HashMapWrapper hashTable = hashTables.getValue();

          //get the jdbm file and path
          String jdbmFile = hashTable.flushMemoryCacheToPersistent();
          Path localPath = new Path(jdbmFile);

          //get current input file name
          String bigBucketFileName = this.getExecContext().getCurrentBigBucketFile();
          if(bigBucketFileName == null ||bigBucketFileName.length()==0) {
            bigBucketFileName="-";
          }
          //get the tmp URI path; it will be a hdfs path if not local mode
          Path tmpURIPath = new Path(tmpURI+Path.SEPARATOR+"-"+tag+"-"+bigBucketFileName+".jdbm");

          //upload jdbm file to this HDFS
          FileSystem fs = tmpURIPath.getFileSystem(this.getExecContext().getJc());
          fs.copyFromLocalFile(localPath, tmpURIPath);
          LOG.info("Upload 1 JDBM File to: "+tmpURIPath);
          //remove the original jdbm tmp file
          hashTable.close();
        }
      }

      super.closeOp(abort);
     }catch(IOException e){
       LOG.error("Copy local file to HDFS error");
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
    return "JDBMSINK";
  }

  @Override
  public int getType() {
    return OperatorType.JDBMSINK;
  }

  private void getPersistentFilePath(Map<Byte,Path> paths) throws HiveException{
    Map<Byte,Path> jdbmFilePaths = paths;
    try{
      if(mapJoinTables != null) {
        for (Map.Entry<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>> hashTables : mapJoinTables.entrySet()) {
          //hashTable.close();

          Byte key = hashTables.getKey();
          HashMapWrapper hashTable = hashTables.getValue();

          //get the jdbm file and path
          String jdbmFile = hashTable.flushMemoryCacheToPersistent();
          Path localPath = new Path(jdbmFile);

          //insert into map
          jdbmFilePaths.put(key, localPath);
        }
      }
    }catch (Exception e){
      LOG.fatal("Get local JDBM file error");
      e.printStackTrace();
    }
  }

}
