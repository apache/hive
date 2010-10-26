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
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.JDBMSinkOperator.JDBMSinkObjectCtx;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectValue;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.util.JoinUtil;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Map side Join operator implementation.
 */
public class MapJoinOperator extends AbstractMapJoinOperator<MapJoinDesc> implements
    Serializable {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(MapJoinOperator.class
      .getName());


  protected transient Map<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>> mapJoinTables;

  private static final transient String[] FATAL_ERR_MSG = {
      null, // counter value 0 means no error
      "Mapside join size exceeds hive.mapjoin.maxsize. "
          + "Please increase that or remove the mapjoin hint."
      };




  transient int metadataKeyTag;
  transient int[] metadataValueTag;
  transient int maxMapJoinSize;
  private int bigTableAlias;
  public MapJoinOperator() {
  }

  public MapJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mjop) {
    super(mjop);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    super.initializeOp(hconf);

    maxMapJoinSize = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEMAXMAPJOINSIZE);

    metadataValueTag = new int[numAliases];
    for (int pos = 0; pos < numAliases; pos++) {
      metadataValueTag[pos] = -1;
    }

    metadataKeyTag = -1;
    bigTableAlias = order[posBigTable];

    mapJoinTables = new HashMap<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>>();

    // initialize the hash tables for other tables
    for (int pos = 0; pos < numAliases; pos++) {
      if (pos == posBigTable) {
        continue;
      }

      int cacheSize = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVEMAPJOINCACHEROWS);
      HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue> hashTable = new HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>(
          cacheSize);

      mapJoinTables.put(Byte.valueOf((byte) pos), hashTable);
    }

  }


  @Override
  protected void fatalErrorMessage(StringBuilder errMsg, long counterCode) {
    errMsg.append("Operator " + getOperatorId() + " (id=" + id + "): "
        + FATAL_ERR_MSG[(int) counterCode]);
  }


  public void generateMapMetaData() throws HiveException,SerDeException{
    //generate the meta data for key
    //index for key is -1
    TableDesc keyTableDesc = conf.getKeyTblDesc();
    SerDe keySerializer = (SerDe) ReflectionUtils.newInstance(
        keyTableDesc.getDeserializerClass(), null);
    keySerializer.initialize(null, keyTableDesc.getProperties());
    MapJoinMetaData.put(Integer.valueOf(metadataKeyTag),
        new JDBMSinkObjectCtx(
        ObjectInspectorUtils
        .getStandardObjectInspector(keySerializer
        .getObjectInspector(),
        ObjectInspectorCopyOption.WRITABLE), keySerializer,
        keyTableDesc, hconf));

    //index for values is just alias
    for (int tag = 0; tag < order.length; tag++) {
      int alias = (int) order[tag];

      if(alias == this.bigTableAlias){
        continue;
      }


      TableDesc valueTableDesc = conf.getValueTblDescs().get(tag);
      SerDe valueSerDe = (SerDe) ReflectionUtils.newInstance(valueTableDesc
          .getDeserializerClass(), null);
      valueSerDe.initialize(null, valueTableDesc.getProperties());

      MapJoinMetaData.put(Integer.valueOf(alias),
          new JDBMSinkObjectCtx(ObjectInspectorUtils
          .getStandardObjectInspector(valueSerDe.getObjectInspector(),
          ObjectInspectorCopyOption.WRITABLE), valueSerDe,
          valueTableDesc, hconf));
    }
  }

  private void loadJDBM() throws HiveException{
    boolean localMode = HiveConf.getVar(hconf, HiveConf.ConfVars.HADOOPJT).equals("local");
    String tmpURI =null;
    HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue> hashtable;
    Byte pos;
    int alias;

    String currentInputFile = HiveConf.getVar(hconf,
        HiveConf.ConfVars.HADOOPMAPFILENAME);
    LOG.info("******* Load from JDBM File: input : "+ currentInputFile);

    String currentFileName;

    if(this.getExecContext().getLocalWork().getInputFileChangeSensitive()) {
      currentFileName= this.getFileName(currentInputFile);
    } else {
      currentFileName="-";
    }
    LOG.info("******* Filename : "+ currentFileName);
    try{
      if(localMode){
        //load the jdbm file from tmp dir
        LOG.info("******* Load from tmp file uri ***");
        tmpURI= this.getExecContext().getLocalWork().getTmpFileURI();
        for(Map.Entry<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>> entry: mapJoinTables.entrySet()){
          pos = entry.getKey();
          hashtable=entry.getValue();
          URI uri = new URI(tmpURI+Path.SEPARATOR+"-"+pos+"-"+currentFileName+".jdbm");
          LOG.info("\tLoad back 1 JDBM file from tmp file uri:"+uri.toString());
          Path path = new Path(tmpURI+Path.SEPARATOR+"-"+pos+"-"+currentFileName+".jdbm");
          LOG.info("\tLoad back 1 JDBM file from tmp file uri:"+path.toString());

          File jdbmFile = new File(path.toUri());
          hashtable.initilizePersistentHash(jdbmFile);
        }
      }else{
        //load the jdbm file from distributed cache
        LOG.info("******* Load from distributed Cache ***:");
         Path[] localFiles= DistributedCache.getLocalCacheFiles(this.hconf);
         for(int i = 0;i<localFiles.length; i++){
           Path path = localFiles[i];
         }


         for(Map.Entry<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>> entry: mapJoinTables.entrySet()){
           pos = entry.getKey();
           hashtable=entry.getValue();
           String suffix="-"+pos+"-"+currentFileName+".jdbm";
           LOG.info("Looking for jdbm file with suffix: "+suffix);

           boolean found=false;
           for(int i = 0;i<localFiles.length; i++){
             Path path = localFiles[i];

             if(path.toString().endsWith(suffix)){
               LOG.info("Matching suffix with cached file:"+path.toString());
               File jdbmFile = new File(path.toString());
               LOG.info("\tInitializing the JDBM by cached file:"+path.toString());
               hashtable.initilizePersistentHash(jdbmFile);
               found = true;
               LOG.info("\tLoad back 1 JDBM file from distributed cache:"+path.toString());
               break;
             }
           }
           if(!found){
             LOG.error("Load nothing from Distributed Cache");
             throw new HiveException();
           }
         }
         LOG.info("******* End of loading *******:");

      }
    }catch (Exception e){
      e.printStackTrace();
      LOG.error("Load Hash Table error");

      throw new HiveException();
    }


  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {

    try {
      if(firstRow){
        //generate the map metadata
        generateMapMetaData();
        firstRow = false;
      }
      if(this.getExecContext().inputFileChanged()){
        loadJDBM();
      }

      // get alias
      alias = order[tag];
      //alias = (byte)tag;

      if ((lastAlias == null) || (!lastAlias.equals(alias))) {
        nextSz = joinEmitInterval;
      }

      // compute keys and values as StandardObjects
      ArrayList<Object> key = JoinUtil.computeKeys(row, joinKeys.get(alias),
          joinKeysObjectInspectors.get(alias));
      ArrayList<Object> value = JoinUtil.computeValues(row, joinValues.get(alias),
          joinValuesObjectInspectors.get(alias), joinFilters.get(alias),
          joinFilterObjectInspectors.get(alias), noOuterJoin);


      // Add the value to the ArrayList
      storage.get((byte)tag).add(value);

      for (Byte pos : order) {
        if (pos.intValue() != tag) {
          MapJoinObjectKey keyMap = new MapJoinObjectKey(metadataKeyTag, key);
          MapJoinObjectValue o = mapJoinTables.get(pos).getMapJoinValueObject(keyMap);

          // there is no join-value or join-key has all null elements
          if (o == null || (hasAnyNulls(key))) {
            if (noOuterJoin) {
              storage.put(pos, emptyList);
            } else {
              storage.put(pos, dummyObjVectors[pos.intValue()]);
            }
          } else {
            storage.put(pos, o.getObj());
          }
        }
      }

      // generate the output records
      checkAndGenObject();

      // done with the row
      storage.get((byte)tag).clear();

      for (Byte pos : order) {
        if (pos.intValue() != tag) {
          storage.put(pos, null);
        }
      }

    } catch (SerDeException e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }
  private String getFileName(String path){
    if(path== null || path.length()==0) {
      return null;
    }

    int last_separator = path.lastIndexOf(Path.SEPARATOR)+1;
    String fileName = path.substring(last_separator);
    return fileName;

  }
  @Override
  public void closeOp(boolean abort) throws HiveException {

    if(mapJoinTables != null) {
      for (HashMapWrapper hashTable : mapJoinTables.values()) {
        hashTable.close();
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
    return "MAPJOIN";
  }

  @Override
  public int getType() {
    return OperatorType.MAPJOIN;
  }
}
