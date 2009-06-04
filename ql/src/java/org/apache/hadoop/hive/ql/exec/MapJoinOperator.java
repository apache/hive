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
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.mapJoinDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hive.ql.util.jdbm.htree.HTree;
import org.apache.hadoop.hive.ql.util.jdbm.RecordManager;
import org.apache.hadoop.hive.ql.util.jdbm.RecordManagerFactory;
import org.apache.hadoop.hive.ql.util.jdbm.RecordManagerOptions;

/**
 * Map side Join operator implementation.
 */
public class MapJoinOperator extends CommonJoinOperator<mapJoinDesc> implements Serializable {
  private static final long serialVersionUID = 1L;
  static final private Log LOG = LogFactory.getLog(MapJoinOperator.class.getName());

  transient protected Map<Byte, List<ExprNodeEvaluator>> joinKeys;
  transient protected Map<Byte, List<ObjectInspector>> joinKeysObjectInspectors;

  transient private int posBigTable;       // one of the tables that is not in memory
  transient int mapJoinRowsKey;            // rows for a given key
  
  transient protected Map<Byte, HTree> mapJoinTables;

  public static class MapJoinObjectCtx {
    ObjectInspector serObjInspector;
    Serializer      serializer;
    Deserializer    deserializer;
    ObjectInspector deserObjInspector;
    
    /**
     * @param serObjInspector
     * @param serializer
     * @param deserializer
     * @param deserObjInspector
     */
    public MapJoinObjectCtx(ObjectInspector serObjInspector,
        Serializer serializer, ObjectInspector deserObjInspector, Deserializer deserializer) {
      this.serObjInspector = serObjInspector;
      this.serializer = serializer;
      this.deserializer = deserializer;
      this.deserObjInspector = deserObjInspector;
    }
    
    /**
     * @return the objInspector
     */
    public ObjectInspector getSerObjInspector() {
      return serObjInspector;
    }

    /**
     * @return the objInspector
     */
    public ObjectInspector getDeserObjInspector() {
      return deserObjInspector;
    }
    
    /**
     * @return the serializer
     */
    public Serializer getSerializer() {
      return serializer;
    }

    /**
     * @return the deserializer
     */
    public Deserializer getDeserializer() {
      return deserializer;
    }

  }

  transient static Map<Integer, MapJoinObjectCtx> mapMetadata = new HashMap<Integer, MapJoinObjectCtx>();
  transient static int nextVal = 0;
  
  static public Map<Integer, MapJoinObjectCtx> getMapMetadata() {
    return mapMetadata;
  }
  
  transient boolean firstRow;
  
  transient int   metadataKeyTag;
  transient int[] metadataValueTag;
  transient List<File> hTables;
  
  @Override
  public void initializeOp(Configuration hconf, Reporter reporter, ObjectInspector[] inputObjInspector) throws HiveException {
    super.initializeOp(hconf, reporter, inputObjInspector);
    firstRow = true;
    try {
      joinKeys  = new HashMap<Byte, List<ExprNodeEvaluator>>();
      joinKeysObjectInspectors = new HashMap<Byte, List<ObjectInspector>>();
      
      populateJoinKeyValue(joinKeys, conf.getKeys());
      
      // all other tables are small, and are cached in the hash table
      posBigTable = conf.getPosBigTable();

      metadataValueTag = new int[numValues];
      for (int pos = 0; pos < numValues; pos++)
        metadataValueTag[pos] = -1;
      
      mapJoinTables = new HashMap<Byte, HTree>();
      hTables = new ArrayList<File>();
      
      // initialize the hash tables for other tables
      for (int pos = 0; pos < numValues; pos++) {
        if (pos == posBigTable)
          continue;
        
        Properties props = new Properties();
        props.setProperty(RecordManagerOptions.CACHE_SIZE, 
          String.valueOf(HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEMAPJOINCACHEROWS)));
        
        Random rand = new Random();
        File newDir = new File("/tmp/" + rand.nextInt());
        String newDirName = null;
        while (true) {
          if (newDir.mkdir()) {
            newDirName = newDir.getAbsolutePath();
            hTables.add(newDir);
            break;
          }
          newDir = new File("/tmp" + rand.nextInt());
        }
        
        RecordManager recman = RecordManagerFactory.createRecordManager(newDirName + "/" + pos, props );
        HTree hashTable = HTree.createInstance(recman);
        
        mapJoinTables.put(new Byte((byte)pos), hashTable);
      }

      storage.put((byte)posBigTable, new Vector<ArrayList<Object>>());
      
      mapJoinRowsKey = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEMAPJOINROWSIZE);
      
      // initialize the join output object inspectors
      ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>(totalSz);

      for (Byte alias : order) {
        int sz = conf.getExprs().get(alias).size();
        List<? extends StructField> listFlds = ((StructObjectInspector)inputObjInspector[alias.intValue()]).getAllStructFieldRefs();
        assert listFlds.size() == sz;
        for (StructField fld: listFlds) {
          structFieldObjectInspectors.add(fld.getFieldObjectInspector());
        }
      }

      joinOutputObjectInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(ObjectInspectorUtils
              .getIntegerArray(totalSz), structFieldObjectInspectors);
      
      initializeChildren(hconf, reporter, new ObjectInspector[]{joinOutputObjectInspector});
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  @Override
  public void process(Object row, ObjectInspector rowInspector, int tag) throws HiveException {
    try {

      // get alias
      alias = (byte)tag;

      if ((lastAlias == null) || (!lastAlias.equals(alias)))
        nextSz = joinEmitInterval;
      
      // compute keys and values     
      ArrayList<Object> key   = computeValues(row, rowInspector, joinKeys.get(alias), joinKeysObjectInspectors);
      ArrayList<Object> value = computeValues(row, rowInspector, joinValues.get(alias), joinValuesObjectInspectors);

      // Until there is one representation for the keys, convert explicitly
      int keyPos = 0;
      // TODO: use keyPos instead
      for (Object keyElem : key) {
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector)joinKeysObjectInspectors.get(alias).get(keyPos);
        if (!poi.isWritable()) {
          // convert o to writable
          key.set(keyPos, ObjectInspectorUtils.copyToStandardObject(key.get(keyPos), poi, ObjectInspectorCopyOption.WRITABLE));
        }
        keyPos++;
      }

      // does this source need to be stored in the hash map
      if (tag != posBigTable) {
        if (firstRow) {
          metadataKeyTag = nextVal++;
          
          tableDesc keyTableDesc = conf.getKeyTblDesc();
          Serializer keySerializer = (Serializer)keyTableDesc.getDeserializerClass().newInstance();
          keySerializer.initialize(null, keyTableDesc.getProperties());

          ExprNodeEvaluator[] keyEval = new ExprNodeEvaluator[conf.getKeys().get(new Byte((byte)tag)).size()];
          int i=0;
          for (exprNodeDesc e: conf.getKeys().get(new Byte((byte)tag))) {
            keyEval[i++] = ExprNodeEvaluatorFactory.get(e);
          }

          ObjectInspector keyObjectInspector = initEvaluatorsAndReturnStruct(keyEval, rowInspector);

          Deserializer deserializer = (Deserializer)ReflectionUtils.newInstance(keyTableDesc.getDeserializerClass(), null);
          deserializer.initialize(null, keyTableDesc.getProperties());
          
          mapMetadata.put(new Integer(metadataKeyTag), new MapJoinObjectCtx(keyObjectInspector, keySerializer, deserializer.getObjectInspector(), deserializer));
          
          firstRow = false;
        }
        
        HTree hashTable = mapJoinTables.get(alias);
        MapJoinObject keyMap = new MapJoinObject(metadataKeyTag, 1, key);
        MapJoinObject o = (MapJoinObject)hashTable.get(keyMap);
        Vector<ArrayList<Object>> res = null;
        
        if (o == null) {
          res = new Vector<ArrayList<Object>>();
        }
        else {
          res = (Vector<ArrayList<Object>>)o.getObj();
        }
        
        res.add(value);

        // TODO: put some warning if the size of res exceeds a given threshold
  

        if (metadataValueTag[tag] == -1) {
          metadataValueTag[tag] = nextVal++;
                    
          tableDesc valueTableDesc = conf.getValueTblDescs().get(tag);
          Serializer valueSerializer = (Serializer)valueTableDesc.getDeserializerClass().newInstance();
          valueSerializer.initialize(null, valueTableDesc.getProperties());

          ExprNodeEvaluator[] valueEval = new ExprNodeEvaluator[conf.getExprs().get(new Byte((byte)tag)).size()];
          int i=0;
          for (exprNodeDesc e: conf.getExprs().get(new Byte((byte)tag))) {
            valueEval[i++] = ExprNodeEvaluatorFactory.get(e);
          }

          ObjectInspector valueObjectInspector = initEvaluatorsAndReturnStruct(valueEval, rowInspector);
 
          Deserializer deserializer = (Deserializer)ReflectionUtils.newInstance(valueTableDesc.getDeserializerClass(), null);
          deserializer.initialize(null, valueTableDesc.getProperties());
          
          mapMetadata.put(new Integer((byte)metadataValueTag[tag]), new MapJoinObjectCtx(valueObjectInspector, valueSerializer, deserializer.getObjectInspector(), deserializer));
        }
        
        // Construct externalizable objects for key and value
        MapJoinObject keyObj = new MapJoinObject();
        
        // currently, key is always byteswritable and value is text - TODO: generalize this
        keyObj.setMetadataTag(metadataKeyTag);
        keyObj.setObjectTypeTag(1);
        keyObj.setObj(key);
        
        MapJoinObject valueObj = new MapJoinObject();
        
        valueObj.setMetadataTag(metadataValueTag[tag]);
        valueObj.setObjectTypeTag(2);
        valueObj.setObj(res);

        if (res.size() > 1)
          hashTable.remove(keyObj);

        hashTable.put(keyObj, valueObj);
        return;
      }


      // Add the value to the vector
      storage.get(alias).add(value);

      for (Byte pos : order) {
        if (pos.intValue() != tag) {
          MapJoinObject keyMap = new MapJoinObject(metadataKeyTag, 1, key);
          MapJoinObject o = (MapJoinObject)mapJoinTables.get(pos).get(keyMap);

          if (o == null) {
            storage.put(pos, new Vector<ArrayList<Object>>());
          }
          else {
            storage.put(pos, (Vector<ArrayList<Object>>)o.getObj());
          }
        }
      }
      
      // generate the output records
      checkAndGenObject();
    
      // done with the row
      storage.get(alias).clear();

      for (Byte pos : order)
        if (pos.intValue() != tag)
          storage.put(pos, null);
    
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }
  
  /**
   * Implements the getName function for the Node Interface.
   * @return the name of the operator
   */
  public String getName() {
    return new String("MAPJOIN");
  }
  
  public void close(boolean abort) throws HiveException {
    for (File hTbl : hTables) {
      deleteDir(hTbl);
    }
    super.close(abort);
  }
  
  public static void deleteDir(File dir) {
    if (dir.isDirectory()) {
      String[] children = dir.list();
      for (int i = 0; i < children.length; i++) {
        deleteDir(new File(dir, children[i]));
      }
    }

    dir.delete();
  }
}
