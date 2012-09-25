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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.persistence.AbstractMapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinDoubleKeys;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinSingleKey;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class JoinUtil {

  public static HashMap<Byte, List<ObjectInspector>> getObjectInspectorsFromEvaluators(
      Map<Byte, List<ExprNodeEvaluator>> exprEntries,
      ObjectInspector[] inputObjInspector,
      int posBigTableAlias) throws HiveException {
    HashMap<Byte, List<ObjectInspector>> result = new HashMap<Byte, List<ObjectInspector>>();
    for (Entry<Byte, List<ExprNodeEvaluator>> exprEntry : exprEntries
        .entrySet()) {
      Byte alias = exprEntry.getKey();
      //get big table
      if(alias == (byte) posBigTableAlias){
        //skip the big tables
          continue;
      }

      List<ExprNodeEvaluator> exprList = exprEntry.getValue();
      ArrayList<ObjectInspector> fieldOIList = new ArrayList<ObjectInspector>();
      for (int i = 0; i < exprList.size(); i++) {
        fieldOIList.add(exprList.get(i).initialize(inputObjInspector[alias]));
      }
      result.put(alias, fieldOIList);
    }
    return result;
  }


  public static HashMap<Byte, List<ObjectInspector>> getStandardObjectInspectors(
      Map<Byte, List<ObjectInspector>> aliasToObjectInspectors,
      int posBigTableAlias) {
    HashMap<Byte, List<ObjectInspector>> result = new HashMap<Byte, List<ObjectInspector>>();
    for (Entry<Byte, List<ObjectInspector>> oiEntry : aliasToObjectInspectors
        .entrySet()) {
      Byte alias = oiEntry.getKey();

      //get big table
      if(alias == (byte) posBigTableAlias ){
        //skip the big tables
          continue;
      }

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
  public static int populateJoinKeyValue(Map<Byte, List<ExprNodeEvaluator>> outMap,
      Map<Byte, List<ExprNodeDesc>> inputMap,
      Byte[] order,
      int posBigTableAlias) {

    int total = 0;

    Iterator<Map.Entry<Byte, List<ExprNodeDesc>>> entryIter = inputMap
        .entrySet().iterator();
    while (entryIter.hasNext()) {
      Map.Entry<Byte, List<ExprNodeDesc>> e = entryIter.next();
      Byte key = order[e.getKey()];

      List<ExprNodeEvaluator> valueFields = new ArrayList<ExprNodeEvaluator>();

      List<ExprNodeDesc> expr = e.getValue();
      int sz = expr.size();
      total += sz;

      for (int j = 0; j < sz; j++) {
        if(key == (byte) posBigTableAlias){
          valueFields.add(null);
        }else{
          valueFields.add(ExprNodeEvaluatorFactory.get(expr.get(j)));
        }
      }

      outMap.put(key, valueFields);
    }

    return total;
  }


  /**
   * Return the key as a standard object. StandardObject can be inspected by a
   * standard ObjectInspector.
   */
  public static ArrayList<Object> computeKeys(Object row,
      List<ExprNodeEvaluator> keyFields, List<ObjectInspector> keyFieldsOI)
      throws HiveException {

    // Compute the keys
    ArrayList<Object> nr = new ArrayList<Object>(keyFields.size());
    for (int i = 0; i < keyFields.size(); i++) {

      nr.add(ObjectInspectorUtils.copyToStandardObject(keyFields.get(i)
          .evaluate(row), keyFieldsOI.get(i),
          ObjectInspectorCopyOption.WRITABLE));
    }

    return nr;
  }

  /**
   * Return the key as a standard object. StandardObject can be inspected by a
   * standard ObjectInspector.
   */
  public static AbstractMapJoinKey computeMapJoinKeys(Object row,
      List<ExprNodeEvaluator> keyFields, List<ObjectInspector> keyFieldsOI)
      throws HiveException {

    int size = keyFields.size();
    if(size == 1){
      Object obj = (ObjectInspectorUtils.copyToStandardObject(keyFields.get(0)
          .evaluate(row), keyFieldsOI.get(0),
          ObjectInspectorCopyOption.WRITABLE));
      MapJoinSingleKey key = new MapJoinSingleKey(obj);
      return key;
    }else if(size == 2){
      Object obj1 = (ObjectInspectorUtils.copyToStandardObject(keyFields.get(0)
          .evaluate(row), keyFieldsOI.get(0),
          ObjectInspectorCopyOption.WRITABLE));

      Object obj2 = (ObjectInspectorUtils.copyToStandardObject(keyFields.get(1)
          .evaluate(row), keyFieldsOI.get(1),
          ObjectInspectorCopyOption.WRITABLE));

      MapJoinDoubleKeys key = new MapJoinDoubleKeys(obj1,obj2);
      return key;
    }else{
      // Compute the keys
      Object[] nr = new Object[keyFields.size()];
      for (int i = 0; i < keyFields.size(); i++) {

        nr[i] = (ObjectInspectorUtils.copyToStandardObject(keyFields.get(i)
            .evaluate(row), keyFieldsOI.get(i),
            ObjectInspectorCopyOption.WRITABLE));
      }
      MapJoinObjectKey key = new MapJoinObjectKey(nr);
      return key;
      }
  }





  /**
   * Return the value as a standard object. StandardObject can be inspected by a
   * standard ObjectInspector.
   */
  public static Object[] computeMapJoinValues(Object row,
      List<ExprNodeEvaluator> valueFields, List<ObjectInspector> valueFieldsOI,
      List<ExprNodeEvaluator> filters, List<ObjectInspector> filtersOI,
      int[] filterMap) throws HiveException {

    // Compute the keys
    Object[] nr;
    if (filterMap != null) {
      nr = new Object[valueFields.size()+1];
      // add whether the row is filtered or not.
      nr[valueFields.size()] = new ByteWritable(isFiltered(row, filters, filtersOI, filterMap));
    }else{
      nr = new Object[valueFields.size()];
    }

    for (int i = 0; i < valueFields.size(); i++) {
      nr[i] = ObjectInspectorUtils.copyToStandardObject(valueFields.get(i)
          .evaluate(row), valueFieldsOI.get(i),
          ObjectInspectorCopyOption.WRITABLE);
    }

    return nr;
  }

  /**
   * Return the value as a standard object. StandardObject can be inspected by a
   * standard ObjectInspector.
   */
  public static ArrayList<Object> computeValues(Object row,
      List<ExprNodeEvaluator> valueFields, List<ObjectInspector> valueFieldsOI,
      List<ExprNodeEvaluator> filters, List<ObjectInspector> filtersOI,
      int[] filterMap) throws HiveException {

    // Compute the values
    ArrayList<Object> nr = new ArrayList<Object>(valueFields.size());
    for (int i = 0; i < valueFields.size(); i++) {
      nr.add(ObjectInspectorUtils.copyToStandardObject(valueFields.get(i)
          .evaluate(row), valueFieldsOI.get(i),
          ObjectInspectorCopyOption.WRITABLE));
    }
    if (filterMap != null) {
      // add whether the row is filtered or not.
      nr.add(new ByteWritable(isFiltered(row, filters, filtersOI, filterMap)));
    }

    return nr;
  }

  private static final byte[] MASKS = new byte[]
      {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, (byte) 0x80};

  /**
   * Returns true if the row does not pass through filters.
   */
  protected static byte isFiltered(Object row, List<ExprNodeEvaluator> filters,
      List<ObjectInspector> ois, int[] filterMap) throws HiveException {
    // apply join filters on the row.
    byte ret = 0;
    int j = 0;
    for (int i = 0; i < filterMap.length; i += 2) {
      int tag = filterMap[i];
      int length = filterMap[i + 1];

      boolean passed = true;
      for (; length > 0; length--, j++) {
        if (passed) {
          Object condition = filters.get(j).evaluate(row);
          Boolean result = (Boolean) ((PrimitiveObjectInspector)
              ois.get(j)).getPrimitiveJavaObject(condition);
          if (result == null || !result) {
            passed = false;
          }
        }
      }
      if (!passed) {
        ret |= MASKS[tag];
      }
    }
    return ret;
  }

  protected static boolean isFiltered(byte filter, int tag) {
    return (filter & MASKS[tag]) != 0;
  }

  protected static boolean hasAnyFiltered(byte tag) {
    return tag != 0;
  }

  public static TableDesc getSpillTableDesc(Byte alias,
      Map<Byte, TableDesc> spillTableDesc,JoinDesc conf,
      boolean noFilter) {
    if (spillTableDesc == null || spillTableDesc.size() == 0) {
      spillTableDesc = initSpillTables(conf,noFilter);
    }
    return spillTableDesc.get(alias);
  }

  public static Map<Byte, TableDesc> getSpillTableDesc(
      Map<Byte, TableDesc> spillTableDesc,JoinDesc conf,
      boolean noFilter) {
    if (spillTableDesc == null) {
      spillTableDesc = initSpillTables(conf,noFilter);
    }
    return spillTableDesc;
  }

  public static SerDe getSpillSerDe(byte alias,
      Map<Byte, TableDesc> spillTableDesc,JoinDesc conf,
      boolean noFilter) {
    TableDesc desc = getSpillTableDesc(alias, spillTableDesc, conf, noFilter);
    if (desc == null) {
      return null;
    }
    SerDe sd = (SerDe) ReflectionUtils.newInstance(desc.getDeserializerClass(),
        null);
    try {
      sd.initialize(null, desc.getProperties());
    } catch (SerDeException e) {
      e.printStackTrace();
      return null;
    }
    return sd;
  }

  public static Map<Byte, TableDesc> initSpillTables(JoinDesc conf, boolean noFilter) {
    Map<Byte, List<ExprNodeDesc>> exprs = conf.getExprs();
    Map<Byte, TableDesc> spillTableDesc = new HashMap<Byte, TableDesc>(exprs.size());
    for (int tag = 0; tag < exprs.size(); tag++) {
      List<ExprNodeDesc> valueCols = exprs.get((byte) tag);
      int columnSize = valueCols.size();
      StringBuilder colNames = new StringBuilder();
      StringBuilder colTypes = new StringBuilder();
      if (columnSize <= 0) {
        continue;
      }
      for (int k = 0; k < columnSize; k++) {
        String newColName = tag + "_VALUE_" + k; // any name, it does not
        // matter.
        colNames.append(newColName);
        colNames.append(',');
        colTypes.append(valueCols.get(k).getTypeString());
        colTypes.append(',');
      }
      if (!noFilter) {
        colNames.append("filtered");
        colNames.append(',');
        colTypes.append(TypeInfoFactory.byteTypeInfo.getTypeName());
        colTypes.append(',');
      }
      // remove the last ','
      colNames.setLength(colNames.length() - 1);
      colTypes.setLength(colTypes.length() - 1);
      TableDesc tblDesc = new TableDesc(LazyBinarySerDe.class,
          SequenceFileInputFormat.class, HiveSequenceFileOutputFormat.class,
          Utilities.makeProperties(
          org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, ""
          + Utilities.ctrlaCode,
          org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS, colNames
          .toString(),
          org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES,
          colTypes.toString()));
      spillTableDesc.put((byte) tag, tblDesc);
    }
    return spillTableDesc;
  }


  public static RowContainer getRowContainer(Configuration hconf,
      List<ObjectInspector> structFieldObjectInspectors,
      Byte alias,int containerSize, Map<Byte, TableDesc> spillTableDesc,
      JoinDesc conf,boolean noFilter) throws HiveException {

    TableDesc tblDesc = JoinUtil.getSpillTableDesc(alias,spillTableDesc,conf, noFilter);
    SerDe serde = JoinUtil.getSpillSerDe(alias, spillTableDesc, conf, noFilter);

    if (serde == null) {
      containerSize = -1;
    }

    RowContainer rc = new RowContainer(containerSize, hconf);
    StructObjectInspector rcOI = null;
    if (tblDesc != null) {
      // arbitrary column names used internally for serializing to spill table
      List<String> colNames = Utilities.getColumnNames(tblDesc.getProperties());
      // object inspector for serializing input tuples
      rcOI = ObjectInspectorFactory.getStandardStructObjectInspector(colNames,
          structFieldObjectInspectors);
    }

    rc.setSerDe(serde, rcOI);
    rc.setTableDesc(tblDesc);
    return rc;
  }
}
