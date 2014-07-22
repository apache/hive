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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class JoinUtil {

  public static List<ObjectInspector>[] getObjectInspectorsFromEvaluators(
      List<ExprNodeEvaluator>[] exprEntries,
      ObjectInspector[] inputObjInspector,
      int posBigTableAlias, int tagLen) throws HiveException {
    List<ObjectInspector>[] result = new List[tagLen];

    int iterate = Math.min(exprEntries.length, inputObjInspector.length);
    for (byte alias = 0; alias < iterate; alias++) {
      if (alias == (byte) posBigTableAlias ||
          exprEntries[alias] == null || inputObjInspector[alias] == null) {
        // skip the driver and directly loadable tables
        continue;
      }

      List<ExprNodeEvaluator> exprList = exprEntries[alias];
      List<ObjectInspector> fieldOIList = new ArrayList<ObjectInspector>();
      for (int i = 0; i < exprList.size(); i++) {
        fieldOIList.add(exprList.get(i).initialize(inputObjInspector[alias]));
      }
      result[alias] = fieldOIList;
    }
    return result;
  }


  public static List<ObjectInspector>[] getStandardObjectInspectors(
      List<ObjectInspector>[] aliasToObjectInspectors,
      int posBigTableAlias, int tagLen) {
    List<ObjectInspector>[] result = new List[tagLen];
    for (byte alias = 0; alias < aliasToObjectInspectors.length; alias++) {
      //get big table
      if(alias == (byte) posBigTableAlias || aliasToObjectInspectors[alias] == null){
        //skip the big tables
          continue;
      }

      List<ObjectInspector> oiList = aliasToObjectInspectors[alias];
      ArrayList<ObjectInspector> fieldOIList = new ArrayList<ObjectInspector>(
          oiList.size());
      for (int i = 0; i < oiList.size(); i++) {
        fieldOIList.add(ObjectInspectorUtils.getStandardObjectInspector(oiList
            .get(i), ObjectInspectorCopyOption.WRITABLE));
      }
      result[alias] = fieldOIList;
    }
    return result;

  }

  public static int populateJoinKeyValue(List<ExprNodeEvaluator>[] outMap,
      Map<Byte, List<ExprNodeDesc>> inputMap, int posBigTableAlias) throws HiveException {
    return populateJoinKeyValue(outMap, inputMap, null, posBigTableAlias);
  }

  public static int populateJoinKeyValue(List<ExprNodeEvaluator>[] outMap,
      Map<Byte, List<ExprNodeDesc>> inputMap,
      Byte[] order,
      int posBigTableAlias) throws HiveException {
    int total = 0;
    for (Entry<Byte, List<ExprNodeDesc>> e : inputMap.entrySet()) {
      if (e.getValue() == null) {
        continue;
      }
      Byte key = order == null ? e.getKey() : order[e.getKey()];
      List<ExprNodeEvaluator> valueFields = new ArrayList<ExprNodeEvaluator>();
      for (ExprNodeDesc expr : e.getValue()) {
        if (key == (byte) posBigTableAlias) {
          valueFields.add(null);
        } else {
          valueFields.add(ExprNodeEvaluatorFactory.get(expr));
        }
      }
      outMap[key] = valueFields;
      total += valueFields.size();
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
      nr[valueFields.size()] = new ShortWritable(isFiltered(row, filters, filtersOI, filterMap));
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
   * If it would be tagged by filter, reserve one more slot for that.
   * outValues can be passed in to avoid allocation
   */
  public static List<Object> computeValues(Object row,
      List<ExprNodeEvaluator> valueFields, List<ObjectInspector> valueFieldsOI, boolean hasFilter)
      throws HiveException {

    // Compute the values
    int reserve = hasFilter ? valueFields.size() + 1 : valueFields.size();
    List<Object> nr = new ArrayList<Object>(reserve);   
    for (int i = 0; i < valueFields.size(); i++) {
      nr.add(ObjectInspectorUtils.copyToStandardObject(valueFields.get(i)
          .evaluate(row), valueFieldsOI.get(i),
          ObjectInspectorCopyOption.WRITABLE));
    }
    return nr;
  }

  private static final short[] MASKS;
  static {
    int num = 32;
    MASKS = new short[num];
    MASKS[0] = 1;
    for (int idx = 1; idx < num; idx++) {
      MASKS[idx] = (short)(2 * MASKS[idx-1]);
    }
  }

  /**
   * Returns true if the row does not pass through filters.
   */
  protected static short isFiltered(Object row, List<ExprNodeEvaluator> filters,
      List<ObjectInspector> ois, int[] filterMap) throws HiveException {
    // apply join filters on the row.
    short ret = 0;
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

  protected static boolean isFiltered(short filter, int tag) {
    return (filter & MASKS[tag]) != 0;
  }

  protected static boolean hasAnyFiltered(short tag) {
    return tag != 0;
  }

  public static TableDesc getSpillTableDesc(Byte alias, TableDesc[] spillTableDesc,
      JoinDesc conf, boolean noFilter) {
    if (spillTableDesc == null || spillTableDesc.length == 0) {
      spillTableDesc = initSpillTables(conf,noFilter);
    }
    return spillTableDesc[alias];
  }

  public static SerDe getSpillSerDe(byte alias, TableDesc[] spillTableDesc,
      JoinDesc conf, boolean noFilter) {
    TableDesc desc = getSpillTableDesc(alias, spillTableDesc, conf, noFilter);
    if (desc == null) {
      return null;
    }
    SerDe sd = (SerDe) ReflectionUtils.newInstance(desc.getDeserializerClass(),
        null);
    try {
      SerDeUtils.initializeSerDe(sd, null, desc.getProperties(), null);
    } catch (SerDeException e) {
      e.printStackTrace();
      return null;
    }
    return sd;
  }

  public static TableDesc[] initSpillTables(JoinDesc conf, boolean noFilter) {
    int tagLen = conf.getTagLength();
    Map<Byte, List<ExprNodeDesc>> exprs = conf.getExprs();
    TableDesc[] spillTableDesc = new TableDesc[tagLen];
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
        colTypes.append(TypeInfoFactory.shortTypeInfo.getTypeName());
        colTypes.append(',');
      }
      // remove the last ','
      colNames.setLength(colNames.length() - 1);
      colTypes.setLength(colTypes.length() - 1);
      TableDesc tblDesc = new TableDesc(
          SequenceFileInputFormat.class, HiveSequenceFileOutputFormat.class,
          Utilities.makeProperties(
          org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT, ""
          + Utilities.ctrlaCode,
          org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS, colNames
          .toString(),
          org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES,
          colTypes.toString(),
          serdeConstants.SERIALIZATION_LIB,LazyBinarySerDe.class.getName()));
      spillTableDesc[tag] = tblDesc;
    }
    return spillTableDesc;
  }


  public static RowContainer<List<Object>> getRowContainer(Configuration hconf,
      List<ObjectInspector> structFieldObjectInspectors,
      Byte alias,int containerSize, TableDesc[] spillTableDesc,
      JoinDesc conf,boolean noFilter, Reporter reporter) throws HiveException {

    TableDesc tblDesc = JoinUtil.getSpillTableDesc(alias,spillTableDesc,conf, noFilter);
    SerDe serde = JoinUtil.getSpillSerDe(alias, spillTableDesc, conf, noFilter);

    if (serde == null) {
      containerSize = -1;
    }

    RowContainer<List<Object>> rc = new RowContainer<List<Object>>(containerSize, hconf, reporter);
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
