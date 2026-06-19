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

package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.BaseStructObjectInspector;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.JobConf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleHBaseKeyFactory2 extends AbstractHBaseKeyFactory {

  private static final int FIXED_LENGTH = 10;

  @Override
  public ObjectInspector createKeyObjectInspector(TypeInfo type) {
    return new StringArrayOI((StructTypeInfo)type);
  }

  @Override
  public LazyObjectBase createKey(ObjectInspector inspector) throws SerDeException {
    return new FixedLengthed(FIXED_LENGTH);
  }

  private final ByteStream.Output output = new ByteStream.Output();

  @Override
  public byte[] serializeKey(Object object, StructField field) throws IOException {
    ObjectInspector inspector = field.getFieldObjectInspector();
    if (inspector.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new IllegalStateException("invalid type value " + inspector.getTypeName());
    }
    output.reset();
    for (Object element : ((StructObjectInspector)inspector).getStructFieldsDataAsList(object)) {
      output.write(toBinary(String.valueOf(element).getBytes(), FIXED_LENGTH, false, false));
    }
    return output.getLength() > 0 ? output.toByteArray() : null;
  }

  private byte[] toBinary(String value, int max, boolean end, boolean nextBA) {
    return toBinary(value.getBytes(), max, end, nextBA);
  }

  private byte[] toBinary(byte[] value, int max, boolean end, boolean nextBA) {
    byte[] bytes = new byte[max + 1];
    System.arraycopy(value, 0, bytes, 0, Math.min(value.length, max));
    if (end) {
      Arrays.fill(bytes, value.length, max, (byte)0xff);
    }
    if (nextBA) {
      bytes[max] = 0x01;
    }
    return bytes;
  }

  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer,
      ExprNodeDesc predicate) {
    String keyColName = keyMapping.columnName;

    IndexPredicateAnalyzer analyzer = IndexPredicateAnalyzer.createAnalyzer(false);
    analyzer.allowColumnName(keyColName);
    analyzer.setAcceptsFields(true);

    DecomposedPredicate decomposed = new DecomposedPredicate();

    List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
    decomposed.residualPredicate =
        (ExprNodeGenericFuncDesc)analyzer.analyzePredicate(predicate, searchConditions);
    if (!searchConditions.isEmpty()) {
      decomposed.pushedPredicate = analyzer.translateSearchConditions(searchConditions);
      try {
        decomposed.pushedPredicateObject = setupFilter(keyColName, searchConditions);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return decomposed;
  }

  private HBaseScanRange setupFilter(String keyColName, List<IndexSearchCondition> conditions)
      throws IOException {
    Map<String, List<IndexSearchCondition>> fieldConds =
        new HashMap<String, List<IndexSearchCondition>>();
    for (IndexSearchCondition condition : conditions) {
      assert keyColName.equals(condition.getColumnDesc().getColumn());
      String fieldName = condition.getFields()[0];
      List<IndexSearchCondition> fieldCond = fieldConds.get(fieldName);
      if (fieldCond == null) {
        fieldConds.put(fieldName, fieldCond = new ArrayList<IndexSearchCondition>());
      }
      fieldCond.add(condition);
    }
    HBaseScanRange range = new HBaseScanRange();

    ByteArrayOutputStream startRow = new ByteArrayOutputStream();
    ByteArrayOutputStream stopRow = new ByteArrayOutputStream();

    StructTypeInfo type = (StructTypeInfo) keyMapping.columnType;
    for (String name : type.getAllStructFieldNames()) {
      List<IndexSearchCondition> fieldCond = fieldConds.get(name);
      if (fieldCond == null || fieldCond.size() > 2) {
        continue;
      }
      byte[] startElement = null;
      byte[] stopElement = null;
      for (IndexSearchCondition condition : fieldCond) {
        if (condition.getConstantDesc().getValue() == null) {
          continue;
        }
        String comparisonOp = condition.getComparisonOp();
        String constantVal = String.valueOf(condition.getConstantDesc().getValue());

        if (comparisonOp.endsWith("UDFOPEqual")) {
          startElement = toBinary(constantVal, FIXED_LENGTH, false, false);
          stopElement = toBinary(constantVal, FIXED_LENGTH, true, true);
        } else if (comparisonOp.endsWith("UDFOPEqualOrGreaterThan")) {
          startElement = toBinary(constantVal, FIXED_LENGTH, false, false);
        } else if (comparisonOp.endsWith("UDFOPGreaterThan")) {
          startElement = toBinary(constantVal, FIXED_LENGTH, false, true);
        } else if (comparisonOp.endsWith("UDFOPEqualOrLessThan")) {
          stopElement = toBinary(constantVal, FIXED_LENGTH, true, false);
        } else if (comparisonOp.endsWith("UDFOPLessThan")) {
          stopElement = toBinary(constantVal, FIXED_LENGTH, true, true);
        } else {
          throw new IOException(comparisonOp + " is not a supported comparison operator");
        }
      }
      if (startRow != null) {
        if (startElement != null) {
          startRow.write(startElement);
        } else {
          if (startRow.size() > 0) {
            range.setStartRow(startRow.toByteArray());
          }
          startRow = null;
        }
      }
      if (stopRow != null) {
        if (stopElement != null) {
          stopRow.write(stopElement);
        } else {
          if (stopRow.size() > 0) {
            range.setStopRow(stopRow.toByteArray());
          }
          stopRow = null;
        }
      }
      if (startElement == null && stopElement == null) {
        break;
      }
    }
    if (startRow != null && startRow.size() > 0) {
      range.setStartRow(startRow.toByteArray());
    }
    if (stopRow != null && stopRow.size() > 0) {
      range.setStopRow(stopRow.toByteArray());
    }
    return range;
  }

  private static class FixedLengthed implements LazyObjectBase {

    private final int fixedLength;
    private final List<Object> fields = new ArrayList<Object>();

    private transient boolean isNull;

    public FixedLengthed(int fixedLength) {
      this.fixedLength = fixedLength;
    }

    @Override
    public void init(ByteArrayRef bytes, int start, int length) {
      fields.clear();
      byte[] data = bytes.getData();
      int rowStart = start;
      int rowStop = rowStart + fixedLength;
      for (; rowStart < length; rowStart = rowStop + 1, rowStop = rowStart + fixedLength) {
        fields.add(new String(data, rowStart, rowStop - rowStart).trim());
      }
      isNull = false;
    }

    @Override
    public void setNull() {
      isNull = true;
    }

    @Override
    public Object getObject() {
      return isNull ? null : this;
    }
  }

  private static class StringArrayOI extends BaseStructObjectInspector {

    private int length;

    private StringArrayOI(StructTypeInfo type) {
      List<String> names = type.getAllStructFieldNames();
      List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
      for (int i = 0; i < names.size(); i++) {
        ois.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      }
      init(names, ois, null);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
      return ((FixedLengthed)data).fields.get(((MyField)fieldRef).getFieldID());
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object data) {
      return ((FixedLengthed)data).fields;
    }
  }
}
