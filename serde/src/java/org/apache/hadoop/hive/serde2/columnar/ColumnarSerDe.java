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

package org.apache.hadoop.hive.serde2.columnar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

/**
 * ColumnarSerDe is used for columnar based storage supported by RCFile.
 * ColumnarSerDe differentiate from LazySimpleSerDe in:<br>
 * (1) ColumnarSerDe uses a ColumnarStruct as its lazy Object <br>
 * (2) ColumnarSerDe initialize ColumnarStruct's field directly. But under the
 * field level, it works like LazySimpleSerDe<br>
 */
@SerDeSpec(schemaProps = {
    serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
    serdeConstants.FIELD_DELIM, serdeConstants.COLLECTION_DELIM, serdeConstants.MAPKEY_DELIM,
    serdeConstants.SERIALIZATION_FORMAT, serdeConstants.SERIALIZATION_NULL_FORMAT,
    serdeConstants.SERIALIZATION_ESCAPE_CRLF,
    serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST,
    serdeConstants.ESCAPE_CHAR,
    serdeConstants.SERIALIZATION_ENCODING,
    LazySerDeParameters.SERIALIZATION_EXTEND_NESTING_LEVELS,
    LazySerDeParameters.SERIALIZATION_EXTEND_ADDITIONAL_NESTING_LEVELS
    })
public class ColumnarSerDe extends ColumnarSerDeBase {

  @Override
  public String toString() {
    return getClass().toString()
        + "["
        + Arrays.asList(serdeParams.getSeparators())
        + ":"
        + ((StructTypeInfo) serdeParams.getRowTypeInfo())
            .getAllStructFieldNames()
        + ":"
        + ((StructTypeInfo) serdeParams.getRowTypeInfo())
            .getAllStructFieldTypeInfos() + "]";
  }

  public static final Logger LOG = LoggerFactory
      .getLogger(ColumnarSerDe.class);

  public ColumnarSerDe() throws SerDeException {
  }

  protected LazySerDeParameters serdeParams = null;

  /**
   * Initialize the SerDe given the parameters.
   *
   * @see org.apache.hadoop.hive.serde2.AbstractSerDe#initialize(Configuration, Properties)
   */
  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {

    serdeParams = new LazySerDeParameters(conf, tbl, getClass().getName());

    // Create the ObjectInspectors for the fields. Note: Currently
    // ColumnarObject uses same ObjectInpector as LazyStruct
    cachedObjectInspector = LazyFactory.createColumnarStructInspector(
        serdeParams.getColumnNames(), serdeParams.getColumnTypes(), serdeParams);

    int size = serdeParams.getColumnTypes().size();
    List<Integer> notSkipIDs = new ArrayList<Integer>();
    if (conf == null || ColumnProjectionUtils.isReadAllColumns(conf)) {
      for (int i = 0; i < size; i++ ) {
        notSkipIDs.add(i);
      }
    } else {
      notSkipIDs = ColumnProjectionUtils.getReadColumnIDs(conf);
    }
    cachedLazyStruct = new ColumnarStruct(
        cachedObjectInspector, notSkipIDs, serdeParams.getNullSequence());

    super.initialize(size);
  }

  /**
   * Serialize a row of data.
   *
   * @param obj
   *          The row object
   * @param objInspector
   *          The ObjectInspector for the row object
   * @return The serialized Writable object
   * @see org.apache.hadoop.hive.serde2.AbstractSerDe#serialize(Object, ObjectInspector)
   */
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {

    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);
    List<? extends StructField> declaredFields = (serdeParams.getRowTypeInfo() != null && ((StructTypeInfo) serdeParams
        .getRowTypeInfo()).getAllStructFieldNames().size() > 0) ? ((StructObjectInspector) getObjectInspector())
        .getAllStructFieldRefs()
        : null;

    try {
      // used for avoid extra byte copy
      serializeStream.reset();
      serializedSize = 0;
      int count = 0;
      // Serialize each field
      for (int i = 0; i < fields.size(); i++) {
        // Get the field objectInspector and the field object.
        ObjectInspector foi = fields.get(i).getFieldObjectInspector();
        Object f = (list == null ? null : list.get(i));

        if (declaredFields != null && i >= declaredFields.size()) {
          throw new SerDeException("Error: expecting " + declaredFields.size()
              + " but asking for field " + i + "\n" + "data=" + obj + "\n"
              + "tableType=" + serdeParams.getRowTypeInfo().toString() + "\n"
              + "dataType="
              + TypeInfoUtils.getTypeInfoFromObjectInspector(objInspector));
        }

        // If the field that is passed in is NOT a primitive, and either the
        // field is not declared (no schema was given at initialization), or
        // the field is declared as a primitive in initialization, serialize
        // the data to JSON string. Otherwise serialize the data in the
        // delimited way.
        if (!foi.getCategory().equals(Category.PRIMITIVE)
            && (declaredFields == null || declaredFields.get(i)
                .getFieldObjectInspector().getCategory().equals(
                    Category.PRIMITIVE))) {
          LazySimpleSerDe.serialize(serializeStream, SerDeUtils.getJSONString(
              f, foi),
              PrimitiveObjectInspectorFactory.javaStringObjectInspector,
              serdeParams.getSeparators(), 1, serdeParams.getNullSequence(),
              serdeParams.isEscaped(), serdeParams.getEscapeChar(), serdeParams
                  .getNeedsEscape());
        } else {
          LazySimpleSerDe.serialize(serializeStream, f, foi, serdeParams
              .getSeparators(), 1, serdeParams.getNullSequence(), serdeParams
              .isEscaped(), serdeParams.getEscapeChar(), serdeParams
              .getNeedsEscape());
        }

        field[i].set(serializeStream.getData(), count, serializeStream
            .getLength()
            - count);
        count = serializeStream.getLength();
      }
      serializedSize = serializeStream.getLength();
      lastOperationSerialize = true;
      lastOperationDeserialize = false;
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    return serializeCache;
  }

}
