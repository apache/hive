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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;


/**
 * Generic UDF for tuple array sort by desired field[s] with [ordering(ASC or DESC)]
 * <code>SORT_ARRAY_BY(array(obj1, obj2, obj3...),'f1','f2',..,['ASC','DESC'])</code>.
 *
 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF
 */
@Description(name = "sort_array_by", value = "_FUNC_(array(obj1, obj2,...),'f1','f2',...,['ASC','DESC']) - "
    + "Sorts the input tuple array in user specified order(ASC,DESC) by desired field[s] name"
    + " If sorting order is not mentioned by user then dafault sorting order is ascending",
    extended = "Example:\n"
        + "  > SELECT _FUNC_(array(struct('g',100),struct('b',200)),'col1','ASC') FROM src LIMIT 1;\n"
        + " array(struct('b',200),struct('g',100)) ")
public class GenericUDFSortArrayByField extends GenericUDF {
  private transient Converter[] converters;
  private transient PrimitiveCategory[] inputTypes;
  /**Output array results*/
  private final List<Object> ret = new ArrayList<Object>();
  private transient ListObjectInspector listObjectInspector;
  private transient StructObjectInspector structObjectInspector;
  /**All sorting fields*/
  private transient StructField[] fields;
  /**Number of fields based on sorting will take place*/
  private transient int noOfInputFields;

  /**All possible ordering constants*/
  private enum SORT_ORDER_TYPE {
    ASC,
    DESC
  };
  /**default sorting order*/
  private transient SORT_ORDER_TYPE sortOrder = SORT_ORDER_TYPE.ASC;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

    /**This UDF requires minimum 2 arguments array_name,field name*/
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException("SORT_ARRAY_BY requires minimum 2 arguments, got " + arguments.length);
    }

    /**First argument must be array*/
    switch (arguments[0].getCategory()) {
      case LIST:
        listObjectInspector = (ListObjectInspector) arguments[0];
        break;
      default:
        throw new UDFArgumentTypeException(0, "Argument 1 of function SORT_ARRAY_BY must be "
            + serdeConstants.LIST_TYPE_NAME + ", but "
            + arguments[0].getTypeName() + " was found.");
    }

    /**Elements inside first argument(array) must be tuple(s)*/
    switch (listObjectInspector.getListElementObjectInspector().getCategory()) {
      case STRUCT:
        structObjectInspector = (StructObjectInspector) listObjectInspector.getListElementObjectInspector();
        break;
      default:
        throw new UDFArgumentTypeException(0, "Element[s] of first argument array in function SORT_ARRAY_BY must be "
            + serdeConstants.STRUCT_TYPE_NAME + ", but " + listObjectInspector.getTypeName() + " was found.");
    }

    /**All sort fields argument name and sort order name must be in String type*/
    converters = new Converter[arguments.length];
    inputTypes = new PrimitiveCategory[arguments.length];
    fields = new StructField[arguments.length - 1];
    noOfInputFields = arguments.length - 1;
    for (int i = 1; i < arguments.length; i++) {
      checkArgPrimitive(arguments, i);
      checkArgGroups(arguments, i, inputTypes, PrimitiveGrouping.STRING_GROUP);
      if (arguments[i] instanceof ConstantObjectInspector) {
        String fieldName = getConstantStringValue(arguments, i);
        /**checking whether any sorting order (ASC,DESC) has specified in last argument*/
        if (i != 1
            && (i == arguments.length - 1)
            && (fieldName.trim().toUpperCase().equals(SORT_ORDER_TYPE.ASC.name()) || fieldName.trim().toUpperCase()
                .equals(SORT_ORDER_TYPE.DESC.name()))) {
          sortOrder = SORT_ORDER_TYPE.valueOf(fieldName.trim().toUpperCase());
          noOfInputFields -= 1;
          continue;
        }
        fields[i - 1] = structObjectInspector.getStructFieldRef(getConstantStringValue(arguments, i));
      }
      obtainStringConverter(arguments, i, inputTypes, converters);
    }

    ObjectInspector returnOI = returnOIResolver.get(structObjectInspector);
    converters[0] = ObjectInspectorConverters.getConverter(structObjectInspector, returnOI);
    return ObjectInspectorFactory.getStandardListObjectInspector(structObjectInspector);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }

    /**Except first argument all remaining are field names and [sorting order]*/

    /**Add all non constant string tuple fields based on which sorting will happen with sorting ordering information if any.*/
    String field = null;

    /**If sorting order is set in initialize method then we are excluding last argument  */
    for (int i = 0; i < noOfInputFields && fields[i] == null; i++) {
      field = getStringValue(arguments, i + 1, converters);
      if (i != 0
          && (i == arguments.length - 2)
          && (field.trim().toUpperCase().equals(SORT_ORDER_TYPE.ASC.name()) || field.trim().toUpperCase()
              .equals(SORT_ORDER_TYPE.DESC.name()))) {
        noOfInputFields -= 1;
        sortOrder = SORT_ORDER_TYPE.valueOf(field.trim().toUpperCase());
        continue;
      }
      fields[i] = structObjectInspector.getStructFieldRef(field);
    }

    Object array = arguments[0].get();
    List<Object> retArray = (List<Object>) listObjectInspector.getList(array);

    /**Sort the tuple*/
    Collections.sort(retArray, new Comparator<Object>() {

      @Override
      public int compare(Object object1, Object object2) {
        int result = 0;
        /**If multiple fields are mentioned for sorting a record then inside the loop we do will do sorting for each field*/
        for (int i = 0; i < noOfInputFields; i++) {
          Object o1 = structObjectInspector.getStructFieldData(object1, fields[i]);
          Object o2 = structObjectInspector.getStructFieldData(object2, fields[i]);
          result =
              ObjectInspectorUtils.compare(o1, fields[i].getFieldObjectInspector(), o2,
                  fields[i].getFieldObjectInspector());
          if (result != 0) {
            /**Ordering*/
            if (sortOrder == SORT_ORDER_TYPE.DESC) {
              result *= -1;
            }
            return result;
          }
        }
        return result;
      }
    });

    ret.clear();
    for (int i = 0; i < retArray.size(); i++) {
      ret.add(converters[0].convert(retArray.get(i)));
    }
    return ret;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("sort_array_by", children);
  }

}
