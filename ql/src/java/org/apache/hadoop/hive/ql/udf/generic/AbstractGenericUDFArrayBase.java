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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

/**
 * Abstract GenericUDF for array functions
 */

public abstract class AbstractGenericUDFArrayBase extends GenericUDF {

    static final int ARRAY_IDX = 0;

    private final int minArgCount;
    private final int maxArgCount;
    private final ObjectInspector.Category outputCategory;

    private final String functionName;

    transient ListObjectInspector arrayOI;
    transient ObjectInspector[] argumentOIs;

    transient Converter converter;

    protected AbstractGenericUDFArrayBase(String functionName, int minArgCount, int maxArgCount, ObjectInspector.Category outputCategory) {
        this.functionName = functionName;
        this.minArgCount = minArgCount;
        this.maxArgCount = maxArgCount;
        this.outputCategory = outputCategory;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {

        // Check if wrong number of arguments were passed
        checkArgsSize(arguments, minArgCount, maxArgCount);

        // Check if the argument is of category LIST or not
        checkArgCategory(arguments, ARRAY_IDX, ObjectInspector.Category.LIST, functionName,
                org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME);

        //return ObjectInspectors based on expected output type
        arrayOI = (ListObjectInspector) arguments[ARRAY_IDX];
        argumentOIs = arguments;
        if (outputCategory == ObjectInspector.Category.LIST) {
            return initListOI(arguments);
        } else {
            return initOI(arguments);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(functionName.toLowerCase(), children);
    }

    void checkArgCategory(ObjectInspector[] arguments, int idx, ObjectInspector.Category category,
                          String functionName, String typeName) throws UDFArgumentTypeException {

        if (!arguments[idx].getCategory().equals(category)) {
            throw new UDFArgumentTypeException(idx,
                    "\"" + typeName + "\" "
                            + "expected at function " + functionName + ", but "
                            + "\"" + arguments[idx].getTypeName() + "\" "
                            + "is found");
        }
    }

    void checkArgIntPrimitiveCategory(PrimitiveObjectInspector objectInspector, String functionName, int idx)
        throws UDFArgumentTypeException {
      switch (objectInspector.getPrimitiveCategory()) {
      case SHORT:
      case INT:
      case LONG:
        break;
      default:
        throw new UDFArgumentTypeException(0,
            "Argument " + idx + " of function " + functionName + " must be \"" + serdeConstants.SMALLINT_TYPE_NAME + "\""
                + " or \"" + serdeConstants.INT_TYPE_NAME + "\"" + " or \"" + serdeConstants.BIGINT_TYPE_NAME
                + "\", but \"" + objectInspector.getTypeName() + "\" was found.");
      }
    }

    ObjectInspector initOI(ObjectInspector[] arguments) {

        GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver =
                new GenericUDFUtils.ReturnObjectInspectorResolver(true);

        ObjectInspector elementObjectInspector =
                ((ListObjectInspector) (arguments[0])).getListElementObjectInspector();

        ObjectInspector returnOI = returnOIResolver.get(elementObjectInspector);
        converter = ObjectInspectorConverters.getConverter(elementObjectInspector, returnOI);
        return returnOI;
    }

    ObjectInspector initListOI(ObjectInspector[] arguments) {
        return ObjectInspectorFactory.getStandardListObjectInspector(initOI(arguments));
    }

}
