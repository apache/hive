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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TextConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

public abstract class GenericUDFBaseTrim extends GenericUDF {

  public static final String DEFAULT_TRIM_CHARS = " ";

  private transient TextConverter stringToTrimConverter;
  private transient TextConverter trimCharsConverter;
  private Text result = new Text();
  private String udfName;

  public GenericUDFBaseTrim(String _udfName) {
    this.udfName = _udfName;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentException(udfName + " requires at least one value argument. Found :"
              + arguments.length);
    }
    if (arguments.length > 2) {
      throw new UDFArgumentException(udfName + " requires no more than two value arguments. Found :"
        + arguments.length);
    }
    stringToTrimConverter = new TextConverter(getArgumentObjectInspector(arguments[0]));
    if (arguments.length == 2) {
      trimCharsConverter = new TextConverter(getArgumentObjectInspector(arguments[1]));
    } else {
      trimCharsConverter = null;
    }

    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  private PrimitiveObjectInspector getArgumentObjectInspector(ObjectInspector argument) throws UDFArgumentException {
    PrimitiveObjectInspector argumentOI;
    if (argument instanceof PrimitiveObjectInspector) {
      argumentOI = (PrimitiveObjectInspector) argument;
    } else {
      throw new UDFArgumentException(udfName + " takes only primitive types. found "
              + argument.getTypeName());
    }
    switch (argumentOI.getPrimitiveCategory()) {
      case STRING:
      case CHAR:
      case VARCHAR:
      case VOID:
        break;
      default:
        throw new UDFArgumentException(udfName + " takes only STRING/CHAR/VARCHAR types. Found "
                + argumentOI.getPrimitiveCategory());
    }
    return argumentOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object valObject = arguments[0].get();
    if (valObject == null) {
      return null;
    }
    String val = stringToTrimConverter.convert(valObject).toString();
    if (val == null) {
      return null;
    }

    String trimChars = DEFAULT_TRIM_CHARS;
    if (trimCharsConverter != null && arguments.length == 2) {
      Object trimCharsObject = arguments[1].get();
      if (trimCharsObject == null) {
        return null;
      }
      trimChars = trimCharsConverter.convert(trimCharsObject).toString();
      if (trimChars == null) {
        return null;
      }
    }

    result.set(performOp(val, trimChars));
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(udfName, children);
  }

  protected abstract String performOp(String val, String trimChars);

}
