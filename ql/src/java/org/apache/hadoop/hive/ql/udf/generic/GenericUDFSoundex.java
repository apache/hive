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

import org.apache.commons.codec.language.Soundex;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFSoundex.
 *
 * Soundex is an encoding used to relate similar names, but can also be used as
 * a general purpose scheme to find word with similar phonemes.
 *
 */
@Description(name = "soundex", value = "_FUNC_(string) - Returns soundex code of the string.",
    extended = "The soundex code consist of the first letter of the name followed by three digits.\n"
    + "Example:\n" + " > SELECT _FUNC_('Miller');\n M460")
public class GenericUDFSoundex extends GenericUDF {
  private final transient Converter[] textConverters = new Converter[1];
  private final transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];
  private final transient Soundex soundex = new Soundex();
  private final Text output = new Text();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(getFuncName() + " requires 1 argument, got "
          + arguments.length);
    }
    checkIfPrimitive(arguments, 0, "1st");

    checkIfStringGroup(arguments, 0, "1st");

    getStringConverter(arguments, 0, "1st");

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object obj0;
    if ((obj0 = arguments[0].get()) == null) {
      return null;
    }

    String str0 = textConverters[0].convert(obj0).toString();
    String soundexCode;
    try {
      soundexCode = soundex.soundex(str0);
    } catch (IllegalArgumentException e) {
      return null;
    }
    output.set(soundexCode);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  protected void checkIfPrimitive(ObjectInspector[] arguments, int i, String argOrder)
      throws UDFArgumentTypeException {
    ObjectInspector.Category oiCat = arguments[i].getCategory();
    if (oiCat != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(i, getFuncName() + " only takes primitive types as "
          + argOrder + " argument, got " + oiCat);
    }
  }

  protected void checkIfStringGroup(ObjectInspector[] arguments, int i, String argOrder)
      throws UDFArgumentTypeException {
    inputTypes[i] = ((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory();
    if (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(inputTypes[i]) != PrimitiveGrouping.STRING_GROUP &&
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(inputTypes[i]) != PrimitiveGrouping.VOID_GROUP) {
      throw new UDFArgumentTypeException(i, getFuncName() + " only takes STRING_GROUP types as "
          + argOrder + " argument, got " + inputTypes[i]);
    }
  }

  protected void getStringConverter(ObjectInspector[] arguments, int i, String argOrder)
      throws UDFArgumentTypeException {
    textConverters[i] = ObjectInspectorConverters.getConverter(
        arguments[i],
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
  }

  @Override
  protected String getFuncName() {
    return "soundex";
  }
}
