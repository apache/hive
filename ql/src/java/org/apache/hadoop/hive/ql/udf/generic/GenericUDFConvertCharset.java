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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@Description(name = "convertCharset", value = "_FUNC_(str, str, str) - Converts the first argument from the second argument character set to the third argument character set", extended =
    "Possible options for the character set are 'US-ASCII', 'ISO-8859-1',\n"
        + "'UTF-8', 'UTF-16BE', 'UTF-16LE', and 'UTF-16'. If either argument\n"
        + "is null, the result will also be null") public class GenericUDFConvertCharset extends GenericUDF {
  private transient CharsetEncoder encoder = null;
  private transient CharsetDecoder decoder = null;
  private transient PrimitiveObjectInspector stringOI = null;
  private transient PrimitiveObjectInspector fromCharsetOI = null;
  private transient PrimitiveObjectInspector toCharsetOI = null;

  @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 3) {
      throw new UDFArgumentLengthException("ConvertCharset() requires exactly three arguments");
    }

    checkInputArgument(arguments, 0);
    stringOI = (PrimitiveObjectInspector) arguments[0];

    checkInputArgument(arguments, 1);
    fromCharsetOI = (PrimitiveObjectInspector) arguments[1];

    checkInputArgument(arguments, 2);
    toCharsetOI = (PrimitiveObjectInspector) arguments[2];

    // If the character set for encoding is constant, we can optimize that
    if (fromCharsetOI instanceof ConstantObjectInspector) {
      String charSetName = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue().toString();
      encoder = Charset.forName(charSetName).newEncoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT);
    }

    // If the character set for decoding is constant, we can optimize that
    if (toCharsetOI instanceof ConstantObjectInspector) {
      String charSetName = ((ConstantObjectInspector) arguments[2]).getWritableConstantValue().toString();
      decoder = Charset.forName(charSetName).newDecoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT);
    }

    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String value = PrimitiveObjectInspectorUtils.getString(arguments[0].get(), stringOI);
    if (value == null) {
      return null;
    }

    ByteBuffer encoded;
    if (encoder != null) {
      try {
        encoded = encoder.encode(CharBuffer.wrap(value));
      } catch (CharacterCodingException e) {
        throw new HiveException(e);
      }
    } else {
      encoded =
          Charset.forName(PrimitiveObjectInspectorUtils.getString(arguments[1].get(), fromCharsetOI)).encode(value);
    }

    CharBuffer decoded;
    if (decoder != null) {
      try {
        decoded = decoder.decode(encoded);
      } catch (CharacterCodingException e) {
        throw new HiveException(e);
      }
    } else {
      decoded =
          Charset.forName(PrimitiveObjectInspectorUtils.getString(arguments[2].get(), toCharsetOI)).decode(encoded);
    }
    return decoded.toString();
  }

  @Override public String getDisplayString(String[] children) {
    assert (children.length == 3);
    return getStandardDisplayString("convertCharset", children, ",");
  }

  private void checkInputArgument(ObjectInspector[] arguments, int index) throws UDFArgumentTypeException {
    if (arguments[index].getCategory() != ObjectInspector.Category.PRIMITIVE
        || PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP
        != PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
        ((PrimitiveObjectInspector) arguments[index]).getPrimitiveCategory())) {
      throw new UDFArgumentTypeException(index, "The argument to ConvertCharset() must be a string/varchar");
    }
  }
}
