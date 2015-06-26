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

package org.apache.hadoop.hive.ql.udf.generic;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.io.BytesWritable;

@Description(name = "encode",
value = "_FUNC_(str, str) - Encode the first argument using the second argument character set",
extended = "Possible options for the character set are 'US-ASCII', 'ISO-8859-1',\n" +
    "'UTF-8', 'UTF-16BE', 'UTF-16LE', and 'UTF-16'. If either argument\n" +
    "is null, the result will also be null")
public class GenericUDFEncode extends GenericUDF {
  private transient CharsetEncoder encoder = null;
  private transient PrimitiveObjectInspector stringOI = null;
  private transient PrimitiveObjectInspector charsetOI = null;
  private transient BytesWritable result = new BytesWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("Encode() requires exactly two arguments");
    }

    if (arguments[0].getCategory() != Category.PRIMITIVE ||
        PrimitiveGrouping.STRING_GROUP != PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
            ((PrimitiveObjectInspector)arguments[0]).getPrimitiveCategory())){
      throw new UDFArgumentTypeException(
          0, "The first argument to Encode() must be a string/varchar");
    }

    stringOI = (PrimitiveObjectInspector) arguments[0];

    if (arguments[1].getCategory() != Category.PRIMITIVE ||
        PrimitiveGrouping.STRING_GROUP != PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
            ((PrimitiveObjectInspector)arguments[1]).getPrimitiveCategory())){
      throw new UDFArgumentTypeException(
          1, "The second argument to Encode() must be a string/varchar");
    }

    charsetOI = (PrimitiveObjectInspector) arguments[1];

    // If the character set for encoding is constant, we can optimize that
    if (charsetOI instanceof ConstantObjectInspector){
      String charSetName =
          ((ConstantObjectInspector) arguments[1]).getWritableConstantValue().toString();
      encoder = Charset.forName(charSetName).newEncoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);
    }

    result = new BytesWritable();

    return (ObjectInspector) PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String value = PrimitiveObjectInspectorUtils.getString(arguments[0].get(), stringOI);
    if (value == null) {
      return null;
    }

    ByteBuffer encoded;
    if (encoder != null){
      try {
        encoded = encoder.encode(CharBuffer.wrap(value));
      } catch (CharacterCodingException e) {
        throw new HiveException(e);
      }
    } else {
      encoded = Charset.forName(
          PrimitiveObjectInspectorUtils.getString(arguments[1].get(), charsetOI)).encode(value);
    }
    result.setSize(encoded.limit());
    encoded.get(result.getBytes(), 0, encoded.limit());
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return getStandardDisplayString("encode", children, ",");
  }
}
