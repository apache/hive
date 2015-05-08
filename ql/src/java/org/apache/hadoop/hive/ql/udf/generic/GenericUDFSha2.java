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

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.BINARY_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFSha2.
 *
 */
@Description(name = "sha2", value = "_FUNC_(string/binary, len) - Calculates the SHA-2 family of hash functions "
    + "(SHA-224, SHA-256, SHA-384, and SHA-512).",
    extended = "The first argument is the string or binary to be hashed. "
    + "The second argument indicates the desired bit length of the result, "
    + "which must have a value of 224, 256, 384, 512, or 0 (which is equivalent to 256). "
    + "SHA-224 is supported starting from Java 8. "
    + "If either argument is NULL or the hash length is not one of the permitted values, the return value is NULL.\n"
    + "Example: > SELECT _FUNC_('ABC', 256);\n 'b5d4045c3f466fa91fe2cc6abe79232a1a57cdf104f7a26e716e0a1e2789df78'")
public class GenericUDFSha2 extends GenericUDF {
  private transient Converter[] converters = new Converter[2];
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[2];
  private final Text output = new Text();
  private transient boolean isStr;
  private transient MessageDigest digest;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);

    // the function should support both string and binary input types
    checkArgGroups(arguments, 0, inputTypes, STRING_GROUP, BINARY_GROUP);
    checkArgGroups(arguments, 1, inputTypes, NUMERIC_GROUP);

    if (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(inputTypes[0]) == STRING_GROUP) {
      obtainStringConverter(arguments, 0, inputTypes, converters);
      isStr = true;
    } else {
      GenericUDFParamUtils.obtainBinaryConverter(arguments, 0, inputTypes, converters);
      isStr = false;
    }

    if (arguments[1] instanceof ConstantObjectInspector) {
      Integer lenObj = getConstantIntValue(arguments, 1);
      if (lenObj != null) {
        int len = lenObj.intValue();
        if (len == 0) {
          len = 256;
        }
        try {
          digest = MessageDigest.getInstance("SHA-" + len);
        } catch (NoSuchAlgorithmException e) {
          // ignore
        }
      }
    } else {
      throw new UDFArgumentTypeException(1, getFuncName() + " only takes constant as "
          + getArgOrder(1) + " argument");
    }

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (digest == null) {
      return null;
    }

    digest.reset();
    if (isStr) {
      Text n = GenericUDFParamUtils.getTextValue(arguments, 0, converters);
      if (n == null) {
        return null;
      }
      digest.update(n.getBytes(), 0, n.getLength());
    } else {
      BytesWritable bWr = GenericUDFParamUtils.getBinaryValue(arguments, 0, converters);
      if (bWr == null) {
        return null;
      }
      digest.update(bWr.getBytes(), 0, bWr.getLength());
    }
    byte[] resBin = digest.digest();
    String resStr = Hex.encodeHexString(resBin);

    output.set(resStr);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "sha2";
  }
}
