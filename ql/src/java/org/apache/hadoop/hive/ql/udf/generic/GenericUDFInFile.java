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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

/**
 * IN_FILE(str, filename) returns true if 'str' appears in the file specified
 * by 'filename'.  A string is considered to be in the file if it that string
 * appears as a line in the file.
 *
 * If either argument is NULL then NULL is returned.
 */
@Description(name = "in_file",
    value = "_FUNC_(str, filename) - Returns true if str appears in the file")
public class GenericUDFInFile extends GenericUDF {

  private HashSet<String> set;
  private transient ObjectInspector strObjectInspector;
  private transient ObjectInspector fileObjectInspector;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "IN_FILE() accepts exactly 2 arguments.");
    }

    for (int i = 0; i < arguments.length; i++) {
      if (!String.class.equals(
            PrimitiveObjectInspectorUtils.
                getJavaPrimitiveClassFromObjectInspector(arguments[i]))) {
        throw new UDFArgumentTypeException(i, "The "
            + GenericUDFUtils.getOrdinal(i + 1)
            + " argument of function IN_FILE must be a string but "
            + arguments[i].toString() + " was given.");
      }
    }

    strObjectInspector = arguments[0];
    fileObjectInspector = arguments[1];

    if (!ObjectInspectorUtils.isConstantObjectInspector(fileObjectInspector)) {
      throw new UDFArgumentTypeException(1,
          "The second argument of IN_FILE() must be a constant string but " +
          fileObjectInspector.toString() + " was given.");
    }

    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  }

  @Override
  public String[] getRequiredFiles() {
    return new String[] {
        ObjectInspectorUtils.getWritableConstantValue(fileObjectInspector)
            .toString()
    };
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }

    String str = (String)ObjectInspectorUtils.copyToStandardJavaObject(
        arguments[0].get(), strObjectInspector);

    if (set == null) {
      String fileName = (String)ObjectInspectorUtils.copyToStandardJavaObject(
          arguments[1].get(), fileObjectInspector);
      try {
        load(new FileInputStream((new File(fileName)).getName()));
      } catch (FileNotFoundException e) {
        throw new HiveException(e);
      }
    }

    return Boolean.valueOf(set.contains(str));
  }

  /**
   * Load the file from an InputStream.
   * @param is The InputStream contains the file data.
   * @throws HiveException
   */
  public void load(InputStream is) throws HiveException {
    BufferedReader reader =
      new BufferedReader(new InputStreamReader(is));

    set = new HashSet<String>();

    try {
      String line;
      while((line = reader.readLine()) != null) {
        set.add(line);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return "in_file(" + children[0] + ", " + children[1] + ")";
  }
}
