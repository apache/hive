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
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IOUtils;

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

    strObjectInspector = arguments[0];
    fileObjectInspector = arguments[1];

    if (!isTypeCompatible(strObjectInspector)) {
      throw new UDFArgumentTypeException(0, "The first " +
        "argument of function IN_FILE must be a string, " +
        "char or varchar but " +
        strObjectInspector.toString() + " was given.");
    }

    if (((PrimitiveObjectInspector) fileObjectInspector).getPrimitiveCategory() !=
          PrimitiveObjectInspector.PrimitiveCategory.STRING ||
      !ObjectInspectorUtils.isConstantObjectInspector(fileObjectInspector)) {
      throw new UDFArgumentTypeException(1, "The second " +
        "argument of IN_FILE() must be a constant string but " +
        fileObjectInspector.toString() + " was given.");
    }

    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  }

  private boolean isTypeCompatible(ObjectInspector argument) {
    PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) argument);
    return
      poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING ||
      poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.CHAR ||
      poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.VARCHAR;
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

    String str = ObjectInspectorUtils.copyToStandardJavaObject(
        arguments[0].get(), strObjectInspector).toString();

    if (set == null) {
      String filePath = (String)ObjectInspectorUtils.copyToStandardJavaObject(
        arguments[1].get(), fileObjectInspector);
      loadFromFile(filePath);
    }

    return set.contains(str);
  }

  private BufferedReader getReaderFor(String filePath) throws HiveException {
    try {
      Path fullFilePath = FileSystems.getDefault().getPath(filePath);
      Path fileName = fullFilePath.getFileName();
      if (Files.exists(fileName)) {
        return Files.newBufferedReader(fileName, Charset.defaultCharset());
      }
      else
      if (Files.exists(fullFilePath)) {
        return Files.newBufferedReader(fullFilePath, Charset.defaultCharset());
      }
      else {
        throw new HiveException("Could not find \"" + fileName + "\" or \"" + fullFilePath + "\" in IN_FILE() UDF.");
      }
    }
    catch(IOException exception) {
      throw new HiveException(exception);
    }
  }

  private void loadFromFile(String filePath) throws HiveException {
    set = new HashSet<String>();
    BufferedReader reader = getReaderFor(filePath);
    try {
      String line;
      while((line = reader.readLine()) != null) {
        set.add(line);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
    finally {
      IOUtils.closeStream(reader);
    }
  }

  @Override
  public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
    super.copyToNewInstance(newInstance); // Asserts the class invariant. (Same types.)
    GenericUDFInFile that = (GenericUDFInFile)newInstance;
    if (that != this) {
      that.set = (this.set == null ? null : (HashSet<String>)this.set.clone());
      that.strObjectInspector = this.strObjectInspector;
      that.fileObjectInspector = this.fileObjectInspector;
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return "in_file(" + children[0] + ", " + children[1] + ")";
  }
}
