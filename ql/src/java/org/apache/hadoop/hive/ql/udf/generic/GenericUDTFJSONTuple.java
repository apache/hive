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

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.json.JSONException;
import org.json.JSONObject;
/**
 * GenericUDTFJSONTuple: this
 *
 */
@Description(name = "json_tuple",
    value = "_FUNC_(jsonStr, p1, p2, ..., pn) - like get_json_object, but it takes multiple names and return a tuple. " +
    		"All the input parameters and output column types are string.")

public class GenericUDTFJSONTuple extends GenericUDTF {

  private static Log LOG = LogFactory.getLog(GenericUDTFJSONTuple.class.getName());

  int numCols;    // number of output columns
  String[] paths; // array of path expressions, each of which corresponds to a column
  Text[] retCols; // array of returned column values
  Text[] cols;    // object pool of non-null Text, avoid creating objects all the time
  Object[] nullCols; // array of null column values
  ObjectInspector[] inputOIs; // input ObjectInspectors
  boolean pathParsed = false;
  boolean seenErrors = false;

  @Override
  public void close() throws HiveException {
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args)
      throws UDFArgumentException {

    inputOIs = args;
    numCols = args.length - 1;

    if (numCols < 1) {
      throw new UDFArgumentException("json_tuple() takes at least two arguments: " +
      		"the json string and a path expression");
    }

    for (int i = 0; i < args.length; ++i) {
      if (args[i].getCategory() != ObjectInspector.Category.PRIMITIVE ||
          !args[i].getTypeName().equals(Constants.STRING_TYPE_NAME)) {
        throw new UDFArgumentException("json_tuple()'s arguments have to be string type");
      }
    }

    seenErrors = false;
    pathParsed = false;
    paths = new String[numCols];
    cols = new Text[numCols];
    retCols = new Text[numCols];
    nullCols = new Object[numCols];

    for (int i = 0; i < numCols; ++i) {
      cols[i] = new Text();
      retCols[i] = cols[i];
      nullCols[i] = null;
    }

    // construct output object inspector
    ArrayList<String> fieldNames = new ArrayList<String>(numCols);
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(numCols);
    for (int i = 0; i < numCols; ++i) {
      // column name can be anything since it will be named by UDTF as clause
      fieldNames.add("c" + i);
      // all returned type will be Text
      fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  @Override
  public void process(Object[] o) throws HiveException {

    if (o[0] == null) {
      forward(nullCols);
      return;
    }
    // get the path expression for the 1st row only
    if (!pathParsed) {
      for (int i = 0;i < numCols; ++i) {
        paths[i] = ((StringObjectInspector) inputOIs[i+1]).getPrimitiveJavaObject(o[i+1]);
      }
      pathParsed = true;
    }

    String jsonStr = ((StringObjectInspector) inputOIs[0]).getPrimitiveJavaObject(o[0]);
    if (jsonStr == null) {
      forward(nullCols);
      return;
    }
    try {
      JSONObject jsonObj = new JSONObject(jsonStr);

      for (int i = 0; i < numCols; ++i) {
        if (jsonObj.isNull(paths[i])) {
          retCols[i] = null;
        } else {
          if (retCols[i] == null) {
            retCols[i] = cols[i]; // use the object pool rather than creating a new object
          }
          retCols[i].set(jsonObj.getString(paths[i]));
        }
      }
      forward(retCols);
      return;
    } catch (JSONException e) {
      // parsing error, invalid JSON string
      if (!seenErrors) {
        LOG.error("The input is not a valid JSON string: " + jsonStr + ". Skipping such error messages in the future.");
        seenErrors = true;
      }
      forward(nullCols);
      return;
    } catch (Throwable e) {
      LOG.error("JSON parsing/evaluation exception" + e);
      forward(nullCols);
    }
  }

  @Override
  public String toString() {
    return "json_tuple";
  }
}
