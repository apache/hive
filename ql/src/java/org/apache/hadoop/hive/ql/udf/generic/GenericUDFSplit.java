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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;

public class GenericUDFSplit extends GenericUDF {
  private ObjectInspectorConverters.Converter[] converters;
  
  public ObjectInspector initialize(ObjectInspector[] arguments)
  throws UDFArgumentException {    
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "The function SPLIT(s, regexp) takes exactly 2 arguments.");
    }
    
    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for(int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }
    
    return ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
  }

  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert(arguments.length == 2);
    
    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }
    
    Text s = (Text)converters[0].convert(arguments[0].get()); 
    Text regex = (Text)converters[1].convert(arguments[1].get());
    
    ArrayList<Text> result = new ArrayList<Text>();
    
    for(String str: s.toString().split(regex.toString())) {
      result.add(new Text(str));
    }
    
    return result;
  }
  
  public String getDisplayString(String[] children) {
    assert(children.length == 2);
    return "split(" + children[0] + ", " + children[1] + ")";
  }

}
