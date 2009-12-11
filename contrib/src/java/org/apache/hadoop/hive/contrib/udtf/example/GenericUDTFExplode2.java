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

package org.apache.hadoop.hive.contrib.udtf.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

@description(
    name = "explode2",
    value = "_FUNC_(a) - like explode, but outputs two identical columns (for "+
            "testing purposes)"
)
public class GenericUDTFExplode2 extends GenericUDTF {

  ListObjectInspector listOI = null;
  
  @Override
  public void close() throws HiveException{
  }
  
  @Override
  public StructObjectInspector initialize(ObjectInspector [] args) 
  throws UDFArgumentException {
    
    if (args.length != 1) {
      throw new UDFArgumentException("explode() takes only one argument");
    }
    
    if (args[0].getCategory() != ObjectInspector.Category.LIST) {
      throw new UDFArgumentException("explode() takes an array as a parameter");
    }
    listOI = (ListObjectInspector)args[0];
    
    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    fieldNames.add("col1");
    fieldNames.add("col2");
    fieldOIs.add(listOI.getListElementObjectInspector());
    fieldOIs.add(listOI.getListElementObjectInspector());
    return ObjectInspectorFactory.getStandardStructObjectInspector(
        fieldNames, fieldOIs);
  }

  Object forwardObj[] = new Object[2];
  @Override
  public void process(Object [] o) throws HiveException {
   
    List<?> list = listOI.getList(o[0]);
    for (Object r : list) {
      forwardObj[0] = r;
      forwardObj[1] = r;
      this.forward(forwardObj);
    }
  }

  @Override
  public String toString() {
    return "explode";
  }
}
