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
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * PosExplode.
 *
 */
@Description(
    name = "posexplode",
    value = "_FUNC_(a) - behaves like explode for arrays, "
    + "but includes the position of items in the original array")
public class GenericUDTFPosExplode extends GenericUDTF {
  private ListObjectInspector listOI = null;
  private final Object[] forwardObj = new Object[2];

  @Override
  public void close() throws HiveException {
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

    if (args.length != 1) {
      throw new UDFArgumentException("posexplode() takes only one argument");
    }

    if (args[0].getCategory() != ObjectInspector.Category.LIST) {
      throw new UDFArgumentException("posexplode() takes an array as a parameter");
    }
    listOI = (ListObjectInspector) args[0];

    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    fieldNames.add("pos");
    fieldNames.add("val");
    fieldOIs.add(PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.INT));
    fieldOIs.add(listOI.getListElementObjectInspector());
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  @Override
  public void process(Object[] o) throws HiveException {
    List<?> list = listOI.getList(o[0]);
    if(list == null)
    {
      return;
    }

    for (int i = 0; i < list.size(); i++) {
      Object r = list.get(i);
      forwardObj[0] = new Integer(i);
      forwardObj[1] = r;
      forward(forwardObj);
    }
  }

  @Override
  public String toString() {
    return "posexplode";
  }
}
