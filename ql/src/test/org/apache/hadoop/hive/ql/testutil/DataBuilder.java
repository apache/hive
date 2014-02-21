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

package org.apache.hadoop.hive.ql.testutil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

/**
 *
 * DataBuilder used to build InspectableObject arrays that are used
 * as part of testing.
 *
 */
public class DataBuilder {

  private final List<String> columnNames;
  private final List<ObjectInspector> columnTypes;
  private final List<List<Object>> rows;

  public DataBuilder(){
    columnNames = new ArrayList<String>();
    columnTypes = new ArrayList<ObjectInspector>();
    rows = new ArrayList<List<Object>>();
  }

  public void setColumnNames(String ... names){
    for (String name: names){
      columnNames.add(name);
    }
  }

  public void setColumnTypes(ObjectInspector ... types){
    for (ObjectInspector type: types){
      columnTypes.add(type);
    }
  }

  public void addRow(Object ... columns){
    List<Object> objects = Arrays.asList(columns);
    rows.add(objects);
  }

  /**
   * returns the InspectableObject array the builder methods
   * helped to assemble.
   * @return InspectableObject array (objects that have data coupled with
   * and object inspector )
   */
  public InspectableObject[] createRows(){
    InspectableObject[] toReturn = new InspectableObject[this.rows.size()];
    for (int i=0; i<toReturn.length;i++){
      toReturn[i] = new InspectableObject();
      toReturn[i].o = rows.get(i);
      toReturn[i].oi = ObjectInspectorFactory.getStandardStructObjectInspector(
          this.columnNames, this.columnTypes);
    }
    return toReturn;
  }
}
