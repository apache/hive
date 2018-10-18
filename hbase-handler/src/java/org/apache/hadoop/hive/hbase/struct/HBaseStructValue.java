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
package org.apache.hadoop.hive.hbase.struct;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

/**
 * This is an extension of LazyStruct. All value structs should extend this class and override the
 * {@link LazyStruct#getField(int)} method where fieldID corresponds to the ID of a value in the
 * value structure.
 * <p>
 * For example, for a value structure <i>"/part1/part2/part3"</i>, <i>part1</i> will have an id
 * <i>0</i>, <i>part2</i> will have an id <i>1</i> and <i>part3</i> will have an id <i>2</i>. Custom
 * implementations of getField(fieldID) should return the value corresponding to that fieldID. So,
 * for the above example, the value returned for <i>getField(0)</i> should be </i>part1</i>,
 * <i>getField(1)</i> should be <i>part2</i> and <i>getField(2)</i> should be <i>part3</i>.
 * </p>
 * <p>
 * All implementation are expected to have a constructor of the form <br>
 *
 * <pre>
 * MyCustomStructObject(LazySimpleStructObjectInspector oi, Properties props, Configuration conf, ColumnMapping colMap)
 * </pre>
 * 
 * </p>
 * */
public class HBaseStructValue extends LazyStruct {

  /**
   * The column family name
   */
  protected String familyName;

  /**
   * The column qualifier name
   */
  protected String qualifierName;

  public HBaseStructValue(LazySimpleStructObjectInspector oi) {
    super(oi);
  }

  /**
   * Set the row data for this LazyStruct.
   * 
   * @see LazyObject#init(ByteArrayRef, int, int)
   * 
   * @param familyName The column family name
   * @param qualifierName The column qualifier name
   */
  public void init(ByteArrayRef bytes, int start, int length, String familyName,
      String qualifierName) {
    init(bytes, start, length);
    this.familyName = familyName;
    this.qualifierName = qualifierName;
  }

  @Override
  public ArrayList<Object> getFieldsAsList() {
    ArrayList<Object> allFields = new ArrayList<Object>();

    List<? extends StructField> fields = oi.getAllStructFieldRefs();

    for (int i = 0; i < fields.size(); i++) {
      allFields.add(getField(i));
    }

    return allFields;
  }

  /**
   * Create an initialize a {@link LazyObject} with the given bytes for the given fieldID.
   * 
   * @param fieldID field for which the object is to be created
   * @param bytes value with which the object is to be initialized with
   * @return initialized {@link LazyObject}
   * */
  public LazyObject<? extends ObjectInspector> toLazyObject(int fieldID, byte[] bytes) {
    ObjectInspector fieldOI = oi.getAllStructFieldRefs().get(fieldID).getFieldObjectInspector();

    LazyObject<? extends ObjectInspector> lazyObject = LazyFactory.createLazyObject(fieldOI);

    ByteArrayRef ref = new ByteArrayRef();

    ref.setData(bytes);

    // initialize the lazy object
    lazyObject.init(ref, 0, ref.getData().length);

    return lazyObject;
  }
}