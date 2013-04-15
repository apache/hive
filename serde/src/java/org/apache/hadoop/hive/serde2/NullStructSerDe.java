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
package org.apache.hadoop.hive.serde2;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

/**
 * Placeholder SerDe for cases where neither serialization nor deserialization is needed
 *
 */
public class NullStructSerDe extends AbstractSerDe {

  class NullStructField implements StructField {
    @Override
    public String getFieldName() {
      return null;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return null;
    }

    @Override
    public String getFieldComment() {
      return "";
    }
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    return null;
  }

  private static ObjectInspector nullStructOI = new NullStructSerDeObjectInspector();

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return nullStructOI;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return NullWritable.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    return NullWritable.get();
  }


  /**
   * A object inspector for null struct serde.
   */
  public static class NullStructSerDeObjectInspector extends StructObjectInspector {
    public String getTypeName() {
      return "null";
    }

    public Category getCategory() {
      return Category.PRIMITIVE;
    }

    @Override
    public StructField getStructFieldRef(String fieldName) {
      return null;
    }

    @Override
    public List<NullStructField> getAllStructFieldRefs() {
      return new ArrayList<NullStructField>();
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
      return null;
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object data) {
      return new ArrayList<Object>();
    }
  }

}
