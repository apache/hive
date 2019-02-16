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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS,
                          serdeConstants.LIST_COLUMN_TYPES,
                          serdeConstants.TIMESTAMP_FORMATS})
public class JsonSerDe extends AbstractSerDe {

  private static final Logger LOG = LoggerFactory.getLogger(JsonSerDe.class);
  private HCatSchema schema;

  private HCatRecordObjectInspector cachedObjectInspector;
  private org.apache.hadoop.hive.serde2.JsonSerDe jsonSerde = new org.apache.hadoop.hive.serde2.JsonSerDe();

  @Override
  public void initialize(Configuration conf, Properties tbl)
    throws SerDeException {

    jsonSerde.initialize(conf, tbl);
    jsonSerde.setWriteablesUsage(false);

    StructTypeInfo rowTypeInfo = jsonSerde.getTypeInfo();
    cachedObjectInspector = HCatRecordObjectInspectorFactory.getHCatRecordObjectInspector(rowTypeInfo);
    try {
      schema = HCatSchemaUtils.getHCatSchema(rowTypeInfo).get(0).getStructSubSchema();
      LOG.debug("schema : {}", schema);
      LOG.debug("fields : {}", schema.getFieldNames());
    } catch (HCatException e) {
      throw new SerDeException(e);
    }
  }

  /**
   * Takes JSON string in Text form, and has to return an object representation above
   * it that's readable by the corresponding object inspector.
   *
   * For this implementation, since we're using the jackson parser, we can construct
   * our own object implementation, and we use HCatRecord for it
   */
  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    try {
      Object row = jsonSerde.deserialize(blob);
      List fatRow = fatLand((Object[]) row);
      return new DefaultHCatRecord(fatRow);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked" })
  private static List fatLand(Object[] arr) {
    List ret = new ArrayList<>();
    for (Object o : arr) {
      if (o != null && o instanceof Map<?, ?>) {
        ret.add(fatMap(((Map) o)));
      } else if (o != null && o instanceof List<?>) {
        ret.add(fatLand(((List) o).toArray()));
      } else if (o != null && o.getClass().isArray() && o.getClass().getComponentType() != byte.class) {
        Class<?> ct = o.getClass().getComponentType();
        if (ct.isPrimitive()) {
          ret.add(primitiveArrayToList(o));
        } else {
          ret.add(fatLand((Object[]) o));
        }
      } else {
        ret.add(o);
      }
    }
    return ret;
  }

  @SuppressWarnings("rawtypes")
  private static Object fatMap(Map<Object, Object> map) {
    Map ret = new LinkedHashMap<>();
    Set<Entry<Object, Object>> es = map.entrySet();
    for (Entry<Object, Object> e : es) {
      Object oldV = e.getValue();
      Object newV;
      if (oldV != null && oldV.getClass().isArray()) {
        newV = fatLand((Object[]) oldV);
      } else {
        newV = oldV;
      }
      ret.put(e.getKey(), newV);
    }
    return ret;
  }

  private static Object primitiveArrayToList(Object arr) {
    Class<?> ct = arr.getClass().getComponentType();
    if (int.class.equals(ct)) {
      return Arrays.asList(ArrayUtils.toObject((int[]) arr));
    }
    if (long.class.equals(ct)) {
      return Arrays.asList(ArrayUtils.toObject((long[]) arr));
    }
    if (char.class.equals(ct)) {
      return Arrays.asList(ArrayUtils.toObject((char[]) arr));
    }
    if (byte.class.equals(ct)) {
      return Arrays.asList(ArrayUtils.toObject((byte[]) arr));
    }
    if (short.class.equals(ct)) {
      return Arrays.asList(ArrayUtils.toObject((short[]) arr));
    }
    if (float.class.equals(ct)) {
      return Arrays.asList(ArrayUtils.toObject((float[]) arr));
    }
    if (double.class.equals(ct)) {
      return Arrays.asList(ArrayUtils.toObject((double[]) arr));
    }
    throw new RuntimeException("Unhandled primitiveArrayToList for type: " + ct);
  }

  public String getHiveInternalColumnName(int fpos) {
    return HiveConf.getColumnInternalName(fpos);
  }

  /**
   * Given an object and object inspector pair, traverse the object
   * and generate a Text representation of the object.
   */
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
    throws SerDeException {
    return jsonSerde.serialize(obj, objInspector);
  }

  /**
   *  Returns an object inspector for the specified schema that
   *  is capable of reading in the object representation of the JSON string
   */
  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics yet
    return null;
  }

}
