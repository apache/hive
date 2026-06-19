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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.JsonBuilder;

/**
 * Hive extension of RelJson.
 * Implement json serialization of types which are not support by Calcite 1.21.0.
 * This class can be removed when Calcite is upgraded to 1.23.0
 */
public class HiveRelJson extends RelJson {
  private final JsonBuilder jsonBuilder;

  public HiveRelJson(JsonBuilder jsonBuilder) {
    super(jsonBuilder);
    this.jsonBuilder = jsonBuilder;
  }

  @Override
  public Object toJson(Object value) {
    if (value instanceof RelDistribution) {
      return toJson((RelDistribution) value);
    }
    if(value instanceof RelDataType) {
      return toJson((RelDataType) value);
    }
    return super.toJson(value);
  }

  // Upgrade to Calcite 1.23.0 to remove this method
  private Object toJson(RelDistribution relDistribution) {
    final Map<String, Object> map = jsonBuilder.map();
    map.put("type", relDistribution.getType().name());

    if (!relDistribution.getKeys().isEmpty()) {
      List<Object> keys = new ArrayList<>(relDistribution.getKeys().size());
      for (Integer key : relDistribution.getKeys()) {
        keys.add(toJson(key));
      }
      map.put("keys", keys);
    }
    return map;
  }

  private Object toJson(RelDataType node) {
    if (Bug.CALCITE_6832_FIXED) {
      throw new IllegalStateException("Method is redundant after CALCITE-6832");
    }
    final Map<String, Object> map = jsonBuilder.map();
    if (node.isStruct()) {
      final List<Object> list = jsonBuilder.list();
      for (RelDataTypeField field : node.getFieldList()) {
        Map<String, Object> mf = (Map<String, Object>) toJson(field.getType());
        mf.put("name", field.getName());
        list.add(mf);
      }
      map.put("fields", list);
      map.put("nullable", node.isNullable());
    } else {
      map.put("type", node.getSqlTypeName().name());
      map.put("nullable", node.isNullable());
      if (node.getComponentType() != null) {
        map.put("component", toJson(node.getComponentType()));
      }
      RelDataType keyType = node.getKeyType();
      if (keyType != null) {
        map.put("key", toJson(keyType));
      }
      RelDataType valueType = node.getValueType();
      if (valueType != null) {
        map.put("value", toJson(valueType));
      }
      if (node.getSqlTypeName().allowsPrec()) {
        map.put("precision", node.getPrecision());
      }
      if (node.getSqlTypeName().allowsScale()) {
        map.put("scale", node.getScale());
      }
    }
    return map;
  }
}
