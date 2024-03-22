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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.externalize.RelEnumTypes;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDigestIncludeType;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
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
    if (value instanceof RexNode) {
      return toJson((RexNode) value);
    }
    if (value instanceof RelDataTypeField) {
      return toJson((RelDataTypeField) value);
    }
    if (value instanceof RelDataType) {
      return toJson((RelDataType) value);
    }
    if (value instanceof AggregateCall) {
      return toJson((AggregateCall) value);
    }
    return super.toJson(value);
  }

  public Object toJson(AggregateCall node) {
    final Map<String, Object> map = (Map<String, Object>) super.toJson(node);
    map.put("type", toJson(node.getType()));
    return map;
  }

  private Object toJson(RelDataTypeField node) {
    final Map<String, Object> map;
    if (node.getType().isStruct()) {
      map = jsonBuilder.map();
      map.put("fields", toJson(node.getType()));
    } else {
      map = (Map<String, Object>) toJson(node.getType());
    }
    map.put("name", node.getName());
    return map;
  }

  private Object toJson(RelDataType node) {
    if (node.isStruct()) {
      final List<Object> list = jsonBuilder.list();
      for (RelDataTypeField field : node.getFieldList()) {
        list.add(toJson(field));
      }
      return list;
    } else {
      final Map<String, Object> map = jsonBuilder.map();
      map.put("type", node.getSqlTypeName().name());
      map.put("nullable", node.isNullable());
      if (node.getSqlTypeName().allowsPrec()) {
        map.put("precision", node.getPrecision());
      }
      if (node.getSqlTypeName().allowsScale()) {
        map.put("scale", node.getScale());
      }

      if (SqlTypeName.MAP == node.getSqlTypeName()) {
        map.put("key", toJson(node.getKeyType()));
        map.put("value", toJson(node.getValueType()));
      } else if (SqlTypeName.ARRAY == node.getSqlTypeName()) {
        map.put("component", toJson(node.getComponentType()));
      }
      return map;
    }
  }

  private Object toJson(RexNode node) {
    final Map<String, Object> map;
    switch (node.getKind()) {
      case FIELD_ACCESS:
        map = jsonBuilder.map();
        final RexFieldAccess fieldAccess = (RexFieldAccess) node;
        map.put("field", fieldAccess.getField().getName());
        map.put("expr", toJson(fieldAccess.getReferenceExpr()));
        return map;
      case LITERAL:
        map = jsonBuilder.map();
        final RexLiteral literal = (RexLiteral) node;
        final Object value;
        if (SqlTypeFamily.TIMESTAMP == literal.getTypeName().getFamily()) {
          // Had to do this to prevent millis or nanos from getting trimmed
          value = literal.computeDigest(RexDigestIncludeType.NO_TYPE);
        } else {
          value = literal.getValue3();
        }
        map.put("literal", RelEnumTypes.fromEnum(value));
        map.put("type", toJson(node.getType()));
        return map;
      case INPUT_REF:
      case LOCAL_REF:
        map = jsonBuilder.map();
        map.put("input", ((RexSlot) node).getIndex());
        map.put("name", ((RexSlot) node).getName());
        map.put("type", toJson(node.getType()));
        return map;
      case CORREL_VARIABLE:
        map = jsonBuilder.map();
        map.put("correl", ((RexCorrelVariable) node).getName());
        map.put("type", toJson(node.getType()));
        return map;
      default:
        if (node instanceof RexCall) {
          final RexCall call = (RexCall) node;
          map = jsonBuilder.map();
          map.put("op", toJson(call.getOperator()));
          map.put("type", toJson(call.getType()));
          final List<Object> list = jsonBuilder.list();
          for (RexNode operand : call.getOperands()) {
            list.add(toJson(operand));
          }
          map.put("operands", list);
          switch (node.getKind()) {
            case CAST:
            case MAP_QUERY_CONSTRUCTOR:
            case MAP_VALUE_CONSTRUCTOR:
            case ARRAY_QUERY_CONSTRUCTOR:
            case ARRAY_VALUE_CONSTRUCTOR:
              map.put("type", toJson(node.getType()));
          }
          if (call.getOperator() instanceof SqlFunction) {
            if (((SqlFunction) call.getOperator()).getFunctionType().isUserDefined()) {
              SqlOperator op = call.getOperator();
              map.put("class", op.getClass().getName());
              map.put("type", toJson(node.getType()));
              map.put("deterministic", op.isDeterministic());
              map.put("dynamic", op.isDynamicFunction());
            }
          }
          if (call instanceof RexOver) {
            RexOver over = (RexOver) call;
            map.put("distinct", over.isDistinct());
            map.put("type", toJson(node.getType()));
            map.put("window", toJson(over.getWindow()));
          }
          return map;
        }
        throw new UnsupportedOperationException("unknown rex " + node);
    }
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

  private Map toJson(SqlOperator operator) {
    // User-defined operators are not yet handled.
    Map map = jsonBuilder.map();
    map.put("name", operator.getName());
    map.put("kind", operator.kind.toString());
    map.put("syntax", operator.getSyntax().toString());
    return map;
  }
}
