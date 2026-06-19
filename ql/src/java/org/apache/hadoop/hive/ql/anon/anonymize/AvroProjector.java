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

package org.apache.hadoop.hive.ql.anon.anonymize;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureRule;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureStatement;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AvroProjector implements Projector {

  private static final Object MISS = new Object();

  public AvroProjector() {
  }

  @Override
  public Map<String, Object> project(final Object msg, final DataErasureStatement statement) {
    if (!(msg instanceof SpecificRecordBase)) {
      throw new RuntimeException("Unsupported message type: "
          + (msg == null ? "null" : msg.getClass().getName()));
    }
    final Map<String, Object> out = new LinkedHashMap<>();
    try {
      for (final DataErasureRule rule : statement.rules) {
        final PathStep[] steps = PathStep.compile(rule.path);
        final Object v = projectInternal(msg, steps, 0, steps.length - 1, rule);
        if (v != MISS) {
          out.put(rule.path, v);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return out;
  }

  private Object projectInternal(final Object node, final PathStep[] steps, final int ix,
                                 final int limit, final DataErasureRule rule)
      throws InvocationTargetException, IllegalAccessException {
    final PathStep step = steps[ix];

    if ("*".equals(step.field)) {
      if (ix != limit) {
        throw new RuntimeException("'*' wildcard must be the terminal path step: " + rule.path);
      }
      return projectAllLeaves(node);
    }

    final Object value = invokeGetter(node, step.field);
    if (value == null) {
      return MISS;
    }

    if (value instanceof List) {
      final List<?> lst = (List<?>) value;
      final List<Object> picked = new ArrayList<>();
      for (final int i : step.selectIndices(lst)) {
        final Object e = lst.get(i);
        if (ix < limit) {
          final Object child = (e == null) ? MISS : projectInternal(e, steps, ix + 1, limit, rule);
          picked.add(child == MISS ? null : child);
        } else {
          picked.add(normalizeLeaf(e));
        }
      }
      return picked;
    }

    if (ix < limit) {
      if (!(value instanceof SpecificRecordBase)) {
        throw new RuntimeException("path descends through a non-record field: " + rule.path);
      }
      return projectInternal(value, steps, ix + 1, limit, rule);
    }

    if (value instanceof SpecificRecordBase) {
      throw new RuntimeException("path terminates on a record without a ':*' wildcard: " + rule.path);
    }
    return normalizeLeaf(value);
  }

  private Map<String, Object> projectAllLeaves(final Object node) {
    if (!(node instanceof SpecificRecordBase)) {
      throw new RuntimeException("':*' applied to a non-record value: "
          + (node == null ? "null" : node.getClass().getName()));
    }
    final SpecificRecordBase rec = (SpecificRecordBase) node;
    final Map<String, Object> out = new LinkedHashMap<>();
    for (final Schema.Field f : rec.getSchema().getFields()) {
      out.put(f.name(), convertDeep(rec.get(f.pos())));
    }
    return out;
  }

  private Object convertDeep(final Object v) {
    if (v == null) {
      return null;
    }
    if (v instanceof List) {
      final List<Object> out = new ArrayList<>();
      for (final Object e : (List<?>) v) {
        out.add(convertDeep(e));
      }
      return out;
    }
    if (v instanceof SpecificRecordBase) {
      return projectAllLeaves(v);
    }
    return normalizeLeaf(v);
  }

  private static Object invokeGetter(final Object node, final String field)
      throws InvocationTargetException, IllegalAccessException {
    final String getter = "get" + field.substring(0, 1).toUpperCase() + field.substring(1);
    for (final Method m : node.getClass().getMethods()) {
      if (m.getParameterCount() == 0 && Modifier.isPublic(m.getModifiers())
          && !Modifier.isStatic(m.getModifiers()) && m.getName().equals(getter)) {
        return m.invoke(node);
      }
    }
    throw new RuntimeException("getter not found for field '" + field + "' on "
        + node.getClass().getName());
  }

  private static Object normalizeLeaf(final Object v) {
    return (v instanceof CharSequence && !(v instanceof String)) ? v.toString() : v;
  }
}
