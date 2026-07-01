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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureRule;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureStatement;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

public class ProtobufAnonymizer implements Anonymizer {

  public ProtobufAnonymizer() {
  }

  @Override
  public Object anonymize(final Object msg, final DataErasureStatement statement) {
    if (!(msg instanceof Message)) {
      throw new RuntimeException("Unsupported type: " + (msg == null ? "null" : msg.getClass().getName()));
    }
    try {
      Message cur = (Message) msg;
      for (final DataErasureRule rule : statement.rules) {
        final PathStep[] steps = PathStep.compile(rule.path);
        cur = anonymizeInternal(cur, steps, 0, steps.length - 1, rule);
      }
      return cur;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Message anonymizeInternal(final Message msg, final PathStep[] steps, final int ix,
                                    final int limit, final DataErasureRule rule)
      throws InvocationTargetException, IllegalAccessException {
    final PathStep step = steps[ix];

    if ("*".equals(step.field)) {
      if (ix != limit) {
        throw new RuntimeException("'*' wildcard must be the terminal path step: " + rule.path);
      }
      return eraseAllFields(msg, rule);
    }

    final Message.Builder b = msg.toBuilder();
    final String fld = step.field;
    final Method getter = findMethod(msg.getClass(), "get" + cap(fld), 0);
    if (getter == null) {
      throw new RuntimeException("getter not found for field '" + fld + "' on " + msg.getClass().getName());
    }
    final Object value = getter.invoke(msg);

    if (value instanceof List) {
      final List<?> lst = (List<?>) value;
      if (!fld.endsWith("List")) {
        throw new RuntimeException("repeated field not bound as '<base>List': " + fld);
      }
      final String base = fld.substring(0, fld.lastIndexOf("List"));
      if (ix < limit) {
        for (final int i : step.selectIndices(lst)) {
          final Object o = lst.get(i);
          if (!(o instanceof Message)) {
            throw new RuntimeException(
                "repeated-message descent on a non-message element: " + rule.path);
          }
          final Message newSub = anonymizeInternal((Message) o, steps, ix + 1, limit, rule);
          final Method set = findIndexedMessageSetter(b.getClass(), cap(base), newSub.getClass());
          if (set == null) {
            throw new RuntimeException("repeated-message setter not found for field: " + fld);
          }
          set.invoke(b, i, newSub);
        }
        return b.build();
      }
      final String elem = RuleValues.stringListElementValueFor(rule);
      final Method set = findRepeatedStringSetter(b.getClass(), cap(base));
      if (set == null) {
        throw new RuntimeException("repeated-string setter not found for field: " + fld);
      }
      for (final int i : step.selectScalarIndices(lst.size())) {
        set.invoke(b, i, elem);
      }
      return b.build();
    }

    if (ix < limit) {
      if (!(value instanceof Message)) {
        throw new RuntimeException("path descends through a non-message field: " + rule.path);
      }
      final Message newSub = anonymizeInternal((Message) value, steps, ix + 1, limit, rule);
      final Method setter = findSetterAccepting(b.getClass(), "set" + cap(fld), newSub.getClass());
      if (setter == null) {
        throw new RuntimeException("message setter not found for field: " + fld);
      }
      setter.invoke(b, newSub);
      return b.build();
    }

    final Method setter = findMethod(b.getClass(), "set" + cap(fld), 1);
    if (setter == null) {
      throw new RuntimeException("setter not found for field: " + fld);
    }
    setter.invoke(b, RuleValues.scalarValueFor(rule, getter.getReturnType().getSimpleName()));
    return b.build();
  }

  private Message eraseAllFields(final Message msg, final DataErasureRule rule) {
    final Message.Builder b = msg.toBuilder();
    for (final Descriptors.FieldDescriptor fd : msg.getDescriptorForType().getFields()) {
      if (fd.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
        if (fd.isRepeated()) {
          final int n = msg.getRepeatedFieldCount(fd);
          for (int i = 0; i < n; i++) {
            b.setRepeatedField(fd, i, eraseAllFields((Message) msg.getRepeatedField(fd, i), rule));
          }
        } else if (msg.hasField(fd)) {
          b.setField(fd, eraseAllFields((Message) msg.getField(fd), rule));
        }
        continue;
      }
      final Object zero = zeroFor(fd, rule);
      if (zero == null) {
        continue;
      }
      if (fd.isRepeated()) {
        final int n = msg.getRepeatedFieldCount(fd);
        for (int i = 0; i < n; i++) {
          b.setRepeatedField(fd, i, zero);
        }
      } else {
        b.setField(fd, zero);
      }
    }
    return b.build();
  }

  private static Object zeroFor(final Descriptors.FieldDescriptor fd, final DataErasureRule rule) {
    switch (fd.getJavaType()) {
      case STRING:  return RuleValues.scalarValueFor(rule, "String");
      case INT:     return RuleValues.scalarValueFor(rule, "int");
      case LONG:    return RuleValues.scalarValueFor(rule, "long");
      case BOOLEAN: return Boolean.FALSE;
      case FLOAT:   return Float.valueOf(0f);
      case DOUBLE:  return Double.valueOf(0d);
      case BYTE_STRING: return ByteString.EMPTY;
      default:      return null;
    }
  }

  private static String cap(final String s) {
    return s.substring(0, 1).toUpperCase() + s.substring(1);
  }

  private static Method findMethod(final Class<?> c, final String name, final int params) {
    for (final Method m : c.getMethods()) {
      if (m.getName().equals(name) && m.getParameterCount() == params && !Modifier.isStatic(m.getModifiers())) {
        return m;
      }
    }
    return null;
  }

  private static Method findSetterAccepting(final Class<?> c, final String name, final Class<?> argType) {
    for (final Method m : c.getMethods()) {
      if (m.getName().equals(name) && m.getParameterCount() == 1
          && m.getParameterTypes()[0].isAssignableFrom(argType)) {
        return m;
      }
    }
    return null;
  }

  private static Method findRepeatedStringSetter(final Class<?> c, final String capBase) {
    for (final Method m : c.getMethods()) {
      if (m.getName().equals("set" + capBase) && m.getParameterCount() == 2
          && m.getParameterTypes()[0] == int.class
          && m.getParameterTypes()[1].isAssignableFrom(String.class)) {
        return m;
      }
    }
    return null;
  }

  private static Method findIndexedMessageSetter(final Class<?> c, final String capBase,
                                                 final Class<?> argType) {
    for (final Method m : c.getMethods()) {
      if (m.getName().equals("set" + capBase) && m.getParameterCount() == 2
          && m.getParameterTypes()[0] == int.class
          && Message.class.isAssignableFrom(m.getParameterTypes()[1])
          && m.getParameterTypes()[1].isAssignableFrom(argType)) {
        return m;
      }
    }
    return null;
  }
}
