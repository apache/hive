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

import org.apache.hadoop.hive.ql.anon.model.BaseMsg;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureRule;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureStatement;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MessageProjector implements Projector {

  public MessageProjector() {
  }

  @Override
  public Map<String, Object> project(final Object msg, final DataErasureStatement statement) {
    if (!(msg instanceof BaseMsg)) {
      throw new RuntimeException("Unsupported message type: "
          + (msg == null ? "null" : msg.getClass().getName()));
    }
    final Map<String, Object> out = new LinkedHashMap<>();
    try {
      for (final DataErasureRule rule : statement.rules) {
        final PathStep[] steps = PathStep.compile(rule.path);
        final Object value = projectInternal((BaseMsg) msg, steps, 0, steps.length - 1, rule);
        if (value != PathMiss.INSTANCE) {
          out.put(rule.path, value);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return out;
  }

  private static final class PathMiss {
    static final PathMiss INSTANCE = new PathMiss();
    private PathMiss() {}
  }

  private Object projectInternal(final BaseMsg msg, final PathStep[] steps, final int ix,
                                 final int limit, final DataErasureRule rule)
      throws InvocationTargetException, IllegalAccessException {

    final PathStep step = steps[ix];
    final String fieldName = step.field;

    if ("*".equals(fieldName)) {
      if (ix != limit) {
        throw new RuntimeException("'*' wildcard must be the terminal path step: " + rule.path);
      }
      return projectSubObjectRecursively(msg);
    }

    final Class<? extends BaseMsg> clazz = msg.getClass();

    Method getterMethod = null;
    for (final Method method : clazz.getMethods()) {
      final int modifiers = method.getModifiers();
      if (!Modifier.isPublic(modifiers) || Modifier.isStatic(modifiers)) {
        continue;
      }
      if (method.getName().equalsIgnoreCase("get" + fieldName)) {
        getterMethod = method;
        break;
      }
    }
    if (getterMethod == null) {
      throw new RuntimeException("getter not found for field: " + fieldName);
    }

    final Class<?> rtc = getterMethod.getReturnType();

    switch (rtc.getSimpleName()) {
      case "int":
      case "short":
      case "long":
      case "String": {
        return getterMethod.invoke(msg);
      }
      case "List": {
        try {
          final Field f = clazz.getDeclaredField(fieldName);
          final ParameterizedType pt = (ParameterizedType) f.getGenericType();
          final Class<?> ptc = (Class<?>) pt.getActualTypeArguments()[0];

          if ("String".equals(ptc.getSimpleName())) {
            final List<?> src = (List<?>) getterMethod.invoke(msg);
            if (src == null) {
              return PathMiss.INSTANCE;
            }
            final List<Object> picked = new ArrayList<>();
            for (final int i : step.selectIndices(src)) {
              picked.add(src.get(i));
            }
            return picked;
          }
          if (!BaseMsg.class.isAssignableFrom(ptc)) {
            throw new RuntimeException("unsupported generic type: " + pt);
          }
          if (ix >= limit) {
            throw new RuntimeException(
                "path terminates on a List of sub-objects without a ':*' wildcard: " + rule.path);
          }
          final List<?> src = (List<?>) getterMethod.invoke(msg);
          if (src == null) {
            return PathMiss.INSTANCE;
          }
          final List<Object> projected = new ArrayList<>();
          for (final int i : step.selectIndices(src)) {
            final Object element = src.get(i);
            if (element == null) {
              projected.add(null);
              continue;
            }
            final Object child = projectInternal((BaseMsg) element, steps, ix + 1, limit, rule);
            projected.add(child == PathMiss.INSTANCE ? null : child);
          }
          return projected;
        } catch (NoSuchFieldException e) {
          throw new RuntimeException(e);
        }
      }
      default: {
        if (!BaseMsg.class.isAssignableFrom(rtc)) {
          throw new RuntimeException("bad field type detected: " + rtc);
        }
        if (ix >= limit) {
          throw new RuntimeException(
              "path terminates on a sub-object without a ':*' wildcard: " + rule.path);
        }
        final Object sub = getterMethod.invoke(msg);
        if (sub == null) {
          return PathMiss.INSTANCE;
        }
        return projectInternal((BaseMsg) sub, steps, ix + 1, limit, rule);
      }
    }
  }

  private Map<String, Object> projectSubObjectRecursively(final BaseMsg obj)
      throws InvocationTargetException, IllegalAccessException {

    final Map<String, Object> out = new LinkedHashMap<>();
    final Class<? extends BaseMsg> clazz = obj.getClass();
    final Method[] methods = clazz.getMethods();

    for (final Method getter : methods) {
      final String name = getter.getName();
      if (!name.startsWith("get") || name.equals("getClass")) {
        continue;
      }
      final int modifiers = getter.getModifiers();
      if (!Modifier.isPublic(modifiers) || Modifier.isStatic(modifiers)) {
        continue;
      }
      if (getter.getParameterCount() != 0) {
        continue;
      }
      final String setterName = "set" + name.substring(3);
      boolean hasSetter = false;
      for (final Method m : methods) {
        if (m.getName().equals(setterName) && m.getParameterCount() == 1
            && Modifier.isPublic(m.getModifiers()) && !Modifier.isStatic(m.getModifiers())) {
          hasSetter = true;
          break;
        }
      }
      if (!hasSetter) {
        continue;
      }

      final String fieldKey = Character.toLowerCase(name.charAt(3)) + name.substring(4);
      final Class<?> rtc = getter.getReturnType();
      switch (rtc.getSimpleName()) {
        case "int":
        case "short":
        case "long":
        case "String": {
          out.put(fieldKey, getter.invoke(obj));
          break;
        }
        case "List": {
          try {
            final Field f = clazz.getDeclaredField(fieldKey);
            final ParameterizedType pt = (ParameterizedType) f.getGenericType();
            final Class<?> ptc = (Class<?>) pt.getActualTypeArguments()[0];
            if ("String".equals(ptc.getSimpleName())) {
              final List<?> src = (List<?>) getter.invoke(obj);
              out.put(fieldKey, src == null ? null : new ArrayList<>(src));
            } else if (BaseMsg.class.isAssignableFrom(ptc)) {
              final List<?> src = (List<?>) getter.invoke(obj);
              if (src == null) {
                out.put(fieldKey, null);
              } else {
                final List<Object> projected = new ArrayList<>(src.size());
                for (final Object element : src) {
                  projected.add(element == null
                      ? null : projectSubObjectRecursively((BaseMsg) element));
                }
                out.put(fieldKey, projected);
              }
            }
          } catch (NoSuchFieldException ignored) {  }
          break;
        }
        default: {
          if (BaseMsg.class.isAssignableFrom(rtc)) {
            final Object sub = getter.invoke(obj);
            out.put(fieldKey, sub == null ? null : projectSubObjectRecursively((BaseMsg) sub));
          }
        }
      }
    }
    return out;
  }
}
