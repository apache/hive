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
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.util.List;

public class MessageAnonymizer implements Anonymizer {

  public MessageAnonymizer() {
  }

  @Override
  public Object anonymize(final Object msg, final DataErasureStatement statement) {
    try {
      if (msg instanceof BaseMsg) {
        for (final DataErasureRule rule : statement.rules) {
          final PathStep[] steps = PathStep.compile(rule.path);
          anonymizeInternal((BaseMsg) msg, steps, 0, steps.length - 1, rule);
        }
        return msg;
      } else {
        throw new RuntimeException("Unsupported type: " + msg.getClass().getName());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void anonymizeInternal(final BaseMsg msg, final PathStep[] steps, final int ix, final int limit,
                                 final DataErasureRule rule)
    throws InvocationTargetException, IllegalAccessException {

    final PathStep step = steps[ix];
    final String fieldName = step.field;

    if ("*".equals(fieldName)) {
      if (ix != limit) {
        throw new RuntimeException("'*' wildcard must be the terminal path step: " + rule.path);
      }
      eraseSubObjectRecursively(msg, rule);
      return;
    }

    final Class<? extends BaseMsg> clazz = msg.getClass();

    Method setterMethod = null;
    Method getterMethod = null;
    Method[] methods = clazz.getMethods();
    for (Method method : methods) {
      String methodName = method.getName();
      int modifiers = method.getModifiers();
      if (!Modifier.isPublic(modifiers) || Modifier.isStatic(modifiers)) {
        continue;
      }
      if (methodName.equalsIgnoreCase("get" + fieldName)) {
        getterMethod = method;
        continue;
      }
      if (methodName.equalsIgnoreCase("set" + fieldName)) {
        setterMethod = method;
        Parameter[] parameters = method.getParameters();
      }
    }

    if (getterMethod == null || setterMethod == null) {
      throw new RuntimeException("methods not found for field: " + fieldName);
    }

    Class<?> rtc = getterMethod.getReturnType();

    switch (rtc.getSimpleName()) {
      case "int":
      case "short":
      case "long":
      case "String": {
        setterMethod.invoke(msg, RuleValues.scalarValueFor(rule, rtc.getSimpleName()));
        break;
      }
      case "List": {
        try {
          Field f = clazz.getDeclaredField(fieldName);
          ParameterizedType pt = (ParameterizedType) f.getGenericType();
          Class<?> ptc = (Class<?>) pt.getActualTypeArguments()[0];

          switch (ptc.getSimpleName()) {
            case "String": {
              final String elem = RuleValues.stringListElementValueFor(rule);
              List lst = (List) getterMethod.invoke(msg);
              if (lst != null) {
                for (final int i : step.selectIndices(lst)) {
                  lst.set(i, elem);
                }
              }
              return;
            }
          }

          if (!BaseMsg.class.isAssignableFrom(ptc)) {
            throw new RuntimeException("bad generic type" + pt);
          }
          if (ix >= limit) {
            throw new RuntimeException(
                "path terminates on a List of sub-objects without a ':*' wildcard: " + rule.path);
          }
          List lst = (List) getterMethod.invoke(msg);
          if (lst == null) {
            return;
          }
          for (final int i : step.selectIndices(lst)) {
            final Object o = lst.get(i);
            if (o != null) {
              anonymizeInternal((BaseMsg) o, steps, ix + 1, limit, rule);
            }
          }
        } catch (NoSuchFieldException e) {
          throw new RuntimeException(e);
        }
        break;
      }
      default: {
        if (!BaseMsg.class.isAssignableFrom(rtc)) {
          throw new RuntimeException("bad field type detected: " + rtc);
        }
        if (ix >= limit) {
          throw new RuntimeException(
              "path terminates on a sub-object without a ':*' wildcard: " + rule.path);
        }
        Object sub = getterMethod.invoke(msg);
        if (sub == null) {
          return;
        }
        anonymizeInternal((BaseMsg) sub, steps, ix + 1, limit, rule);
      }
    }
  }

  private void eraseSubObjectRecursively(final BaseMsg obj, final DataErasureRule rule)
    throws InvocationTargetException, IllegalAccessException {

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
      Method setter = null;
      for (final Method m : methods) {
        if (m.getName().equals(setterName) && m.getParameterCount() == 1
            && Modifier.isPublic(m.getModifiers()) && !Modifier.isStatic(m.getModifiers())) {
          setter = m;
          break;
        }
      }
      if (setter == null) {
        continue;
      }

      final Class<?> rtc = getter.getReturnType();
      switch (rtc.getSimpleName()) {
        case "int":
        case "short":
        case "long":
        case "String": {
          setter.invoke(obj, RuleValues.scalarValueFor(rule, rtc.getSimpleName()));
          break;
        }
        case "List": {
          final String fieldName = Character.toLowerCase(name.charAt(3)) + name.substring(4);
          try {
            final Field f = clazz.getDeclaredField(fieldName);
            final ParameterizedType pt = (ParameterizedType) f.getGenericType();
            final Class<?> ptc = (Class<?>) pt.getActualTypeArguments()[0];
            if (ptc.getSimpleName().equals("String")) {
              final String elem = RuleValues.stringListElementValueFor(rule);
              final List lst = (List) getter.invoke(obj);
              if (lst != null) {
                for (int i = 0; i < lst.size(); i++) {
                  lst.set(i, elem);
                }
              }
            } else if (BaseMsg.class.isAssignableFrom(ptc)) {
              final List lst = (List) getter.invoke(obj);
              if (lst != null) {
                for (final Object o : lst) {
                  if (o != null) {
                    eraseSubObjectRecursively((BaseMsg) o, rule);
                  }
                }
              }
            }
          } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
          }
          break;
        }
        default: {
          if (BaseMsg.class.isAssignableFrom(rtc)) {
            final Object sub = getter.invoke(obj);
            if (sub != null) {
              eraseSubObjectRecursively((BaseMsg) sub, rule);
            }
          }
        }
      }
    }
  }

}
