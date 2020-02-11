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

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

class Reflector {
  private final BeeLine beeLine;

  public Reflector(BeeLine beeLine) {
    this.beeLine = beeLine;
  }

  public Object invoke(Object on, String method, Object[] args)
      throws InvocationTargetException, IllegalAccessException,
      ClassNotFoundException {
    return invoke(on, method, Arrays.asList(args));
  }

  public Object invoke(Object on, String method, List args)
      throws InvocationTargetException, IllegalAccessException,
      ClassNotFoundException {
    return invoke(on, on == null ? null : on.getClass(), method, args);
  }


  public Object invoke(Object on, Class defClass,
      String method, List args)
      throws InvocationTargetException, IllegalAccessException,
      ClassNotFoundException {
    Class c = defClass != null ? defClass : on.getClass();
    List<Method> candidateMethods = new LinkedList<Method>();

    Method[] m = c.getMethods();
    for (int i = 0; i < m.length; i++) {
      if (m[i].getName().equalsIgnoreCase(method)) {
        candidateMethods.add(m[i]);
      }
    }

    if (candidateMethods.size() == 0) {
      throw new IllegalArgumentException(beeLine.loc("no-method",
          new Object[] {method, c.getName()}));
    }

    for (Iterator<Method> i = candidateMethods.iterator(); i.hasNext();) {
      Method meth = i.next();
      Class[] ptypes = meth.getParameterTypes();
      if (!(ptypes.length == args.size())) {
        continue;
      }

      Object[] converted = convert(args, ptypes);
      if (converted == null) {
        continue;
      }

      if (!Modifier.isPublic(meth.getModifiers())) {
        continue;
      }
      return meth.invoke(on, converted);
    }
    return null;
  }


  public static Object[] convert(List objects, Class[] toTypes)
      throws ClassNotFoundException {
    Object[] converted = new Object[objects.size()];
    for (int i = 0; i < converted.length; i++) {
      converted[i] = convert(objects.get(i), toTypes[i]);
    }
    return converted;
  }


  public static Object convert(Object ob, Class toType)
      throws ClassNotFoundException {
    if (ob == null || ob.toString().equals("null")) {
      return null;
    }
    if (toType == String.class) {
      return new String(ob.toString());
    } else if (toType == Map.class) {
      String[] vars = ob.toString().split(",");
      Map<String, String> keyValMap = new HashMap<>();
      for (String keyValStr : vars) {
        String[] keyVal = keyValStr.trim().split("=", 2);
        if (keyVal.length == 2) {
          keyValMap.put(keyVal[0], keyVal[1]);
        }
      }
      return keyValMap;
    } else if (toType == Byte.class || toType == byte.class) {
      return Byte.valueOf(ob.toString());
    } else if (toType == Character.class || toType == char.class) {
      return Character.valueOf(ob.toString().charAt(0));
    } else if (toType == Short.class || toType == short.class) {
      return Short.valueOf(ob.toString());
    } else if (toType == Integer.class || toType == int.class) {
      return Integer.valueOf(ob.toString());
    } else if (toType == Long.class || toType == long.class) {
      return Long.valueOf(ob.toString());
    } else if (toType == Double.class || toType == double.class) {
      return Double.valueOf(ob.toString());
    } else if (toType == Float.class || toType == float.class) {
      return Float.valueOf(ob.toString());
    } else if (toType == Boolean.class || toType == boolean.class) {
      return Boolean.valueOf(ob.toString().equals("true")
          || ob.toString().equals(true + "")
          || ob.toString().equals("1")
          || ob.toString().equals("on")
          || ob.toString().equals("yes"));
    } else if (toType == Class.class) {
      return Class.forName(ob.toString());
    }
    return null;
  }
}
