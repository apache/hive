/*
 *  Copyright (c) 2002,2003,2004,2005 Marc Prud'hommeaux
 *  All rights reserved.
 *
 *
 *  Redistribution and use in source and binary forms,
 *  with or without modification, are permitted provided
 *  that the following conditions are met:
 *
 *  Redistributions of source code must retain the above
 *  copyright notice, this list of conditions and the following
 *  disclaimer.
 *  Redistributions in binary form must reproduce the above
 *  copyright notice, this list of conditions and the following
 *  disclaimer in the documentation and/or other materials
 *  provided with the distribution.
 *  Neither the name of the <ORGANIZATION> nor the names
 *  of its contributors may be used to endorse or promote
 *  products derived from this software without specific
 *  prior written permission.
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS
 *  AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 *  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 *  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 *  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 *  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 *  OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 *  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 *  IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 *  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *  This software is hosted by SourceForge.
 *  SourceForge is a trademark of VA Linux Systems, Inc.
 */

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * The license above originally appeared in src/sqlline/SqlLine.java
 * http://sqlline.sourceforge.net/
 */
package org.apache.hive.beeline;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
    } else if (toType == Byte.class || toType == byte.class) {
      return new Byte(ob.toString());
    } else if (toType == Character.class || toType == char.class) {
      return new Character(ob.toString().charAt(0));
    } else if (toType == Short.class || toType == short.class) {
      return new Short(ob.toString());
    } else if (toType == Integer.class || toType == int.class) {
      return new Integer(ob.toString());
    } else if (toType == Long.class || toType == long.class) {
      return new Long(ob.toString());
    } else if (toType == Double.class || toType == double.class) {
      return new Double(ob.toString());
    } else if (toType == Float.class || toType == float.class) {
      return new Float(ob.toString());
    } else if (toType == Boolean.class || toType == boolean.class) {
      return new Boolean(ob.toString().equals("true")
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
