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

package org.apache.hadoop.hive.ql.optimizer.signature;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Enables to calculate the signature of an object.
 *
 * If the object has methods annotated with {@link Signature}, they will be used.
 * If the object has no methods marked with the annotation;
 * the object itself is used in the signature to prevent incorrect matches.
 */
public final class SignatureUtils {

  private static Map<Class<?>, SignatureMapper> mappers = new HashMap<>();

  public static void write(Map<String, Object> ret, Object o) {
    if (o == null) {
      ret.put("__null", "true");
    } else {
      SignatureMapper mapper = getSigMapper(o.getClass());
      mapper.write(ret, o);
    }
  }

  /** Prevent construction. */
  private SignatureUtils() {
  }

  static class SignatureMapper {

    private List<Method> sigMethods;

    private String classLabel;

    SignatureMapper(Class<?> o) {
      Method[] f = o.getMethods();
      sigMethods = new ArrayList<>();
      for (Method method : f) {
        if (method.isAnnotationPresent(Signature.class)) {
          sigMethods.add(method);
        }
      }
      classLabel = o.getName();
    }

    public void write(Map<String, Object> ret, Object o) {
      if (sigMethods.isEmpty()) {
        // by supplying using "o" this enforces identity/equls matching
        // which will most probably make the signature very unique
        ret.put(classLabel, System.identityHashCode(o));
      } else {
        ret.put(classLabel, "1");
        for (Method method : sigMethods) {
          try {
            Object res = method.invoke(o);
            ret.put(method.getName(), res);
          } catch (Exception e) {
            throw new RuntimeException("Error invoking signature method", e);
          }
        }
      }
    }

  }

  private static SignatureMapper getSigMapper(Class<?> o) {
    SignatureMapper m = mappers.get(o);
    if (m == null) {
      m = new SignatureMapper(o);
      mappers.put(o, m);
    }
    return m;
  }

}
