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
package org.apache.hadoop.hive.ql.testutil;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.hive.ql.txn.compactor.Cleaner;

import java.lang.reflect.Field;
import java.util.Arrays;

import static java.lang.String.format;

/**
 * Various helper methods for mocking.
 * <b>Please use it only in tests, not in production code!</b>
 */
public class ReflectionUtil {

  /**
   * Sets a declared field in a given object.
   * Note: if you want to modify a field in a super class, use the {@link ReflectionUtil#setInAllFields } method.
   * @param object target instance
   * @param field name of the field to set
   * @param value new value
   * @throws RuntimeException in case the field is not found or cannot be set.
   */
  public static void setField(Object object, String field, Object value) {
    try {
      Field fieldToChange = object.getClass().getDeclaredField(field);
      fieldToChange.setAccessible(true);
      fieldToChange.set(object, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(format("Cannot set field %s in object %s", field, object.getClass()));
    }
  }

  /**
   * Sets a declared field in a given object. It finds the field, even if it is declared in a super class.
   * @param object target instance
   * @param field name of the field to set
   * @param value new value
   * @throws RuntimeException in case the field is not found or cannot be set.
   */
  public static void setInAllFields(Object object, String field, Object value) {
    try {
      Field fieldToChange = Arrays.stream(FieldUtils.getAllFields(object.getClass()))
              .filter(f -> f.getName().equals(field))
              .findFirst()
              .orElseThrow(NoSuchFieldException::new);

      fieldToChange.setAccessible(true);

      fieldToChange.set(object, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(format("Cannot set field %s in object %s", field, object.getClass()));
    }
  }
}
