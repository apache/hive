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
package org.apache.hadoop.hive.serde2.lazybinary.objectinspector;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * ObjectInspectorFactory is the primary way to create new ObjectInspector
 * instances.
 *
 * SerDe classes should call the static functions in this library to create an
 * ObjectInspector to return to the caller of SerDe2.getObjectInspector().
 *
 * The reason of having caches here is that ObjectInspectors do not have an
 * internal state - so ObjectInspectors with the same construction parameters
 * should result in exactly the same ObjectInspector.
 */

public final class LazyBinaryObjectInspectorFactory {

  static ConcurrentHashMap<List<Object>, LazyBinaryStructObjectInspector> cachedLazyBinaryStructObjectInspector =
      new ConcurrentHashMap<>();

  static ConcurrentHashMap<List<Object>, LazyBinaryUnionObjectInspector> cachedLazyBinaryUnionObjectInspector =
          new ConcurrentHashMap<>();

  public static LazyBinaryStructObjectInspector getLazyBinaryStructObjectInspector(
      List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors) {
    return getLazyBinaryStructObjectInspector(structFieldNames,
                                              structFieldObjectInspectors, null);
  }

  public static LazyBinaryStructObjectInspector getLazyBinaryStructObjectInspector(
      List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors, List<String> structFieldComments) {
    List<Object> signature = structFieldComments == null ? Arrays.asList(structFieldNames, structFieldObjectInspectors)
        : Arrays.asList(structFieldNames, structFieldObjectInspectors, structFieldComments);

    LazyBinaryStructObjectInspector result = cachedLazyBinaryStructObjectInspector
        .get(signature);
    if (result == null) {
      result = new LazyBinaryStructObjectInspector(structFieldNames,
          structFieldObjectInspectors, structFieldComments);
      LazyBinaryStructObjectInspector prev =
        cachedLazyBinaryStructObjectInspector.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  public static LazyBinaryUnionObjectInspector getLazyBinaryUnionObjectInspector(
          List<ObjectInspector> unionFieldObjectInspectors) {
    List<Object> signature = Collections.singletonList(unionFieldObjectInspectors);

    LazyBinaryUnionObjectInspector result = cachedLazyBinaryUnionObjectInspector
            .get(signature);
    if (result == null) {
      result = new LazyBinaryUnionObjectInspector(unionFieldObjectInspectors);
      LazyBinaryUnionObjectInspector prev =
        cachedLazyBinaryUnionObjectInspector.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static ConcurrentHashMap<List<Object>, LazyBinaryListObjectInspector> cachedLazyBinaryListObjectInspector =
      new ConcurrentHashMap<>();

  public static LazyBinaryListObjectInspector getLazyBinaryListObjectInspector(
      ObjectInspector listElementObjectInspector) {
    List<Object> signature = Collections.singletonList(listElementObjectInspector);
    LazyBinaryListObjectInspector result = cachedLazyBinaryListObjectInspector
        .get(signature);
    if (result == null) {
      result = new LazyBinaryListObjectInspector(listElementObjectInspector);
      LazyBinaryListObjectInspector prev =
        cachedLazyBinaryListObjectInspector.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static ConcurrentHashMap<List<Object>, LazyBinaryMapObjectInspector> cachedLazyBinaryMapObjectInspector =
      new ConcurrentHashMap<>();

  public static LazyBinaryMapObjectInspector getLazyBinaryMapObjectInspector(
      ObjectInspector mapKeyObjectInspector,
      ObjectInspector mapValueObjectInspector) {
    List<Object> signature = Arrays.asList(mapKeyObjectInspector, mapValueObjectInspector);
    LazyBinaryMapObjectInspector result = cachedLazyBinaryMapObjectInspector
        .get(signature);
    if (result == null) {
      result = new LazyBinaryMapObjectInspector(mapKeyObjectInspector,
          mapValueObjectInspector);
      LazyBinaryMapObjectInspector prev =
        cachedLazyBinaryMapObjectInspector.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  private LazyBinaryObjectInspectorFactory() {
    // prevent instantiation
  }
}
