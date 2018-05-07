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

package org.apache.hadoop.hive.serde2.lazy.objectinspector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.serde2.avro.AvroLazyObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParametersImpl;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.io.Text;

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
public final class LazyObjectInspectorFactory {

  static ConcurrentHashMap<ArrayList<Object>, LazySimpleStructObjectInspector> cachedLazySimpleStructObjectInspector =
      new ConcurrentHashMap<ArrayList<Object>, LazySimpleStructObjectInspector>();

  @Deprecated
  public static LazySimpleStructObjectInspector getLazySimpleStructObjectInspector(
      List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors, byte separator,
      Text nullSequence, boolean lastColumnTakesRest, boolean escaped,
      byte escapeChar) {
    return getLazySimpleStructObjectInspector(structFieldNames,
      structFieldObjectInspectors, null, separator, nullSequence,
      lastColumnTakesRest, escaped, escapeChar, ObjectInspectorOptions.JAVA);
  }

  @Deprecated
  public static LazySimpleStructObjectInspector getLazySimpleStructObjectInspector(
      List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors, byte separator,
      Text nullSequence, boolean lastColumnTakesRest, boolean escaped,
      byte escapeChar, ObjectInspectorOptions option) {
    return getLazySimpleStructObjectInspector(structFieldNames,
      structFieldObjectInspectors, null, separator, nullSequence,
      lastColumnTakesRest, escaped, escapeChar, option);
  }

  @Deprecated
  public static LazySimpleStructObjectInspector getLazySimpleStructObjectInspector(
      List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors, List<String> structFieldComments,
      byte separator, Text nullSequence, boolean lastColumnTakesRest,
      boolean escaped, byte escapeChar) {
    return getLazySimpleStructObjectInspector(structFieldNames, structFieldObjectInspectors,
      structFieldComments, separator, nullSequence, lastColumnTakesRest, escaped, escapeChar,
      ObjectInspectorOptions.JAVA);
  }

  @Deprecated
  public static LazySimpleStructObjectInspector getLazySimpleStructObjectInspector(
      List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors, List<String> structFieldComments,
      byte separator, Text nullSequence, boolean lastColumnTakesRest,
      boolean escaped,byte escapeChar, ObjectInspectorOptions option) {

    return getLazySimpleStructObjectInspector(structFieldNames, structFieldObjectInspectors,
      structFieldComments, separator,
      new LazyObjectInspectorParametersImpl(
          escaped, escapeChar, false, null, null, nullSequence, lastColumnTakesRest),
      option);
  }

  public static LazySimpleStructObjectInspector getLazySimpleStructObjectInspector(
      List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors, List<String> structFieldComments,
      byte separator,
      LazyObjectInspectorParameters lazyParams, ObjectInspectorOptions option) {
    ArrayList<Object> signature = new ArrayList<Object>();
    signature.add(structFieldNames);
    signature.add(structFieldObjectInspectors);
    signature.add(Byte.valueOf(separator));
    signature.add(lazyParams.getNullSequence().toString());
    signature.add(Boolean.valueOf(lazyParams.isLastColumnTakesRest()));
    LazyObjectInspectorFactory.addCommonLazyParamsToSignature(lazyParams, signature);
    signature.add(option);
    if(structFieldComments != null) {
      signature.add(structFieldComments);
    }
    LazySimpleStructObjectInspector result = cachedLazySimpleStructObjectInspector.get(signature);
    if (result == null) {
      switch (option) {
      case JAVA:
        result =
            new LazySimpleStructObjectInspector(structFieldNames, structFieldObjectInspectors,
                structFieldComments, separator, lazyParams);
        break;
      case AVRO:
        result =
            new AvroLazyObjectInspector(structFieldNames, structFieldObjectInspectors,
                structFieldComments, separator, lazyParams);
        break;
      default:
        throw new IllegalArgumentException("Illegal ObjectInspector type [" + option + "]");
      }

      LazySimpleStructObjectInspector prev =
        cachedLazySimpleStructObjectInspector.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static ConcurrentHashMap<ArrayList<Object>, LazyListObjectInspector> cachedLazySimpleListObjectInspector =
      new ConcurrentHashMap<ArrayList<Object>, LazyListObjectInspector>();

  @Deprecated
  public static LazyListObjectInspector getLazySimpleListObjectInspector(
      ObjectInspector listElementObjectInspector, byte separator,
      Text nullSequence, boolean escaped, byte escapeChar) {
    return getLazySimpleListObjectInspector(listElementObjectInspector, separator,
        new LazyObjectInspectorParametersImpl(escaped, escapeChar, false, null, null, nullSequence));
  }

  public static LazyListObjectInspector getLazySimpleListObjectInspector(
      ObjectInspector listElementObjectInspector, byte separator,
      LazyObjectInspectorParameters lazyParams) {
    ArrayList<Object> signature = new ArrayList<Object>();
    signature.add(listElementObjectInspector);
    signature.add(Byte.valueOf(separator));
    signature.add(lazyParams.getNullSequence().toString());
    LazyObjectInspectorFactory.addCommonLazyParamsToSignature(lazyParams, signature);
    LazyListObjectInspector result = cachedLazySimpleListObjectInspector
        .get(signature);
    if (result == null) {
      result = new LazyListObjectInspector(listElementObjectInspector,
          separator, lazyParams);
      LazyListObjectInspector prev =
        cachedLazySimpleListObjectInspector.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static ConcurrentHashMap<ArrayList<Object>, LazyMapObjectInspector> cachedLazySimpleMapObjectInspector =
      new ConcurrentHashMap<ArrayList<Object>, LazyMapObjectInspector>();

  @Deprecated
  public static LazyMapObjectInspector getLazySimpleMapObjectInspector(
      ObjectInspector mapKeyObjectInspector,
      ObjectInspector mapValueObjectInspector, byte itemSeparator,
      byte keyValueSeparator, Text nullSequence, boolean escaped,
      byte escapeChar) {
    return getLazySimpleMapObjectInspector(mapKeyObjectInspector, mapValueObjectInspector,
        itemSeparator, keyValueSeparator,
        new LazyObjectInspectorParametersImpl(escaped, escapeChar, false, null, null, nullSequence));
  }

  public static LazyMapObjectInspector getLazySimpleMapObjectInspector(
      ObjectInspector mapKeyObjectInspector,
      ObjectInspector mapValueObjectInspector, byte itemSeparator,
      byte keyValueSeparator, LazyObjectInspectorParameters lazyParams) {
    ArrayList<Object> signature = new ArrayList<Object>();
    signature.add(mapKeyObjectInspector);
    signature.add(mapValueObjectInspector);
    signature.add(Byte.valueOf(itemSeparator));
    signature.add(Byte.valueOf(keyValueSeparator));
    signature.add(lazyParams.getNullSequence().toString());
    LazyObjectInspectorFactory.addCommonLazyParamsToSignature(lazyParams, signature);
    LazyMapObjectInspector result = cachedLazySimpleMapObjectInspector
        .get(signature);
    if (result == null) {
      result = new LazyMapObjectInspector(mapKeyObjectInspector,
          mapValueObjectInspector, itemSeparator, keyValueSeparator, lazyParams);
      LazyMapObjectInspector prev =
        cachedLazySimpleMapObjectInspector.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static ConcurrentHashMap<List<Object>, LazyUnionObjectInspector>
    cachedLazyUnionObjectInspector =
      new ConcurrentHashMap<List<Object>, LazyUnionObjectInspector>();

  @Deprecated
  public static LazyUnionObjectInspector getLazyUnionObjectInspector(
      List<ObjectInspector> ois, byte separator, Text nullSequence,
      boolean escaped, byte escapeChar) {
    return getLazyUnionObjectInspector(ois, separator,
        new LazyObjectInspectorParametersImpl(escaped, escapeChar, false, null, null, nullSequence));
  }

  public static LazyUnionObjectInspector getLazyUnionObjectInspector(
      List<ObjectInspector> ois, byte separator,
      LazyObjectInspectorParameters lazyParams) {
    List<Object> signature = new ArrayList<Object>();
    signature.add(ois);
    signature.add(Byte.valueOf(separator));
    signature.add(lazyParams.getNullSequence().toString());
    LazyObjectInspectorFactory.addCommonLazyParamsToSignature(lazyParams, signature);
    LazyUnionObjectInspector result = cachedLazyUnionObjectInspector
        .get(signature);
    if (result == null) {
      result = new LazyUnionObjectInspector(ois, separator, lazyParams);
      LazyUnionObjectInspector prev =
        cachedLazyUnionObjectInspector.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  private LazyObjectInspectorFactory() {
    // prevent instantiation
  }

  private static void addCommonLazyParamsToSignature(LazyObjectInspectorParameters lazyParams,
      List<Object> signature) {
    signature.add(lazyParams.isEscaped());
    signature.add(lazyParams.getEscapeChar());
    signature.add(lazyParams.isExtendedBooleanLiteral());
    signature.add(lazyParams.getTimestampFormats());
  }
}
