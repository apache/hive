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
package org.apache.hadoop.hive.serde2.objectinspector;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;


/**
 * PrimitiveObjectInspector.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface PrimitiveObjectInspector extends ObjectInspector {

  /**
   * The primitive types supported by Hive.
   */
  public static enum PrimitiveCategory {
    VOID, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, STRING,
    DATE, TIMESTAMP, TIMESTAMPLOCALTZ, BINARY, DECIMAL, VARCHAR, CHAR,
    INTERVAL_YEAR_MONTH, INTERVAL_DAY_TIME, UNKNOWN
  };

  public PrimitiveTypeInfo getTypeInfo();

  /**
   * Get the primitive category of the PrimitiveObjectInspector.
   */
  PrimitiveCategory getPrimitiveCategory();

  /**
   * Get the Primitive Writable class which is the return type of
   * getPrimitiveWritableObject() and copyToPrimitiveWritableObject().
   */
  Class<?> getPrimitiveWritableClass();

  /**
   * Return the data in an instance of primitive writable Object. If the Object
   * is already a primitive writable Object, just return o.
   */
  Object getPrimitiveWritableObject(Object o);

  /**
   * Get the Java Primitive class which is the return type of
   * getJavaPrimitiveObject().
   */
  Class<?> getJavaPrimitiveClass();

  /**
   * Get the Java Primitive object.
   */
  Object getPrimitiveJavaObject(Object o);

  /**
   * Get a copy of the Object in the same class, so the return value can be
   * stored independently of the parameter.
   *
   * If the Object is a Primitive Java Object, we just return the parameter
   * since Primitive Java Object is immutable.
   */
  Object copyObject(Object o);

  /**
   * Whether the ObjectInspector prefers to return a Primitive Writable Object
   * instead of a Primitive Java Object. This can be useful for determining the
   * most efficient way to getting data out of the Object.
   */
  boolean preferWritable();

  /**
   * The precision of the underlying data.
   */
  int precision();

  /**
   * The scale of the underlying data.
   */
  int scale();

}
