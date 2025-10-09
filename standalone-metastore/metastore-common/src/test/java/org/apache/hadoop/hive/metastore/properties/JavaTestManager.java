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
package org.apache.hadoop.hive.metastore.properties;

/**
 * A test manager that handles 3 schemas (domain, package, clazz).
 */
public class JavaTestManager extends PropertyManager {
  static final PropertySchema DOMAIN = new PropertySchema("domain");
  static final PropertySchema PACKAGE = new PropertySchema("package");
  static final PropertySchema CLAZZ = new PropertySchema("clazz");
  static final PropertySchema[] SCHEMAS = new PropertySchema[]{DOMAIN, PACKAGE, CLAZZ};

  /** Declare JavaTestManager. */
  public static void initialize() {
    PropertyManager.declare("jtm", JavaTestManager.class);
  }

  JavaTestManager(PropertyStore store) {
    this("jtm", store);
  }

  public JavaTestManager(String ns, PropertyStore store) {
    super(ns, store);
  }

  @Override
  public PropertySchema getSchema(String schemaName) {
    switch (schemaName) {
      case "domain":  return DOMAIN;
      case "package": return PACKAGE;
      case "clazz": return CLAZZ;
      default:  return null;
    }
  }

  /**
   * 1 key is domain, 2 keys is package, 3 or more is clazz.
   * @param keys the key fragments
   * @return
   */
  @Override
  protected PropertySchema schemaOf(String[] keys) {
    return keys.length > 0 && keys.length < SCHEMAS.length ? SCHEMAS[keys.length - 1] : CLAZZ;
  }

  @Override
  protected int getMapNameLength(String[] keys) {
    return SCHEMAS.length;
  }

  /**
   * Declares a new domain property.
   * @param name the property name
   * @param type the property type
   * @param defaultValue the property default value or null if none
   */
  public static void declareDomainProperty(String name, PropertyType type, Object defaultValue) {
    DOMAIN.declareProperty(name, type, defaultValue);
  }

  /**
   * Declares a new package property.
   * @param name the property name
   * @param type the property type
   * @param defaultValue the property default value or null if none
   */
  public static void declarePackageProperty(String name, PropertyType type, Object defaultValue) {
    PACKAGE.declareProperty(name, type, defaultValue);
  }

  /**
   * Declares a new clazz property.
   * @param name the property name
   * @param type the property type
   * @param defaultValue the property default value or null if none
   */
  public static void declareClazzProperty(String name, PropertyType type, Object defaultValue) {
    CLAZZ.declareProperty(name, type, defaultValue);
  }
}
