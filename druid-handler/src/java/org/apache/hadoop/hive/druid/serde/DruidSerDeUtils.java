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
package org.apache.hadoop.hive.druid.serde;

import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils class for Druid SerDe.
 */
public final class DruidSerDeUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DruidSerDeUtils.class);

  protected static final String FLOAT_TYPE = "FLOAT";
  protected static final String DOUBLE_TYPE = "DOUBLE";
  protected static final String LONG_TYPE = "LONG";
  protected static final String STRING_TYPE = "STRING";

  /* This method converts from the String representation of Druid type
   * to the corresponding Hive type */
  public static PrimitiveTypeInfo convertDruidToHiveType(String typeName) {
    typeName = typeName.toUpperCase();
    switch (typeName) {
      case FLOAT_TYPE:
        return TypeInfoFactory.floatTypeInfo;
      case DOUBLE_TYPE:
        return TypeInfoFactory.doubleTypeInfo;
      case LONG_TYPE:
        return TypeInfoFactory.longTypeInfo;
      case STRING_TYPE:
        return TypeInfoFactory.stringTypeInfo;
      default:
        // This is a guard for special Druid types e.g. hyperUnique
        // (http://druid.io/docs/0.9.1.1/querying/aggregations.html#hyperunique-aggregator).
        // Currently, we do not support doing anything special with them in Hive.
        // However, those columns are there, and they can be actually read as normal
        // dimensions e.g. with a select query. Thus, we print the warning and just read them
        // as String.
        LOG.warn("Transformation to STRING for unknown type " + typeName);
        return TypeInfoFactory.stringTypeInfo;
    }
  }

}
