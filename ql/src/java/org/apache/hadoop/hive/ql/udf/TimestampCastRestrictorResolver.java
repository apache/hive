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

package org.apache.hadoop.hive.ql.udf;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFMethodResolver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Restricts casting timestamp/date types to numeric values.
 *
 * This Resolver is used in {@link UDF} implementations to enforce strict conversion rules.
 */
public class TimestampCastRestrictorResolver implements UDFMethodResolver {

  private UDFMethodResolver parentResolver;
  private boolean strictTsConversion;

  public TimestampCastRestrictorResolver(UDFMethodResolver parentResolver) {
    this.parentResolver = parentResolver;
    SessionState ss = SessionState.get();
    if (ss != null && ss.getConf().getBoolVar(ConfVars.HIVE_STRICT_TIMESTAMP_CONVERSION)) {
      strictTsConversion = true;
    }
  }

  @Override
  public Method getEvalMethod(List<TypeInfo> argClasses) throws UDFArgumentException {
    if (strictTsConversion) {
      TypeInfo arg = argClasses.get(0);
      if (arg instanceof PrimitiveTypeInfo) {
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) arg;
        PrimitiveCategory category = primitiveTypeInfo.getPrimitiveCategory();
        PrimitiveGrouping group = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(category);
        if (group == PrimitiveGrouping.DATE_GROUP) {
          throw new UDFArgumentException(
              "Casting DATE/TIMESTAMP types to NUMERIC is prohibited (" + ConfVars.HIVE_STRICT_TIMESTAMP_CONVERSION
                  + ")");
        }
      }
    }
    return parentResolver.getEvalMethod(argClasses);
  }

}
