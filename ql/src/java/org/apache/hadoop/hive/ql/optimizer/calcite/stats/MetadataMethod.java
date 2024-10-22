/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.util.ImmutableBitSet;

import java.lang.reflect.Method;

public enum MetadataMethod {
  AGGREGATED_COLUMNS(AggregatedColumns.class, "areColumnsAggregated", ImmutableBitSet.class);
  private final Method method;

  MetadataMethod(Class<? extends Metadata> metaClass, String name, Class<?> argClass) {
    this.method = Types.lookupMethod(metaClass, name, argClass);
  }

  public Method method() {
    return method;
  }
}
