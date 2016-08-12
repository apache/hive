/**
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
package org.apache.hadoop.hive.ql.optimizer.calcite.druid;

import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * Schema mapped onto a Druid instance.
 *
 * TODO: to be removed when Calcite is upgraded to 1.9
 */
public class DruidSchema extends AbstractSchema {
  final String url;

  /**
   * Creates a Druid schema.
   *
   * @param url URL of query REST service
   */
  public DruidSchema(String url) {
    this.url = Preconditions.checkNotNull(url);
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    return builder.build();
  }
}

// End DruidSchema.java