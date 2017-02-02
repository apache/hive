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

package org.apache.hadoop.hive.ql.io.sarg;

import org.apache.hadoop.conf.Configuration;

/**
 * A factory for creating SearchArguments, as well as modifying those created by this factory.
 */
public class SearchArgumentFactory {
  public static SearchArgument.Builder newBuilder() {
    return newBuilder(null);
  }

  public static SearchArgument.Builder newBuilder(Configuration conf) {
    return new SearchArgumentImpl.BuilderImpl(conf);
  }

  public static void setPredicateLeafColumn(PredicateLeaf leaf, String newName) {
    SearchArgumentImpl.PredicateLeafImpl.setColumnName(leaf, newName);
  }
}
