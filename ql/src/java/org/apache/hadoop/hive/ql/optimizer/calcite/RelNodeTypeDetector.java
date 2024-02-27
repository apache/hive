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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;

/**
 * A class for checking if a {@link RelNode} plan contains a node with the specified type.
 */
public class RelNodeTypeDetector extends RelHomogeneousShuttle {
  private Class<?> searchType;
  private boolean found = false;

  private RelNodeTypeDetector(Class<?> searchType) {
    this.searchType = searchType;
  }

  @Override public RelNode visit(RelNode other) {
    if(searchType.isAssignableFrom(other.getClass())) {
      found = true;
      return other;
    }
    return visitChildren(other);
  }

  public static boolean contains(RelNode plan, Class<?> type) {
    RelNodeTypeDetector detector = new RelNodeTypeDetector(type);
    plan.accept(detector);
    return detector.found;
  }
}
