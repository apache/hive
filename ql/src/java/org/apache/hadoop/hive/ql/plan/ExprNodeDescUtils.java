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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class ExprNodeDescUtils {

  public static int indexOf(ExprNodeDesc origin, List<ExprNodeDesc> sources) {
    for (int i = 0; i < sources.size(); i++) {
      if (origin.isSame(sources.get(i))) {
        return i;
      }
    }
    return -1;
  }

  // traversing origin, find ExprNodeDesc in sources and replaces it with ExprNodeDesc
  // in targets having same index.
  // return null if failed to find
  public static ExprNodeDesc replace(ExprNodeDesc origin,
      List<ExprNodeDesc> sources, List<ExprNodeDesc> targets) {
    int index = indexOf(origin, sources);
    if (index >= 0) {
      return targets.get(index);
    }
    // encountered column or field which cannot be found in sources
    if (origin instanceof ExprNodeColumnDesc || origin instanceof ExprNodeFieldDesc) {
      return null;
    }
    // for ExprNodeGenericFuncDesc, it should be deterministic and stateless
    if (origin instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) origin;
      if (!FunctionRegistry.isDeterministic(func.getGenericUDF())
          || FunctionRegistry.isStateful(func.getGenericUDF())) {
        return null;
      }
      List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
      for (int i = 0; i < origin.getChildren().size(); i++) {
        ExprNodeDesc child = replace(origin.getChildren().get(i), sources, targets);
        if (child == null) {
          return null;
        }
        children.add(child);
      }
      // duplicate function with possibily replaced children
      ExprNodeGenericFuncDesc clone = (ExprNodeGenericFuncDesc) func.clone();
      clone.setChildExprs(children);
      return clone;
    }
    // constant or null, just return it
    return origin;
  }

  /**
   * return true if predicate is already included in source
    */
  public static boolean containsPredicate(ExprNodeDesc source, ExprNodeDesc predicate) {
    if (source.isSame(predicate)) {
      return true;
    }
    if (FunctionRegistry.isOpAnd(source)) {
      if (containsPredicate(source.getChildren().get(0), predicate) ||
          containsPredicate(source.getChildren().get(1), predicate)) {
        return true;
      }
    }
    return false;
  }

  /**
   * bind two predicates by AND op
    */
  public static ExprNodeDesc mergePredicates(ExprNodeDesc prev, ExprNodeDesc next) {
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(2);
    children.add(prev);
    children.add(next);
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        FunctionRegistry.getGenericUDFForAnd(), children);
  }
}
