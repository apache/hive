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

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import com.google.common.collect.Lists;

public class TypeCheckProcFactoryUtils {

  static ArrayList<ExprNodeDesc> rewriteInToOR(ArrayList<ExprNodeDesc> inOperands) throws SemanticException {
    ExprNodeDesc columnDesc = inOperands.get(0);

    ArrayList<ExprNodeDesc> orOperands = new ArrayList<>();
    for (int i = 1; i < inOperands.size(); i++) {
      ExprNodeDesc andExpr = buildEqualsArr(columnDesc, inOperands.get(i));
      if (andExpr == null) {
        return null;
      }
      orOperands.add(andExpr);
    }
    return orOperands;
  }

  private static ExprNodeDesc buildEqualsArr(ExprNodeDesc columnDesc, ExprNodeDesc exprNodeDesc)
      throws SemanticException {
    List<ExprNodeDesc> lNodes = asListOfNodes(columnDesc);
    List<ExprNodeDesc> rNodes = asListOfNodes(exprNodeDesc);
    if (lNodes == null || rNodes == null) {
      // something went wrong
      return null;
    }
    if (lNodes.size() != rNodes.size()) {
      throw new SemanticException(ErrorMsg.INCOMPATIBLE_STRUCT.getMsg(columnDesc + " and " + exprNodeDesc));
    }

    List<ExprNodeDesc> ret = new ArrayList<>();
    for (int i = 0; i < lNodes.size(); i++) {
      ret.add(buildEquals(lNodes.get(i), rNodes.get(i)));
    }
    return buildAnd(ret);
  }

  private static ExprNodeGenericFuncDesc buildEquals(ExprNodeDesc columnDesc, ExprNodeDesc valueDesc) {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFOPEqual(), "=",
        Lists.newArrayList(columnDesc, valueDesc));
  }

  private static ExprNodeDesc buildAnd(List<ExprNodeDesc> values) {
    if (values.size() == 1) {
      return values.get(0);
    } else {
      return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFOPAnd(), "and", values);
    }
  }

  private static List<ExprNodeDesc> asListOfNodes(ExprNodeDesc desc) {
    ExprNodeDesc valueDesc = desc;
    if (ExprNodeDescUtils.isStructUDF(desc)) {
      List<ExprNodeDesc> valueChilds = ((ExprNodeGenericFuncDesc) valueDesc).getChildren();
      for (ExprNodeDesc exprNodeDesc : valueChilds) {
        if (!isSafeExpression(exprNodeDesc)) {
          return null;
        }
      }
      return valueChilds;
    }
    if (ExprNodeDescUtils.isConstantStruct(valueDesc)) {
      ExprNodeConstantDesc valueConstDesc = (ExprNodeConstantDesc) valueDesc;
      List<Object> oldValues = (List<Object>) valueConstDesc.getValue();
      StructTypeInfo structTypeInfo = (StructTypeInfo) valueConstDesc.getTypeInfo();
      ArrayList<TypeInfo> structFieldInfos = structTypeInfo.getAllStructFieldTypeInfos();

      List<ExprNodeDesc> ret = new ArrayList<>();
      for (int i = 0; i < oldValues.size(); i++) {
        ret.add(new ExprNodeConstantDesc(structFieldInfos.get(i), oldValues.get(i)));
      }
      return ret;
    }
    if (isSafeExpression(desc)) {
      return Lists.newArrayList(desc);
    }

    return null;
  }

  private static boolean isSafeExpression(ExprNodeDesc desc) {
    TypeInfo typeInfo = desc.getTypeInfo();
    if (typeInfo.getCategory() != Category.PRIMITIVE) {
      return false;
    }
    if (isConstantOrColumn(desc)) {
      return true;
    }
    if (desc instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc exprNodeGenericFuncDesc = (ExprNodeGenericFuncDesc) desc;
      if (FunctionRegistry.isConsistentWithinQuery(exprNodeGenericFuncDesc.getGenericUDF())) {
        for (ExprNodeDesc child : exprNodeGenericFuncDesc.getChildren()) {
          if (!isSafeExpression(child)) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }

  private static boolean isConstantOrColumn(ExprNodeDesc desc) {
    return desc instanceof ExprNodeColumnDesc || desc instanceof ExprNodeConstantDesc;
  }

}
