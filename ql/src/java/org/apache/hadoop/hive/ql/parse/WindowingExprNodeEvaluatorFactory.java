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

import java.util.List;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLag;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLead;

/*
 * When constructing the Evaluator Tree from an ExprNode Tree
 * - look for any descendant LeadLag Function Expressions
 * - if they are found:
 *   - add them to the LLInfo.leadLagExprs and
 *   - add a mapping from the Expr Tree root to the LLFunc Expr in LLInfo.mapTopExprToLLFunExprs
 */
public class WindowingExprNodeEvaluatorFactory {

  public static ExprNodeEvaluator get(LeadLagInfo llInfo, ExprNodeDesc desc) throws HiveException
  {
    FindLeadLagFuncExprs visitor = new FindLeadLagFuncExprs(llInfo, desc);
    new ExprNodeWalker(visitor).walk(desc);
    return ExprNodeEvaluatorFactory.get(desc);
  }

  public static class FindLeadLagFuncExprs
  {
    ExprNodeDesc topExpr;
    LeadLagInfo llInfo;

    FindLeadLagFuncExprs(LeadLagInfo llInfo, ExprNodeDesc topExpr)
    {
      this.llInfo = llInfo;
      this.topExpr = topExpr;
    }

    public void visit(ExprNodeGenericFuncDesc fnExpr) throws HiveException
    {
      GenericUDF fn = fnExpr.getGenericUDF();
      if (fn instanceof GenericUDFLead || fn instanceof GenericUDFLag )
      {
        llInfo.addLLFuncExprForTopExpr(topExpr, fnExpr);
      }
    }
  }

  static class ExprNodeWalker
  {
    FindLeadLagFuncExprs visitor;

    public ExprNodeWalker(FindLeadLagFuncExprs visitor)
    {
      super();
      this.visitor = visitor;
    }

    public void walk(ExprNodeDesc e) throws HiveException
    {
      if ( e == null ) {
        return;
      }
      List<ExprNodeDesc>  children = e.getChildren();
      if ( children != null )
      {
        for(ExprNodeDesc child : children)
        {
          walk(child);
        }
      }

      if ( e instanceof ExprNodeGenericFuncDesc)
      {
        visitor.visit((ExprNodeGenericFuncDesc)e);
      }
    }
  }
}
