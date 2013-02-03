package org.apache.hadoop.hive.ql.parse;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.PTFTranslator.PTFTranslationInfo;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLeadLag.GenericUDFLag;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLeadLag.GenericUDFLead;

/*
 * When constructing the Evaluator Tree from an ExprNode Tree
 * - look for any descendant LeadLag Function Expressions
 * - if they are found:
 *   - add them to the LLInfo.leadLagExprs and
 *   - add a mapping from the Expr Tree root to the LLFunc Expr in LLInfo.mapTopExprToLLFunExprs
 */

public class WindowingExprNodeEvaluatorFactory
{
	public static ExprNodeEvaluator get(PTFTranslationInfo tInfo, ExprNodeDesc desc) throws HiveException
	{
		FindLeadLagFuncExprs visitor = new FindLeadLagFuncExprs(tInfo, desc);
		new ExprNodeWalker(visitor).walk(desc);
		return ExprNodeEvaluatorFactory.get(desc);
	}

	public static class FindLeadLagFuncExprs extends ExprNodeVisitor
	{
		ExprNodeDesc topExpr;
		PTFTranslationInfo tInfo;

		FindLeadLagFuncExprs(PTFTranslationInfo tInfo, ExprNodeDesc topExpr)
		{
			this.tInfo = tInfo;
			this.topExpr = topExpr;
		}

		@Override
		public void visit(ExprNodeGenericFuncDesc fnExpr) throws HiveException
		{
			GenericUDF fn = fnExpr.getGenericUDF();
			if (fn instanceof GenericUDFLead || fn instanceof GenericUDFLag )
			{
				tInfo.getLLInfo().addLLFuncExprForTopExpr(topExpr, fnExpr);
			}
		}
	}

	static class ExprNodeVisitor
	{
	  public void visit(ExprNodeColumnDesc e) throws HiveException
	  {
	  }

	  public void visit(ExprNodeConstantDesc e) throws HiveException
	  {
	  }

	  public void visit(ExprNodeFieldDesc e) throws HiveException
	  {
	  }

	  public void visit(ExprNodeGenericFuncDesc e) throws HiveException
	  {
	  }

	  public void visit(ExprNodeNullDesc e) throws HiveException
	  {
	  }
	}

	static class ExprNodeWalker
	{
	  ExprNodeVisitor visitor;

	  public ExprNodeWalker(ExprNodeVisitor visitor)
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

	    if ( e instanceof ExprNodeColumnDesc)
	    {
	      walk((ExprNodeColumnDesc) e);
	    }
	    else if ( e instanceof ExprNodeConstantDesc)
	    {
	      walk((ExprNodeConstantDesc) e);
	    }
	    else if ( e instanceof ExprNodeFieldDesc)
	    {
	      walk((ExprNodeFieldDesc) e);
	    }
	    else if ( e instanceof ExprNodeGenericFuncDesc)
	    {
	      walk((ExprNodeGenericFuncDesc) e);
	    }
	    else if ( e instanceof ExprNodeNullDesc)
	    {
	      walk((ExprNodeNullDesc) e);
	    }
	    else
	    {
	      throw new HiveException("Unknown Expr Type " + e.getClass().getName());
	    }
	  }

	  private void walk(ExprNodeColumnDesc e) throws HiveException
	  {
	    visitor.visit(e);
	  }

	  private void walk(ExprNodeConstantDesc e) throws HiveException
	  {
	    visitor.visit(e);
	  }

	  private void walk(ExprNodeFieldDesc e) throws HiveException
	  {
	    visitor.visit(e);
	  }

	  private void walk(ExprNodeGenericFuncDesc e) throws HiveException
	  {
	    visitor.visit(e);
	  }

	  private void walk(ExprNodeNullDesc e) throws HiveException
	  {
	    visitor.visit(e);
	  }
	}
}
