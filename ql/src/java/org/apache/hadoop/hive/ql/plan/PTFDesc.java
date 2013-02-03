package org.apache.hadoop.hive.ql.plan;

@Explain(displayName = "PTF Operator")
public class PTFDesc extends AbstractOperatorDesc
{

	private static final long serialVersionUID = 1L;
	String queryDefStr;

	public PTFDesc()
	{

	}

	public PTFDesc(String queryDefStr)
	{
		this.queryDefStr = queryDefStr;
	}

	public String getQueryDefStr()
	{
		return queryDefStr;
	}

	public void setQueryDefStr(String queryDefStr)
	{
		this.queryDefStr = queryDefStr;
	}

}
