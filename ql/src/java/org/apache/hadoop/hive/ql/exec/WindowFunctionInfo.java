package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;

@SuppressWarnings("deprecation")
public class WindowFunctionInfo
{
	boolean supportsWindow = true;
	boolean pivotResult = false;
	FunctionInfo fInfo;

	WindowFunctionInfo(FunctionInfo fInfo)
	{
		assert fInfo.isGenericUDAF();
		this.fInfo = fInfo;
		Class<? extends GenericUDAFResolver> wfnCls = fInfo.getGenericUDAFResolver().getClass();
		WindowFunctionDescription def = wfnCls.getAnnotation(WindowFunctionDescription.class);
		if ( def != null)
		{
			supportsWindow = def.supportsWindow();
			pivotResult = def.pivotResult();
		}
	}

	public boolean isSupportsWindow()
	{
		return supportsWindow;
	}

	public boolean isPivotResult()
	{
		return pivotResult;
	}

	public FunctionInfo getfInfo()
	{
		return fInfo;
	}
}