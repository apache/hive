package org.apache.hadoop.hive.ql.exec;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.ptf.WindowingTableFunction;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
public @interface WindowFunctionDescription
{
	Description description ();
	/**
	 * controls whether this function can be applied to a Window
	 */
	boolean supportsWindow() default true;
	/**
	 * A WindowFunc is implemented as {@link GenericUDAFResolver2}. It returns only one value.
	 * If this is true then the function must return a List which is taken to be the column for this function in the Output table returned by the
	 * {@link WindowingTableFunction}. Otherwise the output is assumed to be a single value, the column of the Output will contain the same value
	 * for all the rows.
	 */
	boolean pivotResult() default false;
}
