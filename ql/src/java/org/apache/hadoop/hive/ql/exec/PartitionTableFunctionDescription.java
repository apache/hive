package org.apache.hadoop.hive.ql.exec;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.hadoop.hive.ql.udf.ptf.WindowingTableFunction;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
public @interface PartitionTableFunctionDescription
{
	Description description ();

	/**
	 * if true it is not usable in the language. {@link WindowingTableFunction} is the only internal function.
	 */
	boolean isInternal() default false;
}
