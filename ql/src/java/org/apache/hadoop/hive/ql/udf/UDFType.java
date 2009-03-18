package org.apache.hadoop.hive.ql.udf;


import java.lang.annotation.*;

@Target(ElementType.TYPE) 
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface UDFType {
  boolean deterministic() default true;
}
