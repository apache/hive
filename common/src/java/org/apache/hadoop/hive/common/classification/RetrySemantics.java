package org.apache.hadoop.hive.common.classification;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * These annotations are meant to indicate how to handle retry logic.
 * Initially meant for Metastore API when made across a network, i.e. asynchronously where
 * the response may not reach the caller and thus it cannot know if the operation was actually
 * performed on the server.
 * @see RetryingMetastoreClient
 */
@InterfaceStability.Evolving
@InterfaceAudience.LimitedPrivate("Hive developer")
public class RetrySemantics {
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface Idempotent {
    String[] value() default "";
    int maxRetryCount() default Integer.MAX_VALUE;
    int delayMs() default 100;
  }
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface ReadOnly {/*trivially retry-able*/}
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface CannotRetry {}
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface SafeToRetry {
    /*may not be Idempotent but is safe to retry*/
    String[] value() default "";
  }
}
