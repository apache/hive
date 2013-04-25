package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.io.Text;

/**
 * Interface to support use of standard UDFs inside the vectorized execution code path.
 */
public interface IUDFUnaryString {
  Text evaluate(Text s);
}
