package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import org.junit.Assert;
import org.junit.Test;

public class TestUDFOPDivide {

  UDFOPDivide udf = new UDFOPDivide();

  @Test
  public void testDivideByZero() {
    // Double
    DoubleWritable left = new DoubleWritable(4.5);
    DoubleWritable right = new DoubleWritable(0.0);
    DoubleWritable result = udf.evaluate(left, right);
    Assert.assertNull(result);

    // Decimal
    HiveDecimalWritable dec1 = new HiveDecimalWritable(HiveDecimal.create("4.5"));
    HiveDecimalWritable dec2 = new HiveDecimalWritable(HiveDecimal.create("0"));
    HiveDecimalWritable dec3 = udf.evaluate(dec1, dec2);
    Assert.assertNull(dec3);
  }

}
