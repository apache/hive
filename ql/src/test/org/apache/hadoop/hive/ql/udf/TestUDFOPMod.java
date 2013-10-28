package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.junit.Assert;
import org.junit.Test;

public class TestUDFOPMod {

  UDFOPMod udf = new UDFOPMod();

  @Test
  public void testDivideByZero() {
    // Byte
    ByteWritable b1 = new ByteWritable((byte) 4);
    ByteWritable b2 = new ByteWritable((byte) 0);
    ByteWritable b3 = udf.evaluate(b1, b2);
    Assert.assertNull(b3);
    
    // Short
    ShortWritable s1 = new ShortWritable((short) 4);
    ShortWritable s2 = new ShortWritable((short) 0);
    ShortWritable s3 = udf.evaluate(s1, s2);
    Assert.assertNull(s3);
    
    // Int
    IntWritable i1 = new IntWritable(4);
    IntWritable i2 = new IntWritable(0);
    IntWritable i3 = udf.evaluate(i1, i2);
    Assert.assertNull(i3);
    
    // Long
    LongWritable l1 = new LongWritable(4);
    LongWritable l2 = new LongWritable(0L);
    LongWritable l3 = udf.evaluate(l1, l2);
    Assert.assertNull(l3);

    // Double
    FloatWritable f1 = new FloatWritable(4.5f);
    FloatWritable f2 = new FloatWritable(0.0f);
    FloatWritable f3 = udf.evaluate(f1, f2);
    Assert.assertNull(f3);

    // Double
    DoubleWritable d1 = new DoubleWritable(4.5);
    DoubleWritable d2 = new DoubleWritable(0.0);
    DoubleWritable d3 = udf.evaluate(d1, d2);
    Assert.assertNull(d3);

    // Decimal
    HiveDecimalWritable dec1 = new HiveDecimalWritable(HiveDecimal.create("4.5"));
    HiveDecimalWritable dec2 = new HiveDecimalWritable(HiveDecimal.create("0"));
    HiveDecimalWritable dec3 = udf.evaluate(dec1, dec2);
    Assert.assertNull(dec3);
  }

}
