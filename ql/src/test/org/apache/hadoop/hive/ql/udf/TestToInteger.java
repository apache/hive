package org.apache.hadoop.hive.ql.udf;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestToInteger extends TestCase{

  @Test
  public void testTextToInteger() throws Exception{
    UDFToInteger ti = new UDFToInteger();
    Text t1 = new Text("-1");
    IntWritable i1 = ti.evaluate(t1);
    assertEquals(-1, i1.get());

    Text t2 = new Text("0");
    IntWritable i2 = ti.evaluate(t2);
    assertEquals(0, i2.get());

    Text t3 = new Text("A");
    IntWritable i3 = ti.evaluate(t3);
    assertNull(i3);

    Text t4 = new Text("1.1");
    IntWritable i4 = ti.evaluate(t4);
    assertEquals(1, i4.get());
  }
}
