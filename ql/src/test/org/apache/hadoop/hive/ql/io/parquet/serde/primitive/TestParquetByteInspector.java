package org.apache.hadoop.hive.ql.io.parquet.serde.primitive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

public class TestParquetByteInspector {

  private ParquetByteInspector inspector;

  @Before
  public void setUp() {
    inspector = new ParquetByteInspector();
  }

  @Test
  public void testByteWritable() {
    ByteWritable obj = new ByteWritable((byte) 5);
    assertEquals(obj, inspector.getPrimitiveWritableObject(obj));
    assertEquals((byte) 5, inspector.getPrimitiveJavaObject(obj));
  }

  @Test
  public void testIntWritable() {
    IntWritable obj = new IntWritable(10);
    assertEquals(new ByteWritable((byte) 10), inspector.getPrimitiveWritableObject(obj));
    assertEquals((byte) 10, inspector.getPrimitiveJavaObject(obj));
  }

  @Test
  public void testNull() {
    assertNull(inspector.getPrimitiveWritableObject(null));
    assertNull(inspector.getPrimitiveJavaObject(null));
  }

  @Test
  public void testCreate() {
    assertEquals(new ByteWritable((byte) 8), inspector.create((byte) 8));
  }

  @Test
  public void testSet() {
    ByteWritable obj = new ByteWritable();
    assertEquals(new ByteWritable((byte) 12), inspector.set(obj, (byte) 12));
  }

  @Test
  public void testGet() {
    ByteWritable obj = new ByteWritable((byte) 15);
    assertEquals((byte) 15, inspector.get(obj));
  }
}
