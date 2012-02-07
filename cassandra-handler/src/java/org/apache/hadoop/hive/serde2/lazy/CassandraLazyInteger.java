package org.apache.hadoop.hive.serde2.lazy;

import java.nio.ByteBuffer;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

/**
 * CassandraLazyInteger parses the object into LongInteger value.
 *
 */
public class CassandraLazyInteger extends
    CassandraLazyPrimitive<LazyIntObjectInspector, IntWritable> {

  public CassandraLazyInteger(LazyIntObjectInspector oi) {
    super(oi);
    data = new IntWritable();
  }

  @Override
  public void parseBytes(ByteArrayRef bytes, int start, int length) {
    setData(LazyInteger.parseInt(bytes.getData(), start, length));
  }

  @Override
  public void parsePrimitiveBytes(ByteArrayRef bytes, int start, int length) {

    ByteBuffer buf = ByteBuffer.wrap(bytes.getData(), start, length);
    setData(buf.getInt(buf.position()));
  }

  @Override
  public void setPrimitiveSize() {
    primitiveSize = 4;
  }

  private void setData(int num) {
    data.set(num);
    isNull = false;
  }

}
