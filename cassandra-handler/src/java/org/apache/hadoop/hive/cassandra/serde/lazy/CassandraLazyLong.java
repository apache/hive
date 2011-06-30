package org.apache.hadoop.hive.cassandra.serde.lazy;

import java.nio.ByteBuffer;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyLongObjectInspector;
import org.apache.hadoop.io.LongWritable;

/**
 * CassandraLazyLong parses the object into LongWritable value.
 *
 */
public class CassandraLazyLong extends
    CassandraLazyPrimitive<LazyLongObjectInspector, LongWritable> {

  public CassandraLazyLong(LazyLongObjectInspector oi) {
    super(oi);
    data = new LongWritable();
  }

  @Override
  public void parseBytes(ByteArrayRef bytes, int start, int length) {
    setData(LazyLong.parseLong(bytes.getData(), start, length));
  }

  @Override
  public void parsePrimitiveBytes(ByteArrayRef bytes, int start, int length) {

    ByteBuffer buf = ByteBuffer.wrap(bytes.getData(), start, length);
    setData(buf.getLong(buf.position()));
  }

  @Override
  public void setPrimitiveSize() {
    primitiveSize = 8;
  }

  private void setData(long num) {
    data.set(num);
    isNull = false;
  }

}

