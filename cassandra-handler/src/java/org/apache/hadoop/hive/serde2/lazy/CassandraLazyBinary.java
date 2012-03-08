package org.apache.hadoop.hive.serde2.lazy;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBinaryObjectInspector;
import org.apache.hadoop.io.BytesWritable;

public class CassandraLazyBinary extends LazyBinary {

  private static final Log LOG = LogFactory.getLog(LazyBinary.class);

  public CassandraLazyBinary(LazyBinaryObjectInspector oi) {
    super(oi);
    data = new BytesWritable();
  }

  public CassandraLazyBinary(LazyBinary other){
    super(other);
    BytesWritable incoming = other.getWritableObject();
    byte[] bytes = new byte[incoming.getLength()];
    System.arraycopy(incoming.getBytes(), 0, bytes, 0, incoming.getLength());
    data = new BytesWritable(bytes);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {

    byte[] recv = new byte[length];
    System.arraycopy(bytes.getData(), start, recv, 0, length);

    boolean arrayByteBase64 = false;
    boolean allValid = true;

    for(int i=0; i<recv.length; i++) {
      if(recv[i] < 0) {
        allValid = false;
      }
    }

    if(allValid) {
      arrayByteBase64 = Base64.isArrayByteBase64(recv);
    }

    if (arrayByteBase64) {
      LOG.debug("Data not contains valid characters within the Base64 alphabet so " +
                "decoded the data.");
    }
    byte[] decoded = arrayByteBase64 ? Base64.decodeBase64(recv) : recv;
    data.set(decoded, 0, decoded.length);
  }
}
