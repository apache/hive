package org.apache.hadoop.hive.serde2.lazy;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.CassandraValidatorObjectInspector;
import org.apache.hadoop.io.Text;

public class CassandraLazyValidator  extends
    CassandraLazyPrimitive<CassandraValidatorObjectInspector, Text> {
  private final AbstractType validator;

  public CassandraLazyValidator(CassandraValidatorObjectInspector oi) {
    super(oi);
    validator = oi.getValidatorType();
    data = new Text();
  }

  public CassandraLazyValidator(CassandraLazyValidator copy) {
    super(copy.getInspector());
    validator = copy.validator;
    isNull = copy.isNull;
  }

  @Override
  public void parseBytes(ByteArrayRef bytes, int start, int length) {
    data.set(bytes.getData(), start, length);
  }

  @Override
  public void parsePrimitiveBytes(ByteArrayRef bytes, int start, int length) {
    ByteBuffer buf = ByteBuffer.wrap(bytes.getData(), start, length);
    setData(validator.getString(buf));
  }

  @Override
  public void setPrimitiveSize() {
    primitiveSize = 8;
  }

  private void setData(String str) {
    data.set(str);
    isNull = false;
  }

  @Override
  public boolean checkSize(int length) {
    return true;
  }

}
