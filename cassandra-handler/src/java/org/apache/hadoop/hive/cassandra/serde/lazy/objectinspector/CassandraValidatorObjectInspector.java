package org.apache.hadoop.hive.cassandra.serde.lazy.objectinspector;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.hadoop.hive.cassandra.serde.lazy.CassandraLazyValidator;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.AbstractPrimitiveLazyObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

public class CassandraValidatorObjectInspector
  extends AbstractPrimitiveLazyObjectInspector<Text> implements StringObjectInspector {

  /**
   * The primitive types supported by Hive.
   */
  public static enum CassandraValidatorCategory {
    UUID, ASCII, BYTES, INTEGER, LONG, UTF8, UNKNOWN
  }

  private final AbstractType validator;

  CassandraValidatorObjectInspector(AbstractType type) {
    super(PrimitiveObjectInspectorUtils.stringTypeEntry);
    validator = type;
  }

  @Override
  public String getTypeName() {
    return PrimitiveObjectInspectorUtils.stringTypeEntry.typeName;
  }

  @Override
  public Category getCategory() {
    return Category.PRIMITIVE;
  }

  public AbstractType getValidatorType() {
    return validator;
  }

  @Override
  public String getPrimitiveJavaObject(Object o) {
    return o == null ? null : ((CassandraLazyValidator) o).getWritableObject().toString();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new CassandraLazyValidator((CassandraLazyValidator) o);
  }
}
