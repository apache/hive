package org.apache.hadoop.hive.cassandra.serde.lazy;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

/**
 * CassandraLazyPrimitive extends LazyPrimitive from Hive. When initializing the data, it checks to see if the length
 * of the byte array equals to the pre-allocated size for the primitive type. If the length equals, then we treat it
 * as it maps to a validator type defined in Cassandra. If the length doesn't equal, we parse the data in the original
 * Hive way.
 *
 * @param <OI> object inspector
 * @param <T> writable data
 */
public abstract class CassandraLazyPrimitive<OI extends ObjectInspector, T extends Writable>
    extends LazyPrimitive<OI, T> {

  protected int primitiveSize;

  CassandraLazyPrimitive(OI oi) {
    super(oi);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    //Set primitive size
    setPrimitiveSize();

    /**
     * If the length of the byte array to initialize equals to the pre-allocated size for the primitive value, then
     * most likely this maps to the corresponding type in cassandra. For example, if a column maps to LongType in
     * cassandra, a value 12 would be return as {0,0,0,0,0,0,0,12}. For this case, we parse it directly. Otherwise, we
     * parse it as what Hive LazyObject does.
     */
    if (checkSize(length)) {
      try {
        parsePrimitiveBytes(bytes, start, length);
        return;
      } catch (IndexOutOfBoundsException ie) {
        //we are unable to parse the data, try to parse it in the hive lazy way.
      }
    }

    try {
        parseBytes(bytes, start, length);
    } catch (NumberFormatException e) {
      isNull = true;
    }
  }

  /**
   * This method would check the length of the byte arrays and see if it is the same as the pre-allocated size as the
   * primitives like Long, Integer, Short, Double, and Float.
   *
   * @return true if the length equals the pre-allocated size; otherwise false.
   */
  public boolean checkSize(int length) {
    if (primitiveSize == length) {
      return true;
    }

    return false;
  }

  /**
   * Set the length of the pre-allocated size.
   */
  public abstract void setPrimitiveSize();

  /**
   * Parse the data that maps to the LongType, IntegerType in Cassandra.
   */
  public abstract void parsePrimitiveBytes(ByteArrayRef bytes, int start, int length);

  /**
   * Parse the data in the hive LazyPrimitive way.
   */
  public abstract void parseBytes(ByteArrayRef bytes, int start, int length);
}
