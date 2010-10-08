package org.apache.hadoop.hive.serde2.objectinspector;

/**
 * The UnionObject.
 *
 * It has tag followed by the object it is holding.
 *
 */
public interface UnionObject {
  /**
   * Get the tag of the union.
   *
   * @return the tag byte
   */
  byte getTag();

  /**
   * Get the Object.
   *
   * @return The Object union is holding
   */
  Object getObject();

}
