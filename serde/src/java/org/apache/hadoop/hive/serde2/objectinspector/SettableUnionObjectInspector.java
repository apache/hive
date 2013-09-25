package org.apache.hadoop.hive.serde2.objectinspector;


/**
 * SettableUnionObjectInspector.
 *
 */
public abstract class SettableUnionObjectInspector implements
    UnionObjectInspector {

  /* Create an empty object */
  public abstract Object create();

  /* Add fields to the object */
  public abstract Object addField(Object union, ObjectInspector oi);
}
