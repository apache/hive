package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;

import junit.framework.TestCase;

public class TestPrimitiveObjectInspectorUtils extends TestCase {

  public void testGetPrimitiveGrouping() {
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.BYTE));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.SHORT));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.INT));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.LONG));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.FLOAT));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.DOUBLE));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.DECIMAL));

    assertEquals(PrimitiveGrouping.STRING_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.STRING));

    assertEquals(PrimitiveGrouping.DATE_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.DATE));
    assertEquals(PrimitiveGrouping.DATE_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.TIMESTAMP));

    assertEquals(PrimitiveGrouping.BOOLEAN_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.BOOLEAN));

    assertEquals(PrimitiveGrouping.BINARY_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.BINARY));

    assertEquals(PrimitiveGrouping.UNKNOWN_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.UNKNOWN));
    assertEquals(PrimitiveGrouping.VOID_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.VOID));
  }
}
