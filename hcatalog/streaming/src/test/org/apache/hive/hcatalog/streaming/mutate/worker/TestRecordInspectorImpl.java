package org.apache.hive.hcatalog.streaming.mutate.worker;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hive.hcatalog.streaming.mutate.MutableRecord;
import org.junit.Test;

public class TestRecordInspectorImpl {

  private static final int ROW_ID_COLUMN = 2;

  private RecordInspectorImpl inspector = new RecordInspectorImpl(ObjectInspectorFactory.getReflectionObjectInspector(
      MutableRecord.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA), ROW_ID_COLUMN);

  @Test
  public void testExtractRecordIdentifier() {
    RecordIdentifier recordIdentifier = new RecordIdentifier(10L, 4, 20L);
    MutableRecord record = new MutableRecord(1, "hello", recordIdentifier);
    assertThat(inspector.extractRecordIdentifier(record), is(recordIdentifier));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNotAStructObjectInspector() {
    new RecordInspectorImpl(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector, 2);
  }

}
