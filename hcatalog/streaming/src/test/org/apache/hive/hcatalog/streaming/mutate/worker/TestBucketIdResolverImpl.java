package org.apache.hive.hcatalog.streaming.mutate.worker;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hive.hcatalog.streaming.mutate.MutableRecord;
import org.junit.Test;

public class TestBucketIdResolverImpl {

  private static final int TOTAL_BUCKETS = 12;
  private static final int RECORD_ID_COLUMN = 2;
  // id - TODO: use a non-zero index to check for offset errors.
  private static final int[] BUCKET_COLUMN_INDEXES = new int[] { 0 };

  private BucketIdResolver capturingBucketIdResolver = new BucketIdResolverImpl(
      ObjectInspectorFactory.getReflectionObjectInspector(MutableRecord.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA), RECORD_ID_COLUMN, TOTAL_BUCKETS, BUCKET_COLUMN_INDEXES);

  @Test
  public void testAttachBucketIdToRecord() {
    MutableRecord record = new MutableRecord(1, "hello");
    capturingBucketIdResolver.attachBucketIdToRecord(record);
    assertThat(record.rowId, is(new RecordIdentifier(-1L, 1, -1L)));
    assertThat(record.id, is(1));
    assertThat(record.msg.toString(), is("hello"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoBucketColumns() {
    new BucketIdResolverImpl(ObjectInspectorFactory.getReflectionObjectInspector(MutableRecord.class,
        ObjectInspectorFactory.ObjectInspectorOptions.JAVA), RECORD_ID_COLUMN, TOTAL_BUCKETS, new int[0]);

  }

}
