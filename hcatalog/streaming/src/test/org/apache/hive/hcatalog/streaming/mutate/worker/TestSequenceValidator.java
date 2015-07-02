package org.apache.hive.hcatalog.streaming.mutate.worker;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.junit.Test;

public class TestSequenceValidator {

  private static final int BUCKET_ID = 1;

  private SequenceValidator validator = new SequenceValidator();

  @Test
  public void testSingleInSequence() {
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 0)), is(true));
  }

  @Test
  public void testRowIdInSequence() {
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 0)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 1)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 4)), is(true));
  }

  @Test
  public void testTxIdInSequence() {
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 0)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(1L, BUCKET_ID, 0)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(4L, BUCKET_ID, 0)), is(true));
  }

  @Test
  public void testMixedInSequence() {
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 0)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 1)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(1L, BUCKET_ID, 0)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(1L, BUCKET_ID, 1)), is(true));
  }

  @Test
  public void testNegativeTxId() {
    assertThat(validator.isInSequence(new RecordIdentifier(-1L, BUCKET_ID, 0)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 0)), is(true));
  }

  @Test
  public void testNegativeRowId() {
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, -1)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 0)), is(true));
  }

  @Test
  public void testRowIdOutOfSequence() {
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 0)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 4)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 1)), is(false));
  }

  @Test
  public void testReset() {
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 0)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 4)), is(true));
    // New partition for example
    validator.reset();
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 1)), is(true));
  }

  @Test
  public void testTxIdOutOfSequence() {
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 0)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(4L, BUCKET_ID, 0)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(1L, BUCKET_ID, 0)), is(false));
  }

  @Test
  public void testMixedOutOfSequence() {
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 0)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(1L, BUCKET_ID, 4)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(1L, BUCKET_ID, 0)), is(false));
    assertThat(validator.isInSequence(new RecordIdentifier(1L, BUCKET_ID, 5)), is(true));
    assertThat(validator.isInSequence(new RecordIdentifier(0L, BUCKET_ID, 6)), is(false));
  }

  @Test(expected = NullPointerException.class)
  public void testNullRecordIdentifier() {
    validator.isInSequence(null);
  }

}
