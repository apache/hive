package org.apache.hive.hcatalog.streaming.mutate.worker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

public class TestGroupingValidator {

  private GroupingValidator validator = new GroupingValidator();

  @Test
  public void uniqueGroups() {
    assertTrue(validator.isInSequence(Arrays.asList("a", "A"), 1));
    assertTrue(validator.isInSequence(Arrays.asList("c", "C"), 3));
    assertTrue(validator.isInSequence(Arrays.asList("b", "B"), 2));
  }

  @Test
  public void sameGroup() {
    assertTrue(validator.isInSequence(Arrays.asList("a", "A"), 1));
    assertTrue(validator.isInSequence(Arrays.asList("a", "A"), 1));
    assertTrue(validator.isInSequence(Arrays.asList("a", "A"), 1));
  }

  @Test
  public void revisitedGroup() {
    assertTrue(validator.isInSequence(Arrays.asList("a", "A"), 1));
    assertTrue(validator.isInSequence(Arrays.asList("c", "C"), 3));
    assertFalse(validator.isInSequence(Arrays.asList("a", "A"), 1));
  }

  @Test
  public void samePartitionDifferentBucket() {
    assertTrue(validator.isInSequence(Arrays.asList("a", "A"), 1));
    assertTrue(validator.isInSequence(Arrays.asList("c", "C"), 3));
    assertTrue(validator.isInSequence(Arrays.asList("a", "A"), 2));
  }

  @Test
  public void sameBucketDifferentPartition() {
    assertTrue(validator.isInSequence(Arrays.asList("a", "A"), 1));
    assertTrue(validator.isInSequence(Arrays.asList("c", "C"), 3));
    assertTrue(validator.isInSequence(Arrays.asList("b", "B"), 1));
  }

  @Test
  public void uniqueGroupsNoPartition() {
    assertTrue(validator.isInSequence(Collections.<String> emptyList(), 1));
    assertTrue(validator.isInSequence(Collections.<String> emptyList(), 3));
    assertTrue(validator.isInSequence(Collections.<String> emptyList(), 2));
  }

  @Test
  public void sameGroupNoPartition() {
    assertTrue(validator.isInSequence(Collections.<String> emptyList(), 1));
    assertTrue(validator.isInSequence(Collections.<String> emptyList(), 1));
    assertTrue(validator.isInSequence(Collections.<String> emptyList(), 1));
  }

  @Test
  public void revisitedGroupNoPartition() {
    assertTrue(validator.isInSequence(Collections.<String> emptyList(), 1));
    assertTrue(validator.isInSequence(Collections.<String> emptyList(), 3));
    assertFalse(validator.isInSequence(Collections.<String> emptyList(), 1));
  }
}
