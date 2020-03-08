package org.apache.hadoop.hive.cli.control;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Sets;

public class TestSplitSupport {

  @Test
  public void testSplitParams() {

    List<Object[]> input=new ArrayList<>();
    Set<Object[]> output=Sets.newIdentityHashSet();

  }

  @Test
  public void testIsSplitClass1() {
    Class<?> mainClass = org.apache.hadoop.hive.cli.control.splitsupport.SplitSupportDummy.class;
    Class<?> split0Class =
        org.apache.hadoop.hive.cli.control.splitsupport.split0.SplitSupportDummy.class;
    assertFalse(SplitSupport.isSplitClass(mainClass));
    assertTrue(SplitSupport.isSplitClass(split0Class));
  }

  @Test
  public void testGetSplitIndex0() {
    Class<?> split0Class = org.apache.hadoop.hive.cli.control.splitsupport.split0.SplitSupportDummy.class;
    assertEquals(0, SplitSupport.getSplitIndex(split0Class));
  }

  @Test
  public void testGetSplitIndex125() {
    Class<?> split0Class = org.apache.hadoop.hive.cli.control.splitsupport.split125.SplitSupportDummy.class;
    assertEquals(125, SplitSupport.getSplitIndex(split0Class));
  }
}
