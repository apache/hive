package org.apache.hive.ptest.execution.context;

import java.util.Set;

import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.junit.Assert;
import org.junit.Before;

import com.google.common.collect.Sets;

import static org.mockito.Mockito.*;

public class TestCloudComputeService {
  private static final String GROUP_NAME = "grp";
  private static final String GROUP_TAG = "group=" + GROUP_NAME;
  private NodeMetadata node;
  private Set<String> tags;

  @Before
  public void setup() {
    node = mock(NodeMetadata.class);
    tags = Sets.newHashSet(GROUP_TAG);
    when(node.getStatus()).thenReturn(Status.RUNNING);
    when(node.getName()).thenReturn(GROUP_NAME + "-1");
    when(node.getGroup()).thenReturn(GROUP_NAME + "-1");
    when(node.getTags()).thenReturn(tags);
  }

  @org.junit.Test
  public void testNotStarted() throws Exception {
    when(node.getStatus()).thenReturn(Status.ERROR);
    Assert.assertFalse("Node is not running, should be filtered out", CloudComputeService.
        createFilterPTestPredicate(GROUP_NAME, GROUP_TAG).apply(node));
  }
  @org.junit.Test
  public void testBadName() throws Exception {
    when(node.getName()).thenReturn(null);
    Assert.assertTrue("Node should be filtered in by group or tag", CloudComputeService.
        createFilterPTestPredicate(GROUP_NAME, GROUP_TAG).apply(node));
  }
  @org.junit.Test
  public void testBadGroup() throws Exception {
    when(node.getGroup()).thenReturn(null);
    Assert.assertTrue("Node should be filtered in by name or tag", CloudComputeService.
        createFilterPTestPredicate(GROUP_NAME, GROUP_TAG).apply(node));
  }
  @org.junit.Test
  public void testBadTag() throws Exception {
    tags.clear();
    Assert.assertTrue("Node should be filtered in by name or group", CloudComputeService.
        createFilterPTestPredicate(GROUP_NAME, GROUP_TAG).apply(node));
  }
}
