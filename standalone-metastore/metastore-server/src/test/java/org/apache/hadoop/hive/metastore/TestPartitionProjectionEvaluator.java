/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.PartitionProjectionEvaluator.PartitionFieldNode;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import javax.jdo.PersistenceManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.PartitionProjectionEvaluator.CD_PATTERN;
import static org.apache.hadoop.hive.metastore.PartitionProjectionEvaluator.SD_PATTERN;
import static org.apache.hadoop.hive.metastore.PartitionProjectionEvaluator.SERDE_PATTERN;

@Category(MetastoreUnitTest.class)
public class TestPartitionProjectionEvaluator {
  private ImmutableMap<String, String> fieldNameToColumnName =
      ImmutableMap.<String, String>builder()
          .put("createTime", "\"PARTITIONS\"" + ".\"CREATE_TIME\"")
          .put("lastAccessTime", "\"PARTITIONS\"" + ".\"LAST_ACCESS_TIME\"")
          .put("sd.location", "\"SDS\"" + ".\"LOCATION\"")
          .put("sd.inputFormat", "\"SDS\"" + ".\"INPUT_FORMAT\"")
          .put("sd.outputFormat", "\"SDS\"" + ".\"OUTPUT_FORMAT\"")
          .put("sd.storedAsSubDirectories", "\"SDS\"" + ".\"IS_STOREDASSUBDIRECTORIES\"")
          .put("sd.compressed", "\"SDS\"" + ".\"IS_COMPRESSED\"")
          .put("sd.numBuckets", "\"SDS\"" + ".\"NUM_BUCKETS\"")
          .put("sd.serdeInfo.name", "\"SDS\"" + ".\"NAME\"")
          .put("sd.serdeInfo.serializationLib", "\"SDS\"" + ".\"SLIB\"")
          .put("PART_ID", "\"PARTITIONS\"" + ".\"PART_ID\"").put("SD_ID", "\"SDS\"" + ".\"SD_ID\"")
          .put("SERDE_ID", "\"SERDES\"" + ".\"SERDE_ID\"").put("CD_ID", "\"SDS\"" + ".\"CD_ID\"")
          .build();

  private static void compareTreeUtil(PartitionFieldNode expected, PartitionFieldNode given) {
    if (expected == null || given == null) {
      Assert.assertTrue(expected == null && given == null);
    }
    Assert.assertEquals("Field names should match", expected.getFieldName(), given.getFieldName());
    Assert.assertEquals(
        "IsLeafNode: Expected " + expected + " " + expected.isLeafNode() + " Given " + given + " " + given
            .isLeafNode(), expected.isLeafNode(), given.isLeafNode());
    Assert.assertEquals(
        "IsMultivalued: Expected " + expected + " " + expected.isMultiValued() + " Given " + given + " " + given
            .isMultiValued(), expected.isMultiValued(), given.isMultiValued());
    for (PartitionFieldNode child : expected.getChildren()) {
      Assert.assertTrue("given node " + given + " does not have the child node " + child,
          given.getChildren().contains(child));
      int counter = 0;
      for (PartitionFieldNode giveChild : given.getChildren()) {
        if (child.equals(giveChild)) {
          compareTreeUtil(child, giveChild);
          counter++;
        }
      }
      Assert.assertEquals("More than one copies of node " + child + " found", 1, counter);
    }
  }

  private static void compare(Set<PartitionFieldNode> roots, Set<PartitionFieldNode> giveRoots) {
    Assert.assertEquals("Given roots size does not match with the size of expected number of roots",
        roots.size(), giveRoots.size());
    for (PartitionFieldNode root : roots) {
      Assert.assertTrue(giveRoots.contains(root));
      int counter = 0;
      for (PartitionFieldNode givenRoot : giveRoots) {
        if (givenRoot.equals(root)) {
          compareTreeUtil(root, givenRoot);
          counter++;
        }
      }
      Assert.assertEquals("More than one copies of node found for " + root, 1, counter);
    }
  }

  @Test
  public void testPartitionFieldTree() throws MetaException {
    PersistenceManager mockPm = Mockito.mock(PersistenceManager.class);
    List<String> projectionFields = new ArrayList<>(2);
    projectionFields.add("sd.location");
    projectionFields.add("sd.parameters");
    projectionFields.add("createTime");
    projectionFields.add("sd.serdeInfo.serializationLib");
    projectionFields.add("sd.cols");
    projectionFields.add("parameters");
    PartitionProjectionEvaluator projectionEvaluator =
        new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false,
            false, null, null);
    Set<PartitionFieldNode> roots = projectionEvaluator.getRoots();

    Set<PartitionFieldNode> expected = new HashSet<>();
    PartitionFieldNode sdNode = new PartitionFieldNode("sd");
    sdNode.addChild(new PartitionFieldNode("sd.location"));
    sdNode.addChild(new PartitionFieldNode("sd.parameters", true));
    PartitionFieldNode sdColsNodes = new PartitionFieldNode("sd.cols", true);
    sdColsNodes.addChild(new PartitionFieldNode("sd.cols.name", true));
    sdColsNodes.addChild(new PartitionFieldNode("sd.cols.type", true));
    sdColsNodes.addChild(new PartitionFieldNode("sd.cols.comment", true));
    sdNode.addChild(sdColsNodes);

    PartitionFieldNode serdeNode = new PartitionFieldNode("sd.serdeInfo");
    serdeNode.addChild(new PartitionFieldNode("sd.serdeInfo.serializationLib"));

    sdNode.addChild(serdeNode);
    expected.add(sdNode);
    expected.add(new PartitionFieldNode("parameters", true));
    expected.add(new PartitionFieldNode("createTime"));
    expected.add(new PartitionFieldNode("PART_ID"));
    expected.add(new PartitionFieldNode("SD_ID"));
    expected.add(new PartitionFieldNode("CD_ID"));
    expected.add(new PartitionFieldNode("SERDE_ID"));
    compare(expected, roots);
  }

  @Test
  public void testProjectionCompaction() throws MetaException {
    PersistenceManager mockPm = Mockito.mock(PersistenceManager.class);
    List<String> projectionFields = new ArrayList<>(2);
    projectionFields.add("sd.location");
    projectionFields.add("sd.parameters");
    projectionFields.add("createTime");
    projectionFields.add("sd");
    PartitionProjectionEvaluator projectionEvaluator =
        new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false,
            false, null, null);
    Set<PartitionFieldNode> roots = projectionEvaluator.getRoots();
    Assert.assertFalse("sd.location should not contained since it is already included in sd",
        roots.contains(new PartitionFieldNode("sd.location")));
    Assert.assertFalse("sd.parameters should not contained since it is already included in sd",
        roots.contains(new PartitionFieldNode("sd.parameters")));
  }

  @Test(expected = MetaException.class)
  public void testInvalidProjectFields() throws MetaException {
    PersistenceManager mockPm = Mockito.mock(PersistenceManager.class);
    List<String> projectionFields = new ArrayList<>(2);
    projectionFields.add("sd.location");
    projectionFields.add("sd.parameters");
    projectionFields.add("createTime");
    projectionFields.add("sd");
    projectionFields.add("invalid");
    new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false, false,
        null, null);
  }

  @Test
  public void testFind() throws MetaException {
    PersistenceManager mockPm = Mockito.mock(PersistenceManager.class);
    List<String> projectionFields = Arrays.asList("sd", "createTime", "sd.location", "parameters");
    PartitionProjectionEvaluator projectionEvaluator =
        new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false,
            false, null, null);
    Assert.assertTrue(projectionEvaluator.find(SD_PATTERN));

    projectionFields = Arrays.asList("sd", "createTime", "parameters");
    projectionEvaluator =
        new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false,
            false, null, null);
    Assert.assertTrue(projectionEvaluator.find(SD_PATTERN));

    projectionFields = Arrays.asList("createTime", "parameters", "sd.serdeInfo.serializationLib");
    projectionEvaluator =
        new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false,
            false, null, null);
    Assert.assertTrue(projectionEvaluator.find(SD_PATTERN));

    projectionFields = Arrays.asList("createTime", "parameters", "sd.location");
    projectionEvaluator =
        new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false,
            false, null, null);
    Assert.assertTrue(projectionEvaluator.find(SD_PATTERN));

    projectionFields = Arrays.asList("createTime", "parameters", "sd.location");
    projectionEvaluator =
        new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false,
            false, null, null);
    Assert.assertFalse(projectionEvaluator.find(SERDE_PATTERN));

    projectionFields = Arrays.asList("createTime", "parameters", "sd.serdeInfo.serializationLib");
    projectionEvaluator =
        new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false,
            false, null, null);
    Assert.assertTrue(projectionEvaluator.find(SERDE_PATTERN));

    projectionFields = Arrays.asList("createTime", "parameters", "sd.serdeInfo");
    projectionEvaluator =
        new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false,
            false, null, null);
    Assert.assertTrue(projectionEvaluator.find(SERDE_PATTERN));

    projectionFields = Arrays.asList("createTime", "parameters");
    projectionEvaluator =
        new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false,
            false, null, null);
    Assert.assertFalse(projectionEvaluator.find(SD_PATTERN));

    projectionFields = Arrays.asList("createTime", "parameters", "sd.cols");
    projectionEvaluator =
        new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false,
            false, null, null);
    Assert.assertTrue(projectionEvaluator.find(CD_PATTERN));

    projectionFields = Arrays.asList("createTime", "parameters", "sd.cols.name");
    projectionEvaluator =
        new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false,
            false, null, null);
    Assert.assertTrue(projectionEvaluator.find(CD_PATTERN));

    projectionFields = Arrays.asList("createTime", "parameters", "sd", "sd.location");
    projectionEvaluator =
        new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false,
            false, null, null);
    // CD_PATTERN should exist since sd gets expanded to all the child nodes
    Assert.assertTrue(projectionEvaluator.find(CD_PATTERN));
  }

  @Test(expected = MetaException.class)
  public void testFindNegative() throws MetaException {
    PersistenceManager mockPm = Mockito.mock(PersistenceManager.class);
    List<String> projectionFields = projectionFields = Arrays.asList("createTime", "parameters", "sdxcols");
    PartitionProjectionEvaluator projectionEvaluator =
        new PartitionProjectionEvaluator(mockPm, fieldNameToColumnName, projectionFields, false,
            false, null, null);
  }
}