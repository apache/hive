/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.kafka;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Kafka Hadoop InputSplit Test.
 */
public class KafkaPullerInputSplitTest {
  private KafkaPullerInputSplit expectedInputSplit;

  public KafkaPullerInputSplitTest() {
    String topic = "my_topic";
    this.expectedInputSplit = new KafkaPullerInputSplit(topic, 1, 50L, 56L, new Path("/tmp"));
  }

  @Test public void testWriteRead() throws IOException {
    DataOutput output = new DataOutputBuffer();
    this.expectedInputSplit.write(output);
    KafkaPullerInputSplit kafkaPullerInputSplit = new KafkaPullerInputSplit();
    DataInput input = new DataInputBuffer();
    ((DataInputBuffer) input).reset(((DataOutputBuffer) output).getData(), 0, ((DataOutputBuffer) output).getLength());
    kafkaPullerInputSplit.readFields(input);
    Assert.assertEquals(this.expectedInputSplit, kafkaPullerInputSplit);
  }

  @Test public void andRangeOverLapping() {
    KafkaPullerInputSplit kafkaPullerInputSplit = new KafkaPullerInputSplit("test-topic", 2, 10, 400, new Path("/tmp"));

    KafkaPullerInputSplit kafkaPullerInputSplit2 = new KafkaPullerInputSplit("test-topic", 2, 3, 200, new Path("/tmp"));

    Assert.assertEquals(new KafkaPullerInputSplit("test-topic", 2, 10, 200, new Path("/tmp")),
        KafkaPullerInputSplit.intersectRange(kafkaPullerInputSplit, kafkaPullerInputSplit2));

  }

  @Test public void andRangeNonOverLapping() {
    KafkaPullerInputSplit kafkaPullerInputSplit = new KafkaPullerInputSplit("test-topic", 2, 10, 400, new Path("/tmp"));

    KafkaPullerInputSplit
        kafkaPullerInputSplit2 =
        new KafkaPullerInputSplit("test-topic", 2, 550, 700, new Path("/tmp"));

    Assert.assertEquals(null, KafkaPullerInputSplit.intersectRange(kafkaPullerInputSplit, kafkaPullerInputSplit2));

  }

  @Test public void orRange() {
    KafkaPullerInputSplit
        kafkaPullerInputSplit =
        new KafkaPullerInputSplit("test-topic", 2, 300, 400, new Path("/tmp"));

    KafkaPullerInputSplit kafkaPullerInputSplit2 = new KafkaPullerInputSplit("test-topic", 2, 3, 600, new Path("/tmp"));

    Assert.assertEquals(kafkaPullerInputSplit2,
        KafkaPullerInputSplit.unionRange(kafkaPullerInputSplit, kafkaPullerInputSplit2));

    KafkaPullerInputSplit
        kafkaPullerInputSplit3 =
        new KafkaPullerInputSplit("test-topic", 2, 700, 6000, new Path("/tmp"));

    Assert.assertEquals(new KafkaPullerInputSplit("test-topic", 2, 300, 6000, new Path("/tmp")),
        KafkaPullerInputSplit.unionRange(kafkaPullerInputSplit, kafkaPullerInputSplit3));
  }

  @Test public void copyOf() {
    KafkaPullerInputSplit
        kafkaPullerInputSplit =
        new KafkaPullerInputSplit("test-topic", 2, 300, 400, new Path("/tmp"));

    KafkaPullerInputSplit copyOf = KafkaPullerInputSplit.copyOf(kafkaPullerInputSplit);
    Assert.assertEquals(kafkaPullerInputSplit, copyOf);
    Assert.assertTrue(kafkaPullerInputSplit != copyOf);
  }

  @Test public void testClone() {
    KafkaPullerInputSplit
        kafkaPullerInputSplit =
        new KafkaPullerInputSplit("test-topic", 2, 300, 400, new Path("/tmp"));

    KafkaPullerInputSplit clone = kafkaPullerInputSplit.clone();
    Assert.assertEquals(kafkaPullerInputSplit, clone);
    Assert.assertTrue(clone != kafkaPullerInputSplit);

  }

  @Test public void testSlice() {
    KafkaPullerInputSplit
        kafkaPullerInputSplit =
        new KafkaPullerInputSplit("test-topic", 2, 300, 400, new Path("/tmp"));
    List<KafkaPullerInputSplit> kafkaPullerInputSplitList = KafkaPullerInputSplit.slice(14, kafkaPullerInputSplit);
    Assert.assertEquals(kafkaPullerInputSplitList.stream()
        .mapToLong(kafkaPullerInputSplit1 -> kafkaPullerInputSplit1.getEndOffset()
            - kafkaPullerInputSplit1.getStartOffset())
        .sum(), kafkaPullerInputSplit.getEndOffset() - kafkaPullerInputSplit.getStartOffset());
    Assert.assertTrue(kafkaPullerInputSplitList.stream()
        .filter(kafkaPullerInputSplit1 -> kafkaPullerInputSplit.getStartOffset()
            == kafkaPullerInputSplit1.getStartOffset())
        .count() == 1);
    Assert.assertTrue(kafkaPullerInputSplitList.stream()
        .filter(kafkaPullerInputSplit1 -> kafkaPullerInputSplit.getEndOffset() == kafkaPullerInputSplit1.getEndOffset())
        .count() == 1);

  }
}
