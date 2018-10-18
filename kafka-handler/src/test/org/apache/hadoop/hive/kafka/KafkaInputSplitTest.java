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

import java.io.IOException;
import java.util.List;

/**
 * Kafka Hadoop InputSplit Test.
 */
public class KafkaInputSplitTest {
  private final KafkaInputSplit expectedInputSplit;

  public KafkaInputSplitTest() {
    String topic = "my_topic";
    this.expectedInputSplit = new KafkaInputSplit(topic, 1, 50L, 56L, new Path("/tmp"));
  }

  @Test public void testWriteRead() throws IOException {
    DataOutputBuffer output = new DataOutputBuffer();
    this.expectedInputSplit.write(output);
    KafkaInputSplit kafkaInputSplit = new KafkaInputSplit();
    DataInputBuffer input = new DataInputBuffer();
    input.reset(output.getData(), 0, output.getLength());
    kafkaInputSplit.readFields(input);
    Assert.assertEquals(this.expectedInputSplit, kafkaInputSplit);
  }

  @Test public void andRangeOverLapping() {
    KafkaInputSplit kafkaInputSplit = new KafkaInputSplit("test-topic", 2, 10, 400, new Path("/tmp"));

    KafkaInputSplit kafkaInputSplit2 = new KafkaInputSplit("test-topic", 2, 3, 200, new Path("/tmp"));

    Assert.assertEquals(new KafkaInputSplit("test-topic", 2, 10, 200, new Path("/tmp")),
        KafkaInputSplit.intersectRange(kafkaInputSplit, kafkaInputSplit2));

  }

  @Test public void andRangeNonOverLapping() {
    KafkaInputSplit kafkaInputSplit = new KafkaInputSplit("test-topic", 2, 10, 400, new Path("/tmp"));

    KafkaInputSplit kafkaInputSplit2 =
        new KafkaInputSplit("test-topic", 2, 550, 700, new Path("/tmp"));

    Assert.assertNull(KafkaInputSplit.intersectRange(kafkaInputSplit, kafkaInputSplit2));

  }

  @Test public void orRange() {
    KafkaInputSplit kafkaInputSplit =
        new KafkaInputSplit("test-topic", 2, 300, 400, new Path("/tmp"));

    KafkaInputSplit kafkaInputSplit2 = new KafkaInputSplit("test-topic", 2, 3, 600, new Path("/tmp"));

    Assert.assertEquals(kafkaInputSplit2,
        KafkaInputSplit.unionRange(kafkaInputSplit, kafkaInputSplit2));

    KafkaInputSplit kafkaInputSplit3 =
        new KafkaInputSplit("test-topic", 2, 700, 6000, new Path("/tmp"));

    Assert.assertEquals(new KafkaInputSplit("test-topic", 2, 300, 6000, new Path("/tmp")),
        KafkaInputSplit.unionRange(kafkaInputSplit, kafkaInputSplit3));
  }

  @Test public void copyOf() {
    KafkaInputSplit kafkaInputSplit =
        new KafkaInputSplit("test-topic", 2, 300, 400, new Path("/tmp"));

    KafkaInputSplit copyOf = KafkaInputSplit.copyOf(kafkaInputSplit);
    Assert.assertEquals(kafkaInputSplit, copyOf);
    Assert.assertNotSame(kafkaInputSplit, copyOf);
  }

  @Test public void testClone() {
    KafkaInputSplit kafkaInputSplit =
        new KafkaInputSplit("test-topic", 2, 300, 400, new Path("/tmp"));

    KafkaInputSplit clone = KafkaInputSplit.copyOf(kafkaInputSplit);
    Assert.assertEquals(kafkaInputSplit, clone);
    Assert.assertNotSame(clone, kafkaInputSplit);

  }

  @Test public void testSlice() {
    KafkaInputSplit kafkaInputSplit =
        new KafkaInputSplit("test-topic", 2, 300, 400, new Path("/tmp"));
    List<KafkaInputSplit> kafkaInputSplitList = KafkaInputSplit.slice(14, kafkaInputSplit);
    Assert.assertEquals(kafkaInputSplitList.stream()
        .mapToLong(kafkaPullerInputSplit1 -> kafkaPullerInputSplit1.getEndOffset()
            - kafkaPullerInputSplit1.getStartOffset())
        .sum(), kafkaInputSplit.getEndOffset() - kafkaInputSplit.getStartOffset());
    Assert.assertEquals(1,
        kafkaInputSplitList.stream()
            .filter(kafkaPullerInputSplit1 -> kafkaInputSplit.getStartOffset()
                == kafkaPullerInputSplit1.getStartOffset())
            .count());
    Assert.assertEquals(1,
        kafkaInputSplitList.stream()
            .filter(kafkaPullerInputSplit1 -> kafkaInputSplit.getEndOffset()
                == kafkaPullerInputSplit1.getEndOffset())
            .count());

  }
}
