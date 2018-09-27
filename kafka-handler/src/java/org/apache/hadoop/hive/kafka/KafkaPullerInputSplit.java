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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Kafka Hadoop Input Split Class.
 */
@SuppressWarnings("WeakerAccess") public class KafkaPullerInputSplit extends FileSplit
    implements org.apache.hadoop.mapred.InputSplit {
  private String topic;
  private long startOffset;
  private int partition;
  private long endOffset;

  public KafkaPullerInputSplit() {
    super(null, 0, 0, (String[]) null);
  }

  public KafkaPullerInputSplit(String topic, int partition, long startOffset, long endOffset, Path dummyPath) {
    super(dummyPath, 0, 0, (String[]) null);
    this.topic = topic;
    this.startOffset = startOffset;
    this.partition = partition;
    this.endOffset = endOffset;
    Preconditions.checkArgument(startOffset >= 0 && startOffset <= endOffset,
        "start [%s] has to be positive and >= end [%]",
        startOffset,
        endOffset);
  }

  @Override public long getLength() {
    return 0;
  }

  @Override public String[] getLocations() {
    return new String[0];
  }

  @Override public void write(DataOutput dataOutput) throws IOException {
    super.write(dataOutput);
    dataOutput.writeUTF(topic);
    dataOutput.writeInt(partition);
    dataOutput.writeLong(startOffset);
    dataOutput.writeLong(endOffset);
  }

  @Override public void readFields(DataInput dataInput) throws IOException {
    super.readFields(dataInput);
    topic = dataInput.readUTF();
    partition = dataInput.readInt();
    startOffset = dataInput.readLong();
    endOffset = dataInput.readLong();
    Preconditions.checkArgument(startOffset >= 0 && startOffset <= endOffset,
        "start [%s] has to be positive and >= end [%]",
        startOffset,
        endOffset);
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public long getEndOffset() {
    return endOffset;
  }

  /**
   * Compute the intersection of 2 splits. Splits must share the same topic and partition number.
   *
   * @param split1 left split
   * @param split2 right split
   *
   * @return new split that represents range intersection or null if it is not overlapping
   */
  @Nullable public static KafkaPullerInputSplit intersectRange(KafkaPullerInputSplit split1,
      KafkaPullerInputSplit split2) {
    assert (split1.topic.equals(split2.topic));
    assert (split1.partition == split2.partition);
    final long startOffset = Math.max(split1.getStartOffset(), split2.getStartOffset());
    final long endOffset = Math.min(split1.getEndOffset(), split2.getEndOffset());
    if (startOffset > endOffset) {
      // there is no overlapping
      return null;
    }
    return new KafkaPullerInputSplit(split1.topic, split1.partition, startOffset, endOffset, split1.getPath());
  }

  /**
   * Compute union of ranges between splits. Splits must share the same topic and partition
   *
   * @param split1 left split
   * @param split2 right split
   *
   * @return new split with a range including both splits.
   */
  public static KafkaPullerInputSplit unionRange(KafkaPullerInputSplit split1, KafkaPullerInputSplit split2) {
    assert (split1.topic.equals(split2.topic));
    assert (split1.partition == split2.partition);
    final long startOffset = Math.min(split1.getStartOffset(), split2.getStartOffset());
    final long endOffset = Math.max(split1.getEndOffset(), split2.getEndOffset());
    return new KafkaPullerInputSplit(split1.topic, split1.partition, startOffset, endOffset, split1.getPath());
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KafkaPullerInputSplit)) {
      return false;
    }
    KafkaPullerInputSplit that = (KafkaPullerInputSplit) o;
    return Objects.equal(getTopic(), that.getTopic())
        && Objects.equal(getStartOffset(), that.getStartOffset())
        && Objects.equal(getPartition(), that.getPartition())
        && Objects.equal(getEndOffset(), that.getEndOffset());
  }

  @Override public int hashCode() {
    return Objects.hashCode(getTopic(), getStartOffset(), getPartition(), getEndOffset());
  }

  @Override public String toString() {
    return "KafkaPullerInputSplit{"
        + "topic='"
        + topic
        + '\''
        + ", startOffset="
        + startOffset
        + ", partition="
        + partition
        + ", endOffset="
        + endOffset
        + ", path="
        + super.getPath().toString()
        + '}';
  }

  public static KafkaPullerInputSplit copyOf(KafkaPullerInputSplit other) {
    return new KafkaPullerInputSplit(other.getTopic(),
        other.getPartition(),
        other.getStartOffset(),
        other.getEndOffset(),
        other.getPath());
  }

  @SuppressWarnings("MethodDoesntCallSuperMethod") public KafkaPullerInputSplit clone() {
    return copyOf(this);
  }

  public static List<KafkaPullerInputSplit> slice(long sliceSize, final KafkaPullerInputSplit split) {
    if (split.getEndOffset() - split.getStartOffset() > sliceSize) {
      ImmutableList.Builder<KafkaPullerInputSplit> builder = ImmutableList.builder();
      long start = split.getStartOffset();
      while (start < split.getEndOffset() - sliceSize) {
        builder.add(new KafkaPullerInputSplit(split.topic,
            split.partition,
            start,
            start + sliceSize + 1,
            split.getPath()));
        start += sliceSize + 1;
      }
      // last split
      if (start < split.getEndOffset()) {
        builder.add(new KafkaPullerInputSplit(split.topic,
            split.partition,
            start,
            split.getEndOffset(),
            split.getPath()));
      }
      return builder.build();
    }

    return Collections.singletonList(copyOf(split));
  }

}
