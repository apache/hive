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

package org.apache.hadoop.hive.ql.io.protobuf;

import com.google.protobuf.MessageLite;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tez.dag.history.logging.proto.ProtoMessageWritable;

import java.io.EOFException;
import java.io.IOException;

/**
 * Lenient variant of ProtobufMessageInputFormat that tolerates read failures
 * in SequenceFileRecordReader by using a forgiving subclass.
 */
public class LenientProtobufMessageInputFormat<K, V extends MessageLite>
    extends ProtobufMessageInputFormat<K, V> {

  @Override
  public RecordReader<K, ProtoMessageWritable<V>> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    final RecordReader<K, ProtoMessageWritable<V>> reader = super.getRecordReader(split, job, reporter);
    return new IgnoreEOFProtoMessageRecordReader<>(reader);
  }

  /**
   * A RecordReader wrapper that tolerates EOF conditions while reading
   * protobuf messages from a SequenceFile.
   *
   * <p>Unlike the parent {@link ProtobufMessageInputFormat}, which only ignores
   * {@link EOFException} during RecordReader creation,
   * this reader also handles EOF during read and returns false from the next() method.</p>
   *
   * Note: This is a leniency mechanism and may result in silently skipping
   * records.
   */
  private class IgnoreEOFProtoMessageRecordReader<K, V extends MessageLite>
      implements RecordReader<K, ProtoMessageWritable<V>> {

    final RecordReader<K, ProtoMessageWritable<V>> mainReader;

    private IgnoreEOFProtoMessageRecordReader(RecordReader reader) throws IOException {
      mainReader = reader;
    }

    @Override
    public ProtoMessageWritable<V> createValue() {
      return mainReader != null ? mainReader.createValue() : null;
    }

    @Override
    public K createKey() {
      return mainReader != null ? mainReader.createKey() : null;
    }

    @Override
    public void close() throws IOException {
      if (mainReader != null) {
        mainReader.close();
      }
    }

    @Override
    public long getPos() throws IOException {
      return mainReader != null ? mainReader.getPos() : 0;
    }

    @Override
    public float getProgress() throws IOException {
      return mainReader != null ? mainReader.getProgress() : 1.0f;
    }

    @Override
    public boolean next(K arg0, ProtoMessageWritable<V> arg1) throws IOException {
      try {
        return mainReader != null ? mainReader.next(arg0, arg1) : false;
      } catch (EOFException e) {
        LOG.warn("Premature EOF while reading proto record at position {}. Returning false.",
            mainReader != null ? mainReader.getPos() : -1);
        return false;
      }
    }
  }
}
