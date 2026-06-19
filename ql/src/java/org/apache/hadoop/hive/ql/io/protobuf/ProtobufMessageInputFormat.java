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

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.tez.dag.history.logging.proto.ProtoMessageWritable;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

/**
 * InputFormat to support reading ProtoWritable stored in a sequence file. You cannot use the
 * sequence file directly since the createValue method uses default constructor. But ProtoWritable
 * has a package protected constructor which takes a parser.
 * By reading the proto class name from job conf which is copied from table properties by Hive this
 * class manages to give a generic implementation where only can set the proto.class in table
 * properties and load the file.
 *
 * It is also enhanced to ignore EOF exception while opening a file, so as to ignore 0 bytes files
 * in the table. Maybe we should allow this to be configured.
 *
 * @param <K> K for the sequence file.
 * @param <V> The proto message type stored in the sequence file. Just to keep java compiler happy.
 */
public class ProtobufMessageInputFormat<K, V extends MessageLite>
    extends SequenceFileInputFormat<K, ProtoMessageWritable<V>> {
  private static final String PROTO_CLASS = "proto.class";

  @SuppressWarnings("unchecked")
  private Parser<V> getParser(String protoClass) throws IOException {
    if (protoClass == null) {
      throw new IOException("Please specificy table property: " + PROTO_CLASS);
    }
    try {
      Class<?> clazz = getClass().getClassLoader().loadClass(protoClass);
      return (Parser<V>)clazz.getField("PARSER").get(null);
    } catch (ClassNotFoundException | IllegalArgumentException | IllegalAccessException |
             NoSuchFieldException | SecurityException e) {
      throw new IOException("Could not load class: " + protoClass, e);
    }
  }

  private RecordReader<K, ProtoMessageWritable<V>> getSafeRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    try {
      return super.getRecordReader(split, job, reporter);
    } catch (EOFException e) {
      // Ignore EOFException, we create an empty reader for this, instead of failing.
      return null;
    }
  }

  @Override
  public RecordReader<K, ProtoMessageWritable<V>> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {

    final Parser<V> parser = getParser(job.get(PROTO_CLASS));
    final RecordReader<K, ProtoMessageWritable<V>> reader = getSafeRecordReader(
        split, job, reporter);
    return new RecordReader<K, ProtoMessageWritable<V>>() {
      // Overload create value, since there is no default constructor for ProtoMessageWritable.
      @SuppressWarnings("unchecked")
      @Override
      public ProtoMessageWritable<V> createValue() {
        try {
          @SuppressWarnings("rawtypes")
          Constructor<ProtoMessageWritable> cons = ProtoMessageWritable.class
              .getDeclaredConstructor(Parser.class);
          cons.setAccessible(true);
          return cons.newInstance(parser);
        } catch (Exception e) {
          throw new RuntimeException("Unexpected error: ", e);
        }
      }

      @Override
      public K createKey() {
        return reader != null ? reader.createKey() : null;
      }

      @Override
      public void close() throws IOException {
        if (reader != null) {
          reader.close();
        }
      }

      @Override
      public long getPos() throws IOException {
        return reader != null ? reader.getPos() : 0;
      }

      @Override
      public float getProgress() throws IOException {
        return reader != null ? reader.getProgress() : 1.0f;
      }

      @Override
      public boolean next(K arg0, ProtoMessageWritable<V> arg1) throws IOException {
        return reader != null ? reader.next(arg0, arg1) : false;
      }
    };
  }
}
