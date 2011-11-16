/**
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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * RCFileRecordReader.
 *
 * @param <K>
 * @param <V>
 */
public class RCFileRecordReader<K extends LongWritable, V extends BytesRefArrayWritable>
    implements RecordReader<LongWritable, BytesRefArrayWritable> {

  private final Reader in;
  private final long start;
  private final long end;
  private boolean more = true;
  protected Configuration conf;

  public RCFileRecordReader(Configuration conf, FileSplit split)
      throws IOException {
    Path path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    this.in = new RCFile.Reader(fs, path, conf);
    this.end = split.getStart() + split.getLength();
    this.conf = conf;

    if (split.getStart() > in.getPosition()) {
      in.sync(split.getStart()); // sync to start
    }

    this.start = in.getPosition();
    more = start < end;
  }

  public Class<?> getKeyClass() {
    return LongWritable.class;
  }

  public Class<?> getValueClass() {
    return BytesRefArrayWritable.class;
  }

  public LongWritable createKey() {
    return (LongWritable) ReflectionUtils.newInstance(getKeyClass(), conf);
  }

  public BytesRefArrayWritable createValue() {
    return (BytesRefArrayWritable) ReflectionUtils.newInstance(getValueClass(),
        conf);
  }

  @Override
  public boolean next(LongWritable key, BytesRefArrayWritable value)
      throws IOException {

    more = next(key);

    if (more) {
      in.getCurrentRow(value);
    }
    return more;
  }

  protected boolean next(LongWritable key) throws IOException {
    if (!more) {
      return false;
    }

    more = in.next(key);
    if (!more) {
      return false;
    }

    long lastSeenSyncPos = in.lastSeenSyncPos();
    if (lastSeenSyncPos >= end) {
      more = false;
      return more;
    }
    return more;
  }

  /**
   * Return the progress within the input split.
   *
   * @return 0.0 to 1.0 of the input byte range
   */
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.getPosition() - start) / (float) (end - start));
    }
  }

  public long getPos() throws IOException {
    return in.getPosition();
  }

  protected void seek(long pos) throws IOException {
    in.seek(pos);
  }

  public void sync(long pos) throws IOException {
    in.sync(pos);
  }

  public void resetBuffer() {
    in.resetBuffer();
  }

  public long getStart() {
    return start;
  }

  public void close() throws IOException {
    in.close();
  }
}
