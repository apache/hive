/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.mapreduce;

import com.github.luben.zstd.Zstd;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.util.SerializationUtil;
import org.xerial.snappy.Snappy;

// Since this class extends `mapreduce.InputSplit and implements `mapred.InputSplit`, it can be returned by both MR v1
// and v2 file formats.
public class IcebergSplit extends InputSplit implements IcebergSplitContainer {

  public static final String[] ANYWHERE = new String[]{"*"};

  private ScanTaskGroup<FileScanTask> taskGroup;

  private transient String[] locations;
  private transient Configuration conf;

  // public no-argument constructor for deserialization
  public IcebergSplit() {
  }

  IcebergSplit(Configuration conf, ScanTaskGroup<FileScanTask> taskGroup) {
    this.taskGroup = taskGroup;
    this.conf = conf;
  }

  public ScanTaskGroup<FileScanTask> taskGroup() {
    return taskGroup;
  }

  @Override
  public IcebergSplit icebergSplit() {
    return this;
  }

  @Override
  public long getLength() {
    return taskGroup.tasks().stream().mapToLong(FileScanTask::length).sum();
  }

  @Override
  public String[] getLocations() {
    // The implementation of getLocations() is only meant to be used during split computation
    // getLocations() won't be accurate when called on worker nodes and will always return "*"
    if (locations == null && conf != null) {
      boolean localityPreferred = conf.getBoolean(InputFormatConfig.LOCALITY, false);
      locations = localityPreferred ? Util.blockLocations(taskGroup, conf) : ANYWHERE;
    } else {
      locations = ANYWHERE;
    }

    return locations;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    String codecName = conf != null ?
        HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_SPLIT_COMPRESSION_CODEC) :
        HiveConf.ConfVars.HIVE_ICEBERG_SPLIT_COMPRESSION_CODEC.getDefaultValue();
    Codec codec = Codec.valueOf(codecName.toUpperCase(Locale.ROOT));
    byte[] stored = codec.compress(SerializationUtil.serializeToBytes(this.taskGroup));
    out.writeByte(codec.id);
    out.writeInt(stored.length);
    out.write(stored);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Codec codec = Codec.of(in.readByte());
    byte[] stored = new byte[in.readInt()];
    in.readFully(stored);
    this.taskGroup = SerializationUtil.deserializeFromBytes(codec.decompress(stored));
  }

  // The payload is the codec id, the length of the stored bytes and the stored bytes. Snappy and zstd keep the length
  // of the serialized task group in their own headers.
  private enum Codec {
    NONE(0) {
      @Override
      byte[] compress(byte[] data) {
        return data;
      }

      @Override
      byte[] decompress(byte[] stored) {
        return stored;
      }
    },
    SNAPPY(1) {
      @Override
      byte[] compress(byte[] data) throws IOException {
        return Snappy.compress(data);
      }

      @Override
      byte[] decompress(byte[] stored) throws IOException {
        return Snappy.uncompress(stored);
      }
    },
    ZSTD(2) {
      @Override
      byte[] compress(byte[] data) {
        // the fastest level: a payload is a few KB, the ratio of a higher level does not pay for its time
        return Zstd.compress(data, 1);
      }

      @Override
      byte[] decompress(byte[] stored) {
        return Zstd.decompress(stored, (int) Zstd.getFrameContentSize(stored));
      }
    };

    private final byte id;

    Codec(int id) {
      this.id = (byte) id;
    }

    static Codec of(byte id) throws IOException {
      for (Codec codec : values()) {
        if (codec.id == id) {
          return codec;
        }
      }
      throw new IOException("Unknown Iceberg split codec id " + id);
    }

    abstract byte[] compress(byte[] data) throws IOException;

    abstract byte[] decompress(byte[] stored) throws IOException;
  }
}
