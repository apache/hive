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

package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;

/**
 * Define the configuration properties that Orc understands.
 */
public enum OrcConf {
  STRIPE_SIZE("hive.exec.orc.default.stripe.size",
      64L * 1024 * 1024,
      "Define the default ORC stripe size, in bytes."),
  BLOCK_SIZE("hive.exec.orc.default.block.size", 256L * 1024 * 1024,
      "Define the default file system block size for ORC files."),
  ROW_INDEX_STRIDE("hive.exec.orc.default.row.index.stride", 10000,
      "Define the default ORC index stride in number of rows. (Stride is the\n"+
          " number of rows n index entry represents.)"),
  BUFFER_SIZE("hive.exec.orc.default.buffer.size", 256 * 1024,
      "Define the default ORC buffer size, in bytes."),
  BLOCK_PADDING("hive.exec.orc.default.block.padding", true,
      "Define the default block padding, which pads stripes to the HDFS\n" +
          " block boundaries."),
  COMPRESS("hive.exec.orc.default.compress", "ZLIB",
      "Define the default compression codec for ORC file"),
  WRITE_FORMAT("hive.exec.orc.write.format", null,
      "Define the version of the file to write. Possible values are 0.11 and\n"+
          " 0.12. If this parameter is not defined, ORC will use the run\n" +
          " length encoding (RLE) introduced in Hive 0.12. Any value other\n" +
          " than 0.11 results in the 0.12 encoding."),
  ENCODING_STRATEGY("hive.exec.orc.encoding.strategy", "SPEED",
      "Define the encoding strategy to use while writing data. Changing this\n"+
          "will only affect the light weight encoding for integers. This\n" +
          "flag will not change the compression level of higher level\n" +
          "compression codec (like ZLIB)."),
  COMPRESSION_STRATEGY("hive.exec.orc.compression.strategy", "SPEED",
      "Define the compression strategy to use while writing data.\n" +
          "This changes the compression level of higher level compression\n" +
          "codec (like ZLIB)."),
  BLOCK_PADDING_TOLERANCE("hive.exec.orc.block.padding.tolerance",
      0.05,
      "Define the tolerance for block padding as a decimal fraction of\n" +
          "stripe size (for example, the default value 0.05 is 5% of the\n" +
          "stripe size). For the defaults of 64Mb ORC stripe and 256Mb HDFS\n" +
          "blocks, the default block padding tolerance of 5% will\n" +
          "reserve a maximum of 3.2Mb for padding within the 256Mb block.\n" +
          "In that case, if the available size within the block is more than\n"+
          "3.2Mb, a new smaller stripe will be inserted to fit within that\n" +
          "space. This will make sure that no stripe written will block\n" +
          " boundaries and cause remote reads within a node local task."),
  BLOOM_FILTER_FPP("orc.default.bloom.fpp", 0.05,
      "Define the default false positive probability for bloom filters."),
  USE_ZEROCOPY("hive.exec.orc.zerocopy", false,
      "Use zerocopy reads with ORC. (This requires Hadoop 2.3 or later.)"),
  SKIP_CORRUPT_DATA("hive.exec.orc.skip.corrupt.data", false,
      "If ORC reader encounters corrupt data, this value will be used to\n" +
          "determine whether to skip the corrupt data or throw exception.\n" +
          "The default behavior is to throw exception."),
  MEMORY_POOL("hive.exec.orc.memory.pool", 0.5,
      "Maximum fraction of heap that can be used by ORC file writers"),
  DICTIONARY_KEY_SIZE_THRESHOLD("hive.exec.orc.dictionary.key.size.threshold",
      0.8,
      "If the number of keys in a dictionary is greater than this fraction\n" +
          "of the total number of non-null rows, turn off dictionary\n" +
          "encoding.  Use 1 to always use dictionary encoding."),
  ROW_INDEX_STRIDE_DICTIONARY_CHECK("hive.orc.row.index.stride.dictionary.check",
      true,
      "If enabled dictionary check will happen after first row index stride\n" +
          "(default 10000 rows) else dictionary check will happen before\n" +
          "writing first stripe. In both cases, the decision to use\n" +
          "dictionary or not will be retained thereafter."),
  ;

  private final String attribute;
  private final Object defaultValue;
  private final String description;

  OrcConf(String attribute, Object defaultValue, String description) {
    this.attribute = attribute;
    this.defaultValue = defaultValue;
    this.description = description;
  }

  public String getAttribute() {
    return attribute;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public String getDescription() {
    return description;
  }

  public long getLong(Configuration conf) {
    return conf.getLong(attribute, ((Number) defaultValue).longValue());
  }

  public String getString(Configuration conf) {
    return conf.get(attribute, (String) defaultValue);
  }

  public boolean getBoolean(Configuration conf) {
    if (conf == null) {
      return (Boolean) defaultValue;
    }
    return conf.getBoolean(attribute, (Boolean) defaultValue);
  }

  public double getDouble(Configuration conf) {
    String str = conf.get(attribute);
    if (str == null) {
      return ((Number) defaultValue).doubleValue();
    }
    return Double.parseDouble(str);
  }
}
