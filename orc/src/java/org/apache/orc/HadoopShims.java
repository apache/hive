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

package org.apache.orc;

import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;

public class HadoopShims {

  public enum DirectCompressionType {
    NONE,
    ZLIB_NOHEADER,
    ZLIB,
    SNAPPY,
  };

  public static DirectDecompressor getDirectDecompressor(
      DirectCompressionType codec) {
    DirectDecompressor decompressor = null;
    switch (codec) {
      case ZLIB:
        return new ZlibDecompressor.ZlibDirectDecompressor();
      case ZLIB_NOHEADER:
        return new ZlibDecompressor.ZlibDirectDecompressor(ZlibDecompressor.CompressionHeader.NO_HEADER, 0);
      case SNAPPY:
        return new SnappyDecompressor.SnappyDirectDecompressor();
      default:
    }
    /* not supported */
    return null;
  }
}
