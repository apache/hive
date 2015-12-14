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

package org.apache.orc.impl;

import org.apache.hadoop.util.VersionInfo;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface HadoopShims {

  enum DirectCompressionType {
    NONE,
    ZLIB_NOHEADER,
    ZLIB,
    SNAPPY,
  }

  interface DirectDecompressor {
    void decompress(ByteBuffer var1, ByteBuffer var2) throws IOException;
  }

  /**
   * Get a direct decompressor codec, if it is available
   * @param codec
   * @return
   */
  DirectDecompressor getDirectDecompressor(DirectCompressionType codec);


  class Factory {
    private static HadoopShims SHIMS = null;

    public static synchronized HadoopShims get() {
      if (SHIMS == null) {
        String[] versionParts = VersionInfo.getVersion().split("[.]");
        int major = Integer.parseInt(versionParts[0]);
        int minor = Integer.parseInt(versionParts[1]);
        if (major < 2 || (major == 2 && minor < 3)) {
          SHIMS = new HadoopShims_2_2();
        } else {
          SHIMS = new HadoopShimsCurrent();
        }
      }
      return SHIMS;
    }
  }
}
