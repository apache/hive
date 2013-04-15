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

package org.apache.hadoop.hive.common;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

/**
 * This class contains methods used for the purposes of compression, this class
 * should not be accessed from code run in Hadoop.
 */
public class CompressionUtils {

  /**
   * Archive all the files in the inputFiles into outputFile
   *
   * @param inputFiles
   * @param outputFile
   * @throws IOException
   */
  public static void tar(String parentDir, String[] inputFiles, String outputFile)
      throws IOException {

    FileOutputStream out = null;
    try {
      out = new FileOutputStream(new File(parentDir, outputFile));
      TarArchiveOutputStream tOut = new TarArchiveOutputStream(
          new GzipCompressorOutputStream(new BufferedOutputStream(out)));

      for (int i = 0; i < inputFiles.length; i++) {
        File f = new File(parentDir, inputFiles[i]);
        TarArchiveEntry tarEntry = new TarArchiveEntry(f, f.getName());
        tOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
        tOut.putArchiveEntry(tarEntry);
        FileInputStream input = new FileInputStream(f);
        try {
          IOUtils.copy(input, tOut); // copy with 8K buffer, not close
        } finally {
          input.close();
        }
        tOut.closeArchiveEntry();
      }
      tOut.close(); // finishes inside
    } finally {
      // TarArchiveOutputStream seemed not to close files properly in error situation
      org.apache.hadoop.io.IOUtils.closeStream(out);
    }
  }
}
