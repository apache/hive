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

package org.apache.hadoop.hive.common;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains methods used for the purposes of compression, this class
 * should not be accessed from code run in Hadoop.
 */
public class CompressionUtils {

  static final Logger LOG = LoggerFactory.getLogger(CompressionUtils.class);

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

  public static void zip(String parentDir, String[] inputFiles, String outputFile)
      throws IOException {
    ZipOutputStream output = null;
    try {
      output = new ZipOutputStream(new FileOutputStream(new File(parentDir, outputFile)));
      for (int i = 0; i < inputFiles.length; i++) {
        File f = new File(parentDir, inputFiles[i]);
        FileInputStream input = new FileInputStream(f);
        output.putNextEntry(new ZipEntry(inputFiles[i]));
        try {
          IOUtils.copy(input, output);
        } finally {
          input.close();
        }
      }
    } finally {
      org.apache.hadoop.io.IOUtils.closeStream(output);
    }
  }

  /**
   * Untar an input file into an output file.
   *
   * The output file is created in the output folder, having the same name as the input file, minus
   * the '.tar' extension.
   *
   * @param inputFileName the input .tar file
   * @param outputDirName the output directory file.
   * @throws IOException
   * @throws FileNotFoundException
   *
   * @return The {@link List} of {@link File}s with the untarred content.
   * @throws ArchiveException
   */
  public static List<File> unTar(final String inputFileName, final String outputDirName)
      throws FileNotFoundException, IOException, ArchiveException {
    return unTar(inputFileName, outputDirName, false);
  }

  /**
   * Untar an input file into an output file.
   *
   * The output file is created in the output folder, having the same name as the input file, minus
   * the '.tar' extension.
   *
   * @param inputFileName the input .tar file
   * @param outputDirName the output directory file.
   * @throws IOException
   * @throws FileNotFoundException
   *
   * @return The {@link List} of {@link File}s with the untarred content.
   * @throws ArchiveException
   */
  public static List<File> unTar(final String inputFileName, final String outputDirName,
      boolean flatten) throws FileNotFoundException, IOException, ArchiveException {

    File inputFile = new File(inputFileName);
    File outputDir = new File(outputDirName);

    final List<File> untarredFiles = new LinkedList<File>();
    InputStream is = null;

    try {
      if (inputFileName.endsWith(".gz")) {
        is = new GzipCompressorInputStream(new FileInputStream(inputFile));
      } else {
        is = new FileInputStream(inputFile);
      }

      final TarArchiveInputStream debInputStream =
              (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", is);
      TarArchiveEntry entry = null;
      while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {
        final File outputFile = new File(outputDir, entry.getName());
        if (!outputFile.toPath().toAbsolutePath().normalize()
                .startsWith(outputDir.toPath().toAbsolutePath().normalize())) {
          throw new IOException("Untarred file is not under the output directory");
        }
        if (entry.isDirectory()) {
          if (flatten) {
            // no sub-directories
            continue;
          }
          LOG.debug(String.format("Attempting to write output directory %s.",
                  outputFile.getAbsolutePath()));
          if (!outputFile.exists()) {
            LOG.debug(String.format("Attempting to create output directory %s.",
                    outputFile.getAbsolutePath()));
            if (!outputFile.mkdirs()) {
              throw new IllegalStateException(String.format("Couldn't create directory %s.",
                      outputFile.getAbsolutePath()));
            }
          }
        } else {
          final OutputStream outputFileStream;
          if (flatten) {
            File flatOutputFile = new File(outputDir, outputFile.getName());
            LOG.debug(String.format("Creating flat output file %s.", flatOutputFile.getAbsolutePath()));
            outputFileStream = new FileOutputStream(flatOutputFile);
          } else if (!outputFile.getParentFile().exists()) {
            LOG.debug(String.format("Attempting to create output directory %s.",
                    outputFile.getParentFile().getAbsoluteFile()));
            if (!outputFile.getParentFile().getAbsoluteFile().mkdirs()) {
              throw new IllegalStateException(String.format("Couldn't create directory %s.",
                      outputFile.getParentFile().getAbsolutePath()));
            }
            LOG.debug(String.format("Creating output file %s.", outputFile.getAbsolutePath()));
            outputFileStream = new FileOutputStream(outputFile);
          } else {
            outputFileStream = new FileOutputStream(outputFile);
          }
          IOUtils.copy(debInputStream, outputFileStream);
          outputFileStream.close();
        }
        untarredFiles.add(outputFile);
      }
      debInputStream.close();
      return untarredFiles;

    } finally {
      if (is != null)  is.close();
    }
  }
}
