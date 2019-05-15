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
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.orc.CompressionKind;
import org.apache.orc.StripeInformation;
import org.apache.orc.OrcProto.StripeStatistics;
import org.apache.orc.impl.OrcAcidUtils;
import org.apache.orc.tools.FileDump;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to check and fix the ACID key index of an ORC file if it has been written incorrectly
 * due to HIVE-18817.
 * The condition that will be checked in the ORC file will be if the number of stripes in the
 * acid key index matches the number of stripes in the ORC StripeInformation.
 */
public class FixAcidKeyIndex {
  public final static Logger LOG = LoggerFactory.getLogger(FixAcidKeyIndex.class);

  public static final String DEFAULT_BACKUP_PATH = System.getProperty("java.io.tmpdir");
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final CharsetDecoder utf8Decoder = UTF8.newDecoder();

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Options opts = createOptions();
    CommandLine cli = new GnuParser().parse(opts, args);

    if (cli.hasOption('h')) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("fixacidkeyindex", opts);
      return;
    }

    String backupPath = DEFAULT_BACKUP_PATH;
    if (cli.hasOption("backup-path")) {
      backupPath = cli.getOptionValue("backup-path");
    }

    boolean checkOnly = cli.hasOption("check-only");
    boolean recover = cli.hasOption("recover");

    String[] files = cli.getArgs();
    if (files.length == 0) {
      System.err.println("Error : ORC files are not specified");
      return;
    }

    // if the specified path is directory, iterate through all files
    List<String> filesInPath = new ArrayList<>();
    for (String filename : files) {
      Path path = new Path(filename);
      filesInPath.addAll(getAllFilesInPath(path, conf));
    }

    if (checkOnly) {
      checkFiles(conf, filesInPath);
    } else if (recover) {
      recoverFiles(conf, filesInPath, backupPath);
    } else {
      System.err.println("check-only or recover option must be specified");
    }
  }

  static boolean isAcidKeyIndexValid(Reader reader) {
    if (reader.getNumberOfRows() == 0) {
      return true;
    }

    // The number of stripes should match the key index count
    List<StripeInformation> stripes = reader.getStripes();
    RecordIdentifier[] keyIndex = OrcRecordUpdater.parseKeyIndex(reader);
    if (keyIndex == null) {
      return false;
    }

    for (int idx = 0; idx < keyIndex.length; ++idx) {
      if (keyIndex[idx] == null) {
        LOG.info("*** keyIndex[" + idx + "] is null");
        return false;
      }
    }

    return stripes.size() == keyIndex.length;
  }

  static void recoverFiles(Configuration conf, List<String> fileList, String backup) {
    for (String fileName : fileList) {
      try {
        Path filePath = new Path(fileName);
        recoverFile(conf, filePath, backup);
      } catch (Exception err) {
        System.err.println("ERROR recovering " + fileName);
        err.printStackTrace(System.err);
      }
    }
  }

  static void checkFiles(Configuration conf, List<String> fileList) {
    for (String fileName : fileList) {
      try {
        Path filePath = new Path(fileName);
        checkFile(conf, filePath);
      } catch (Exception err) {
        System.err.println("ERROR checking " + fileName);
        err.printStackTrace(System.err);
      }
    }
  }

  static void checkFile(Configuration conf, Path inputPath) throws IOException {
    FileSystem fs = inputPath.getFileSystem(conf);
    Reader reader = OrcFile.createReader(fs, inputPath);

    if (OrcInputFormat.isOriginal(reader)) {
      System.out.println(inputPath + " is not an acid file");
      return;
    }

    boolean validIndex = isAcidKeyIndexValid(reader);
    System.out.println("Checking " + inputPath + " - acid key index is " +
        (validIndex ? "valid" : "invalid"));
  }

  static void recoverFile(Configuration conf, Path inputPath, String backup) throws IOException {
    FileSystem fs = inputPath.getFileSystem(conf);
    Reader reader = OrcFile.createReader(fs, inputPath);

    if (OrcInputFormat.isOriginal(reader)) {
      System.out.println(inputPath + " is not an acid file. No need to recover.");
      return;
    }

    boolean validIndex = isAcidKeyIndexValid(reader);
    if (validIndex) {
      System.out.println(inputPath + " has a valid acid key index. No need to recover.");
      return;
    }

    System.out.println("Recovering " + inputPath);

    Path recoveredPath = getRecoveryFile(inputPath);
    // make sure that file does not exist
    if (fs.exists(recoveredPath)) {
      fs.delete(recoveredPath, false);
    }

    // Writer should match the orc configuration from the original file
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
        .compress(reader.getCompression())
        .version(reader.getFileVersion())
        .rowIndexStride(reader.getRowIndexStride())
        .inspector(reader.getObjectInspector());
    // compression buffer size should only be set if compression is enabled
    if (reader.getCompression() != org.apache.hadoop.hive.ql.io.orc.CompressionKind.NONE) {
      writerOptions.bufferSize(reader.getCompressionSize()).enforceBufferSize();
    }

    try (Writer writer = OrcFile.createWriter(recoveredPath, writerOptions)) {

      // For HIVE-18817, the only thing missing is the last stripe index information.
      // Get the information from the last stripe and append it to the existing index.
      // The actual stripe data can be written as-is, similar to OrcFileMergeOperator.

      String keyIndexString = getKeyIndexAsString(reader);
      if (keyIndexString == null || keyIndexString.equals("null")) {
        // Key index can be null/"null" if there is only a single stripe. Just start fresh.
        keyIndexString = "";
      }

      List<StripeInformation> stripes = reader.getStripes();
      List<StripeStatistics> stripeStats = reader.getOrcProtoStripeStatistics();

      try (FSDataInputStream inputStream = fs.open(inputPath)) {
        for (int idx = 0; idx < stripes.size(); ++idx) {
          // initialize buffer to read the entire stripe.
          StripeInformation stripe = stripes.get(idx);
          int stripeLength = (int) stripe.getLength();
          byte[] buffer = new byte[stripeLength];
          inputStream.readFully(stripe.getOffset(), buffer, 0, stripeLength);

          // append the stripe buffer to the new ORC file
          writer.appendStripe(buffer, 0, buffer.length, stripe, stripeStats.get(idx));
        }
      }

      // For last stripe we need to get the last trasactionId/bucket/rowId from the last row.
      long lastRow = reader.getNumberOfRows() - 1;
      //RecordReader rr = reader.rows();
      try (RecordReader rr = reader.rows()) {
        rr.seekToRow(lastRow);
        OrcStruct row = (OrcStruct) rr.next(null);
        StructObjectInspector soi = (StructObjectInspector) reader.getObjectInspector();
        // struct<operation:int,originalTransaction:bigint,bucket:int,rowId:bigint,currentTransaction:bigint
        List<? extends StructField> structFields = soi.getAllStructFieldRefs();

        StructField transactionField = structFields.get(1);
        StructField bucketField = structFields.get(2);
        StructField rowIdField = structFields.get(3);

        long lastTransaction = ((LongObjectInspector) transactionField.getFieldObjectInspector()).get(
            soi.getStructFieldData(row, transactionField));
        int lastBucket = ((IntObjectInspector) bucketField.getFieldObjectInspector()).get(
            soi.getStructFieldData(row, bucketField));
        long lastRowId = ((LongObjectInspector) rowIdField.getFieldObjectInspector()).get(
            soi.getStructFieldData(row, rowIdField));
        keyIndexString += lastTransaction + "," + lastBucket + "," + lastRowId + ";";
      }

      // Add the rest of the metadata keys.
      for (String metadataKey : reader.getMetadataKeys()) {
        if (!metadataKey.equals(OrcRecordUpdater.ACID_KEY_INDEX_NAME)) {
          writer.addUserMetadata(metadataKey, reader.getMetadataValue(metadataKey));
        }
      }

      // Finally add the fixed acid key index.
      writer.addUserMetadata(OrcRecordUpdater.ACID_KEY_INDEX_NAME, UTF8.encode(keyIndexString));
    }

    // Confirm the file is really fixed, and replace the old file.
    Reader newReader = OrcFile.createReader(fs, recoveredPath);
    boolean fileFixed = isAcidKeyIndexValid(newReader);
    if (fileFixed) {
      Path backupDataPath;
      String scheme = inputPath.toUri().getScheme();
      String authority = inputPath.toUri().getAuthority();
      String filePath = inputPath.toUri().getPath();

      // use the same filesystem as input file if backup-path is not explicitly specified
      if (backup.equals(DEFAULT_BACKUP_PATH)) {
        backupDataPath = new Path(scheme, authority, DEFAULT_BACKUP_PATH + filePath);
      } else {
        backupDataPath = Path.mergePaths(new Path(backup), inputPath);
      }

      // Move data file to backup path
      moveFiles(fs, inputPath, backupDataPath);
      // finally move recovered file to actual file
      moveFiles(fs, recoveredPath, inputPath);

      System.out.println("Fixed acid key index for " + inputPath);
    } else {
      System.out.println("Unable to fix acid key index for " + inputPath);
    }
  }

  private static void moveFiles(final FileSystem fs, final Path src, final Path dest)
      throws IOException {
    try {
      // create the dest directory if not exist
      if (!fs.exists(dest.getParent())) {
        fs.mkdirs(dest.getParent());
      }

      // if the destination file exists for some reason delete it
      fs.delete(dest, false);

      if (fs.rename(src, dest)) {
        System.err.println("Moved " + src + " to " + dest);
      } else {
        throw new IOException("Unable to move " + src + " to " + dest);
      }

    } catch (Exception e) {
      throw new IOException("Unable to move " + src + " to " + dest, e);
    }
  }

  static String getKeyIndexAsString(Reader reader) {
    try {
      ByteBuffer val =
          reader.getMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME)
              .duplicate();
      return utf8Decoder.decode(val).toString();
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException("Bad string encoding for " +
          OrcRecordUpdater.ACID_KEY_INDEX_NAME, e);
    }
  }

  static Path getRecoveryFile(final Path corruptPath) {
    return new Path(corruptPath.getParent(), corruptPath.getName() + ".fixacidindex");
  }

  static Options createOptions() {
    Options result = new Options();

    result.addOption(OptionBuilder
        .withLongOpt("check-only")
        .withDescription("Check acid orc file for valid acid key index and exit without fixing")
        .create('c'));

    result.addOption(OptionBuilder
        .withLongOpt("recover")
        .withDescription("Fix the acid key index for acid orc file if it requires fixing")
        .create('r'));

    result.addOption(OptionBuilder
        .withLongOpt("backup-path")
        .withDescription("specify a backup path to store the corrupted files (default: /tmp)")
        .hasArg()
        .create());

    result.addOption(OptionBuilder
        .withLongOpt("help")
        .withDescription("print help message")
        .create('h'));

    return result;
  }

  public static Collection<String> getAllFilesInPath(final Path path,
      final Configuration conf) throws IOException {
    List<String> filesInPath = new ArrayList<>();
    FileSystem fs = path.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(path);
    if (fileStatus.isDir()) {
      FileStatus[] fileStatuses = fs.listStatus(path, FileDump.HIDDEN_AND_SIDE_FILE_FILTER);
      for (FileStatus fileInPath : fileStatuses) {
        if (fileInPath.isDir()) {
          filesInPath.addAll(getAllFilesInPath(fileInPath.getPath(), conf));
        } else {
          filesInPath.add(fileInPath.getPath().toString());
        }
      }
    } else {
      filesInPath.add(path.toString());
    }

    return filesInPath;
  }
}
