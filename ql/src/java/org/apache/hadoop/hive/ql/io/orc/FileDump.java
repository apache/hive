/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.filters.BloomFilterIO;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONWriter;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * A tool for printing out the file structure of ORC files.
 */
public final class FileDump {
  public static final String UNKNOWN = "UNKNOWN";
  public static final String SEPARATOR = Strings.repeat("_", 120) + "\n";
  public static final int DEFAULT_BLOCK_SIZE = 256 * 1024 * 1024;
  public static final String DEFAULT_BACKUP_PATH = "/tmp";
  public static final PathFilter HIDDEN_AND_SIDE_FILE_FILTER = new PathFilter() {
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".") && !name.endsWith(
          AcidUtils.DELTA_SIDE_FILE_SUFFIX);
    }
  };

  // not used
  private FileDump() {
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    List<Integer> rowIndexCols = null;
    Options opts = createOptions();
    CommandLine cli = new GnuParser().parse(opts, args);

    if (cli.hasOption('h')) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("orcfiledump", opts);
      return;
    }

    boolean dumpData = cli.hasOption('d');
    boolean recover = cli.hasOption("recover");
    boolean skipDump = cli.hasOption("skip-dump");
    String backupPath = DEFAULT_BACKUP_PATH;
    if (cli.hasOption("backup-path")) {
      backupPath = cli.getOptionValue("backup-path");
    }

    if (cli.hasOption("r")) {
      String[] colStrs = cli.getOptionValue("r").split(",");
      rowIndexCols = new ArrayList<Integer>(colStrs.length);
      for (String colStr : colStrs) {
        rowIndexCols.add(Integer.parseInt(colStr));
      }
    }

    boolean printTimeZone = cli.hasOption('t');
    boolean jsonFormat = cli.hasOption('j');
    String[] files = cli.getArgs();
    if (files.length == 0) {
      System.err.println("Error : ORC files are not specified");
      return;
    }

    // if the specified path is directory, iterate through all files and print the file dump
    List<String> filesInPath = Lists.newArrayList();
    for (String filename : files) {
      Path path = new Path(filename);
      filesInPath.addAll(getAllFilesInPath(path, conf));
    }

    if (dumpData) {
      printData(filesInPath, conf);
    } else if (recover && skipDump) {
      recoverFiles(filesInPath, conf, backupPath);
    } else {
      if (jsonFormat) {
        boolean prettyPrint = cli.hasOption('p');
        JsonFileDump.printJsonMetaData(filesInPath, conf, rowIndexCols, prettyPrint, printTimeZone);
      } else {
        printMetaData(filesInPath, conf, rowIndexCols, printTimeZone, recover, backupPath);
      }
    }
  }

  /**
   * This method returns an ORC reader object if the specified file is readable. If the specified
   * file has side file (_flush_length) file, then max footer offset will be read from the side
   * file and orc reader will be created from that offset. Since both data file and side file
   * use hflush() for flushing the data, there could be some inconsistencies and both files could be
   * out-of-sync. Following are the cases under which null will be returned
   *
   * 1) If the file specified by path or its side file is still open for writes
   * 2) If *_flush_length file does not return any footer offset
   * 3) If *_flush_length returns a valid footer offset but the data file is not readable at that
   *    position (incomplete data file)
   * 4) If *_flush_length file length is not a multiple of 8, then reader will be created from
   *    previous valid footer. If there is no such footer (file length > 0 and < 8), then null will
   *    be returned
   *
   * Also, if this method detects any file corruption (mismatch between data file and side file)
   * then it will add the corresponding file to the specified input list for corrupted files.
   *
   * In all other cases, where the file is readable this method will return a reader object.
   *
   * @param path - file to get reader for
   * @param conf - configuration object
   * @param corruptFiles - fills this list with all possible corrupted files
   * @return - reader for the specified file or null
   * @throws IOException
   */
  static Reader getReader(final Path path, final Configuration conf,
      final List<String> corruptFiles) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    long dataFileLen = fs.getFileStatus(path).getLen();
    System.err.println("Processing data file " + path + " [length: " + dataFileLen + "]");
    Path sideFile = OrcRecordUpdater.getSideFile(path);
    final boolean sideFileExists = fs.exists(sideFile);
    boolean openDataFile = false;
    boolean openSideFile = false;
    if (fs instanceof DistributedFileSystem) {
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      openDataFile = !dfs.isFileClosed(path);
      openSideFile = sideFileExists && !dfs.isFileClosed(sideFile);
    }

    if (openDataFile || openSideFile) {
      if (openDataFile && openSideFile) {
        System.err.println("Unable to perform file dump as " + path + " and " + sideFile +
            " are still open for writes.");
      } else if (openSideFile) {
        System.err.println("Unable to perform file dump as " + sideFile +
            " is still open for writes.");
      } else {
        System.err.println("Unable to perform file dump as " + path +
            " is still open for writes.");
      }

      return null;
    }

    Reader reader = null;
    if (sideFileExists) {
      final long maxLen = OrcRawRecordMerger.getLastFlushLength(fs, path);
      final long sideFileLen = fs.getFileStatus(sideFile).getLen();
      System.err.println("Found flush length file " + sideFile
          + " [length: " + sideFileLen + ", maxFooterOffset: " + maxLen + "]");
      // no offsets read from side file
      if (maxLen == -1) {

        // if data file is larger than last flush length, then additional data could be recovered
        if (dataFileLen > maxLen) {
          System.err.println("Data file has more data than max footer offset:" + maxLen +
              ". Adding data file to recovery list.");
          if (corruptFiles != null) {
            corruptFiles.add(path.toUri().toString());
          }
        }
        return null;
      }

      try {
        reader = OrcFile.createReader(path, OrcFile.readerOptions(conf).maxLength(maxLen));

        // if data file is larger than last flush length, then additional data could be recovered
        if (dataFileLen > maxLen) {
          System.err.println("Data file has more data than max footer offset:" + maxLen +
              ". Adding data file to recovery list.");
          if (corruptFiles != null) {
            corruptFiles.add(path.toUri().toString());
          }
        }
      } catch (Exception e) {
        if (corruptFiles != null) {
          corruptFiles.add(path.toUri().toString());
        }
        System.err.println("Unable to read data from max footer offset." +
            " Adding data file to recovery list.");
        return null;
      }
    } else {
      reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    }

    return reader;
  }

  public static Collection<String> getAllFilesInPath(final Path path,
      final Configuration conf) throws IOException {
    List<String> filesInPath = Lists.newArrayList();
    FileSystem fs = path.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(path);
    if (fileStatus.isDir()) {
      FileStatus[] fileStatuses = fs.listStatus(path, HIDDEN_AND_SIDE_FILE_FILTER);
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

  private static void printData(List<String> files,
      Configuration conf) throws IOException,
      JSONException {
    for (String file : files) {
      try {
        Path path = new Path(file);
        Reader reader = getReader(path, conf, Lists.<String>newArrayList());
        if (reader == null) {
          continue;
        }
        printJsonData(reader);
        System.out.println(SEPARATOR);
      } catch (Exception e) {
        System.err.println("Unable to dump data for file: " + file);
        continue;
      }
    }
  }

  private static void printMetaData(List<String> files, Configuration conf,
      List<Integer> rowIndexCols, boolean printTimeZone, final boolean recover,
      final String backupPath)
      throws IOException {
    List<String> corruptFiles = Lists.newArrayList();
    for (String filename : files) {
      printMetaDataImpl(filename, conf, rowIndexCols, printTimeZone, corruptFiles);
      System.out.println(SEPARATOR);
    }

    if (!corruptFiles.isEmpty()) {
      if (recover) {
        recoverFiles(corruptFiles, conf, backupPath);
      } else {
        System.err.println(corruptFiles.size() + " file(s) are corrupted." +
            " Run the following command to recover corrupted files.\n");
        String fileNames = Joiner.on(" ").skipNulls().join(corruptFiles);
        System.err.println("hive --orcfiledump --recover --skip-dump " + fileNames);
        System.out.println(SEPARATOR);
      }
    }
  }

  private static void printMetaDataImpl(final String filename,
      final Configuration conf, final List<Integer> rowIndexCols, final boolean printTimeZone,
      final List<String> corruptFiles) throws IOException {
    Path file = new Path(filename);
    Reader reader = getReader(file, conf, corruptFiles);
    // if we can create reader then footer is not corrupt and file will readable
    if (reader == null) {
      return;
    }

    System.out.println("Structure for " + filename);
    System.out.println("File Version: " + reader.getFileVersion().getName() +
        " with " + reader.getWriterVersion());
    RecordReaderImpl rows = (RecordReaderImpl) reader.rows();
    System.out.println("Rows: " + reader.getNumberOfRows());
    System.out.println("Compression: " + reader.getCompression());
    if (reader.getCompression() != CompressionKind.NONE) {
      System.out.println("Compression size: " + reader.getCompressionSize());
    }
    System.out.println("Type: " + reader.getObjectInspector().getTypeName());
    System.out.println("\nStripe Statistics:");
    List<StripeStatistics> stripeStats = reader.getStripeStatistics();
    for (int n = 0; n < stripeStats.size(); n++) {
      System.out.println("  Stripe " + (n + 1) + ":");
      StripeStatistics ss = stripeStats.get(n);
      for (int i = 0; i < ss.getColumnStatistics().length; ++i) {
        System.out.println("    Column " + i + ": " +
            ss.getColumnStatistics()[i].toString());
      }
    }
    ColumnStatistics[] stats = reader.getStatistics();
    int colCount = stats.length;
    System.out.println("\nFile Statistics:");
    for (int i = 0; i < stats.length; ++i) {
      System.out.println("  Column " + i + ": " + stats[i].toString());
    }
    System.out.println("\nStripes:");
    int stripeIx = -1;
    for (StripeInformation stripe : reader.getStripes()) {
      ++stripeIx;
      long stripeStart = stripe.getOffset();
      OrcProto.StripeFooter footer = rows.readStripeFooter(stripe);
      if (printTimeZone) {
        String tz = footer.getWriterTimezone();
        if (tz == null || tz.isEmpty()) {
          tz = UNKNOWN;
        }
        System.out.println("  Stripe: " + stripe.toString() + " timezone: " + tz);
      } else {
        System.out.println("  Stripe: " + stripe.toString());
      }
      long sectionStart = stripeStart;
      for (OrcProto.Stream section : footer.getStreamsList()) {
        String kind = section.hasKind() ? section.getKind().name() : UNKNOWN;
        System.out.println("    Stream: column " + section.getColumn() +
            " section " + kind + " start: " + sectionStart +
            " length " + section.getLength());
        sectionStart += section.getLength();
      }
      for (int i = 0; i < footer.getColumnsCount(); ++i) {
        OrcProto.ColumnEncoding encoding = footer.getColumns(i);
        StringBuilder buf = new StringBuilder();
        buf.append("    Encoding column ");
        buf.append(i);
        buf.append(": ");
        buf.append(encoding.getKind());
        if (encoding.getKind() == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
            encoding.getKind() == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
          buf.append("[");
          buf.append(encoding.getDictionarySize());
          buf.append("]");
        }
        System.out.println(buf);
      }
      if (rowIndexCols != null && !rowIndexCols.isEmpty()) {
        // include the columns that are specified, only if the columns are included, bloom filter
        // will be read
        boolean[] sargColumns = new boolean[colCount];
        for (int colIdx : rowIndexCols) {
          sargColumns[colIdx] = true;
        }
        OrcIndex indices = rows
            .readRowIndex(stripeIx, null, null, null, sargColumns);
        for (int col : rowIndexCols) {
          StringBuilder buf = new StringBuilder();
          String rowIdxString = getFormattedRowIndices(col, indices.getRowGroupIndex());
          buf.append(rowIdxString);
          String bloomFilString = getFormattedBloomFilters(col, indices.getBloomFilterIndex());
          buf.append(bloomFilString);
          System.out.println(buf);
        }
      }
    }

    FileSystem fs = file.getFileSystem(conf);
    long fileLen = fs.getFileStatus(file).getLen();
    long paddedBytes = getTotalPaddingSize(reader);
    // empty ORC file is ~45 bytes. Assumption here is file length always >0
    double percentPadding = ((double) paddedBytes / (double) fileLen) * 100;
    DecimalFormat format = new DecimalFormat("##.##");
    System.out.println("\nFile length: " + fileLen + " bytes");
    System.out.println("Padding length: " + paddedBytes + " bytes");
    System.out.println("Padding ratio: " + format.format(percentPadding) + "%");
    OrcRecordUpdater.AcidStats acidStats = OrcRecordUpdater.parseAcidStats(reader);
    if (acidStats != null) {
      System.out.println("ACID stats:" + acidStats);
    }
    rows.close();
  }

  private static void recoverFiles(final List<String> corruptFiles, final Configuration conf,
      final String backup)
      throws IOException {
    for (String corruptFile : corruptFiles) {
      System.err.println("Recovering file " + corruptFile);
      Path corruptPath = new Path(corruptFile);
      FileSystem fs = corruptPath.getFileSystem(conf);
      FSDataInputStream fdis = fs.open(corruptPath);
      try {
        long corruptFileLen = fs.getFileStatus(corruptPath).getLen();
        long remaining = corruptFileLen;
        List<Long> footerOffsets = Lists.newArrayList();

        // start reading the data file form top to bottom and record the valid footers
        while (remaining > 0) {
          int toRead = (int) Math.min(DEFAULT_BLOCK_SIZE, remaining);
          byte[] data = new byte[toRead];
          long startPos = corruptFileLen - remaining;
          fdis.readFully(startPos, data, 0, toRead);

          // find all MAGIC string and see if the file is readable from there
          int index = 0;
          long nextFooterOffset;

          while (index != -1) {
            index = indexOf(data, OrcFile.MAGIC.getBytes(), index + 1);
            if (index != -1) {
              nextFooterOffset = startPos + index + OrcFile.MAGIC.length() + 1;
              if (isReadable(corruptPath, conf, nextFooterOffset)) {
                footerOffsets.add(nextFooterOffset);
              }
            }
          }

          System.err.println("Scanning for valid footers - startPos: " + startPos +
              " toRead: " + toRead + " remaining: " + remaining);
          remaining = remaining - toRead;
        }

        System.err.println("Readable footerOffsets: " + footerOffsets);
        recoverFile(corruptPath, fs, conf, footerOffsets, backup);
      } catch (Exception e) {
        Path recoveryFile = getRecoveryFile(corruptPath);
        if (fs.exists(recoveryFile)) {
          fs.delete(recoveryFile, false);
        }
        System.err.println("Unable to recover file " + corruptFile);
        e.printStackTrace();
        System.err.println(SEPARATOR);
        continue;
      } finally {
        fdis.close();
      }
      System.err.println(corruptFile + " recovered successfully!");
      System.err.println(SEPARATOR);
    }
  }

  private static void recoverFile(final Path corruptPath, final FileSystem fs,
      final Configuration conf, final List<Long> footerOffsets, final String backup)
      throws IOException {

    // first recover the file to .recovered file and then once successful rename it to actual file
    Path recoveredPath = getRecoveryFile(corruptPath);

    // make sure that file does not exist
    if (fs.exists(recoveredPath)) {
      fs.delete(recoveredPath, false);
    }

    // if there are no valid footers, the file should still be readable so create an empty orc file
    if (footerOffsets == null || footerOffsets.isEmpty()) {
      System.err.println("No readable footers found. Creating empty orc file.");
      TypeDescription schema = TypeDescription.createStruct();
      Writer writer = OrcFile.createWriter(recoveredPath,
          OrcFile.writerOptions(conf).setSchema(schema));
      writer.close();
    } else {
      FSDataInputStream fdis = fs.open(corruptPath);
      FileStatus fileStatus = fs.getFileStatus(corruptPath);
      // read corrupt file and copy it to recovered file until last valid footer
      FSDataOutputStream fdos = fs.create(recoveredPath, true,
          conf.getInt("io.file.buffer.size", 4096),
          fileStatus.getReplication(),
          fileStatus.getBlockSize());
      try {
        long fileLen = footerOffsets.get(footerOffsets.size() - 1);
        long remaining = fileLen;

        while (remaining > 0) {
          int toRead = (int) Math.min(DEFAULT_BLOCK_SIZE, remaining);
          byte[] data = new byte[toRead];
          long startPos = fileLen - remaining;
          fdis.readFully(startPos, data, 0, toRead);
          fdos.write(data);
          System.err.println("Copying data to recovery file - startPos: " + startPos +
              " toRead: " + toRead + " remaining: " + remaining);
          remaining = remaining - toRead;
        }
      } catch (Exception e) {
        fs.delete(recoveredPath, false);
        throw new IOException(e);
      } finally {
        fdis.close();
        fdos.close();
      }
    }

    // validate the recovered file once again and start moving corrupt files to backup folder
    if (isReadable(recoveredPath, conf, Long.MAX_VALUE)) {
      Path backupDataPath;
      String scheme = corruptPath.toUri().getScheme();
      String authority = corruptPath.toUri().getAuthority();
      String filePath = corruptPath.toUri().getPath();

      // use the same filesystem as corrupt file if backup-path is not explicitly specified
      if (backup.equals(DEFAULT_BACKUP_PATH)) {
        backupDataPath = new Path(scheme, authority, DEFAULT_BACKUP_PATH + filePath);
      } else {
        backupDataPath = new Path(backup + filePath);
      }

      // Move data file to backup path
      moveFiles(fs, corruptPath, backupDataPath);

      // Move side file to backup path
      Path sideFilePath = OrcRecordUpdater.getSideFile(corruptPath);
      Path backupSideFilePath = new Path(backupDataPath.getParent(), sideFilePath.getName());
      moveFiles(fs, sideFilePath, backupSideFilePath);

      // finally move recovered file to actual file
      moveFiles(fs, recoveredPath, corruptPath);

      // we are done recovering, backing up and validating
      System.err.println("Validation of recovered file successful!");
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

  private static Path getRecoveryFile(final Path corruptPath) {
    return new Path(corruptPath.getParent(), corruptPath.getName() + ".recovered");
  }

  private static boolean isReadable(final Path corruptPath, final Configuration conf,
      final long maxLen) {
    try {
      OrcFile.createReader(corruptPath, OrcFile.readerOptions(conf).maxLength(maxLen));
      return true;
    } catch (Exception e) {
      // ignore this exception as maxLen is unreadable
      return false;
    }
  }

  // search for byte pattern in another byte array
  private static int indexOf(final byte[] data, final byte[] pattern, final int index) {
    if (data == null || data.length == 0 || pattern == null || pattern.length == 0 ||
        index > data.length || index < 0) {
      return -1;
    }

    int j = 0;
    for (int i = index; i < data.length; i++) {
      if (pattern[j] == data[i]) {
        j++;
      } else {
        j = 0;
      }

      if (j == pattern.length) {
        return i - pattern.length + 1;
      }
    }

    return -1;
  }

  private static String getFormattedBloomFilters(int col,
      OrcProto.BloomFilterIndex[] bloomFilterIndex) {
    StringBuilder buf = new StringBuilder();
    BloomFilterIO stripeLevelBF = null;
    if (bloomFilterIndex != null && bloomFilterIndex[col] != null) {
      int idx = 0;
      buf.append("\n    Bloom filters for column ").append(col).append(":");
      for (OrcProto.BloomFilter bf : bloomFilterIndex[col].getBloomFilterList()) {
        BloomFilterIO toMerge = new BloomFilterIO(bf);
        buf.append("\n      Entry ").append(idx++).append(":").append(getBloomFilterStats(toMerge));
        if (stripeLevelBF == null) {
          stripeLevelBF = toMerge;
        } else {
          stripeLevelBF.merge(toMerge);
        }
      }
      String bloomFilterStats = getBloomFilterStats(stripeLevelBF);
      buf.append("\n      Stripe level merge:").append(bloomFilterStats);
    }
    return buf.toString();
  }

  private static String getBloomFilterStats(BloomFilterIO bf) {
    StringBuilder sb = new StringBuilder();
    int bitCount = bf.getBitSize();
    int popCount = 0;
    for (long l : bf.getBitSet()) {
      popCount += Long.bitCount(l);
    }
    int k = bf.getNumHashFunctions();
    float loadFactor = (float) popCount / (float) bitCount;
    float expectedFpp = (float) Math.pow(loadFactor, k);
    DecimalFormat df = new DecimalFormat("###.####");
    sb.append(" numHashFunctions: ").append(k);
    sb.append(" bitCount: ").append(bitCount);
    sb.append(" popCount: ").append(popCount);
    sb.append(" loadFactor: ").append(df.format(loadFactor));
    sb.append(" expectedFpp: ").append(expectedFpp);
    return sb.toString();
  }

  private static String getFormattedRowIndices(int col,
                                               OrcProto.RowIndex[] rowGroupIndex) {
    StringBuilder buf = new StringBuilder();
    OrcProto.RowIndex index;
    buf.append("    Row group indices for column ").append(col).append(":");
    if (rowGroupIndex == null || (col >= rowGroupIndex.length) ||
        ((index = rowGroupIndex[col]) == null)) {
      buf.append(" not found\n");
      return buf.toString();
    }

    for (int entryIx = 0; entryIx < index.getEntryCount(); ++entryIx) {
      buf.append("\n      Entry ").append(entryIx).append(": ");
      OrcProto.RowIndexEntry entry = index.getEntry(entryIx);
      if (entry == null) {
        buf.append("unknown\n");
        continue;
      }
      OrcProto.ColumnStatistics colStats = entry.getStatistics();
      if (colStats == null) {
        buf.append("no stats at ");
      } else {
        ColumnStatistics cs = ColumnStatisticsImpl.deserialize(colStats);
        buf.append(cs.toString());
      }
      buf.append(" positions: ");
      for (int posIx = 0; posIx < entry.getPositionsCount(); ++posIx) {
        if (posIx != 0) {
          buf.append(",");
        }
        buf.append(entry.getPositions(posIx));
      }
    }
    return buf.toString();
  }

  public static long getTotalPaddingSize(Reader reader) throws IOException {
    long paddedBytes = 0;
    List<StripeInformation> stripes = reader.getStripes();
    for (int i = 1; i < stripes.size(); i++) {
      long prevStripeOffset = stripes.get(i - 1).getOffset();
      long prevStripeLen = stripes.get(i - 1).getLength();
      paddedBytes += stripes.get(i).getOffset() - (prevStripeOffset + prevStripeLen);
    }
    return paddedBytes;
  }

  static Options createOptions() {
    Options result = new Options();

    // add -d and --data to print the rows
    result.addOption(OptionBuilder
        .withLongOpt("data")
        .withDescription("Should the data be printed")
        .create('d'));

    // to avoid breaking unit tests (when run in different time zones) for file dump, printing
    // of timezone is made optional
    result.addOption(OptionBuilder
        .withLongOpt("timezone")
        .withDescription("Print writer's time zone")
        .create('t'));

    result.addOption(OptionBuilder
        .withLongOpt("help")
        .withDescription("print help message")
        .create('h'));

    result.addOption(OptionBuilder
        .withLongOpt("rowindex")
        .withArgName("comma separated list of column ids for which row index should be printed")
        .withDescription("Dump stats for column number(s)")
        .hasArg()
        .create('r'));

    result.addOption(OptionBuilder
        .withLongOpt("json")
        .withDescription("Print metadata in JSON format")
        .create('j'));

    result.addOption(OptionBuilder
        .withLongOpt("pretty")
        .withDescription("Pretty print json metadata output")
        .create('p'));

    result.addOption(OptionBuilder
        .withLongOpt("recover")
        .withDescription("recover corrupted orc files generated by streaming")
        .create());

    result.addOption(OptionBuilder
        .withLongOpt("skip-dump")
        .withDescription("used along with --recover to directly recover files without dumping")
        .create());

    result.addOption(OptionBuilder
        .withLongOpt("backup-path")
        .withDescription("specify a backup path to store the corrupted files (default: /tmp)")
        .hasArg()
        .create());
    return result;
  }

  private static void printMap(JSONWriter writer,
      Map<Object, Object> obj,
      List<OrcProto.Type> types,
      OrcProto.Type type
  ) throws IOException, JSONException {
    writer.array();
    int keyType = type.getSubtypes(0);
    int valueType = type.getSubtypes(1);
    for (Map.Entry<Object, Object> item : obj.entrySet()) {
      writer.object();
      writer.key("_key");
      printObject(writer, item.getKey(), types, keyType);
      writer.key("_value");
      printObject(writer, item.getValue(), types, valueType);
      writer.endObject();
    }
    writer.endArray();
  }

  private static void printList(JSONWriter writer,
      List<Object> obj,
      List<OrcProto.Type> types,
      OrcProto.Type type
  ) throws IOException, JSONException {
    int subtype = type.getSubtypes(0);
    writer.array();
    for (Object item : obj) {
      printObject(writer, item, types, subtype);
    }
    writer.endArray();
  }

  private static void printUnion(JSONWriter writer,
      OrcUnion obj,
      List<OrcProto.Type> types,
      OrcProto.Type type
  ) throws IOException, JSONException {
    int subtype = type.getSubtypes(obj.getTag());
    printObject(writer, obj.getObject(), types, subtype);
  }

  static void printStruct(JSONWriter writer,
      OrcStruct obj,
      List<OrcProto.Type> types,
      OrcProto.Type type) throws IOException, JSONException {
    writer.object();
    List<Integer> fieldTypes = type.getSubtypesList();
    for (int i = 0; i < fieldTypes.size(); ++i) {
      writer.key(type.getFieldNames(i));
      printObject(writer, obj.getFieldValue(i), types, fieldTypes.get(i));
    }
    writer.endObject();
  }

  static void printObject(JSONWriter writer,
      Object obj,
      List<OrcProto.Type> types,
      int typeId) throws IOException, JSONException {
    OrcProto.Type type = types.get(typeId);
    if (obj == null) {
      writer.value(null);
    } else {
      switch (type.getKind()) {
        case STRUCT:
          printStruct(writer, (OrcStruct) obj, types, type);
          break;
        case UNION:
          printUnion(writer, (OrcUnion) obj, types, type);
          break;
        case LIST:
          printList(writer, (List<Object>) obj, types, type);
          break;
        case MAP:
          printMap(writer, (Map<Object, Object>) obj, types, type);
          break;
        case BYTE:
          writer.value(((ByteWritable) obj).get());
          break;
        case SHORT:
          writer.value(((ShortWritable) obj).get());
          break;
        case INT:
          writer.value(((IntWritable) obj).get());
          break;
        case LONG:
          writer.value(((LongWritable) obj).get());
          break;
        case FLOAT:
          writer.value(((FloatWritable) obj).get());
          break;
        case DOUBLE:
          writer.value(((DoubleWritable) obj).get());
          break;
        case BOOLEAN:
          writer.value(((BooleanWritable) obj).get());
          break;
        default:
          writer.value(obj.toString());
          break;
      }
    }
  }

  static void printJsonData(final Reader reader) throws IOException, JSONException {
    PrintStream printStream = System.out;
    OutputStreamWriter out = new OutputStreamWriter(printStream, "UTF-8");
    RecordReader rows = reader.rows(null);
    Object row = null;
    List<OrcProto.Type> types = reader.getTypes();
    while (rows.hasNext()) {
      row = rows.next(row);
      JSONWriter writer = new JSONWriter(out);
      printObject(writer, row, types, 0);
      out.write("\n");
      out.flush();
      if (printStream.checkError()) {
        throw new IOException("Error encountered when writing to stdout.");
      }
    }
  }
}
