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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.filters.BloomFilterIO;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndexEntry;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONWriter;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * A tool for printing out the file structure of ORC files.
 */
public final class FileDump {
  public static final String UNKNOWN = "UNKNOWN";

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
    } else {
      if (jsonFormat) {
        boolean prettyPrint = cli.hasOption('p');
        JsonFileDump.printJsonMetaData(filesInPath, conf, rowIndexCols, prettyPrint,
            printTimeZone);
      } else {
        printMetaData(filesInPath, conf, rowIndexCols, printTimeZone);
      }
    }
  }

  private static Collection<? extends String> getAllFilesInPath(final Path path,
      final Configuration conf) throws IOException {
    List<String> filesInPath = Lists.newArrayList();
    FileSystem fs = path.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(path);
    if (fileStatus.isDir()) {
      FileStatus[] fileStatuses = fs.listStatus(path, AcidUtils.hiddenFileFilter);
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

  private static void printData(List<String> files, Configuration conf) throws IOException,
      JSONException {
    for (String file : files) {
      printJsonData(conf, file);
      if (files.size() > 1) {
        System.out.println(Strings.repeat("=", 80) + "\n");
      }
    }
  }

  private static void printMetaData(List<String> files, Configuration conf,
      List<Integer> rowIndexCols, boolean printTimeZone) throws IOException {
    for (String filename : files) {
      System.out.println("Structure for " + filename);
      Path path = new Path(filename);
      Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
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
          RecordReaderImpl.Index indices = rows.readRowIndex(stripeIx, null, null, null, sargColumns);
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

      FileSystem fs = path.getFileSystem(conf);
      long fileLen = fs.getContentSummary(path).getLength();
      long paddedBytes = getTotalPaddingSize(reader);
      // empty ORC file is ~45 bytes. Assumption here is file length always >0
      double percentPadding = ((double) paddedBytes / (double) fileLen) * 100;
      DecimalFormat format = new DecimalFormat("##.##");
      System.out.println("\nFile length: " + fileLen + " bytes");
      System.out.println("Padding length: " + paddedBytes + " bytes");
      System.out.println("Padding ratio: " + format.format(percentPadding) + "%");
      rows.close();
      if (files.size() > 1) {
        System.out.println(Strings.repeat("=", 80) + "\n");
      }
    }
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

  private static String getFormattedRowIndices(int col, RowIndex[] rowGroupIndex) {
    StringBuilder buf = new StringBuilder();
    RowIndex index;
    buf.append("    Row group indices for column ").append(col).append(":");
    if (rowGroupIndex == null || (col >= rowGroupIndex.length) ||
        ((index = rowGroupIndex[col]) == null)) {
      buf.append(" not found\n");
      return buf.toString();
    }

    for (int entryIx = 0; entryIx < index.getEntryCount(); ++entryIx) {
      buf.append("\n      Entry ").append(entryIx).append(": ");
      RowIndexEntry entry = index.getEntry(entryIx);
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
    List<org.apache.hadoop.hive.ql.io.orc.StripeInformation> stripes = reader.getStripes();
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

  static void printJsonData(Configuration conf,
      String filename) throws IOException, JSONException {
    Path path = new Path(filename);
    Reader reader = OrcFile.createReader(path.getFileSystem(conf), path);
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
