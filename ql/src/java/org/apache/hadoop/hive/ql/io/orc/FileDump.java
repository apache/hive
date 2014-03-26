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
import org.apache.hadoop.fs.Path;

/**
 * A tool for printing out the file structure of ORC files.
 */
public final class FileDump {

  // not used
  private FileDump() {}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    for(String filename: args) {
      System.out.println("Structure for " + filename);
      Path path = new Path(filename);
      Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
      RecordReaderImpl rows = (RecordReaderImpl) reader.rows();
      System.out.println("Rows: " + reader.getNumberOfRows());
      System.out.println("Compression: " + reader.getCompression());
      if (reader.getCompression() != CompressionKind.NONE) {
        System.out.println("Compression size: " + reader.getCompressionSize());
      }
      System.out.println("Type: " + reader.getObjectInspector().getTypeName());
      System.out.println("\nStripe Statistics:");
      Metadata metadata = reader.getMetadata();
      for (int n = 0; n < metadata.getStripeStatistics().size(); n++) {
        System.out.println("  Stripe " + (n + 1) + ":");
        StripeStatistics ss = metadata.getStripeStatistics().get(n);
        for (int i = 0; i < ss.getColumnStatistics().length; ++i) {
          System.out.println("    Column " + i + ": " +
              ss.getColumnStatistics()[i].toString());
        }
      }
      ColumnStatistics[] stats = reader.getStatistics();
      System.out.println("\nFile Statistics:");
      for(int i=0; i < stats.length; ++i) {
        System.out.println("  Column " + i + ": " + stats[i].toString());
      }
      System.out.println("\nStripes:");
      for(StripeInformation stripe: reader.getStripes()) {
        long stripeStart = stripe.getOffset();
        System.out.println("  Stripe: " + stripe.toString());
        OrcProto.StripeFooter footer = rows.readStripeFooter(stripe);
        long sectionStart = stripeStart;
        for(OrcProto.Stream section: footer.getStreamsList()) {
          System.out.println("    Stream: column " + section.getColumn() +
            " section " + section.getKind() + " start: " + sectionStart +
            " length " + section.getLength());
          sectionStart += section.getLength();
        }
        for(int i=0; i < footer.getColumnsCount(); ++i) {
          OrcProto.ColumnEncoding encoding = footer.getColumns(i);
          StringBuilder buf = new StringBuilder();
          buf.append("    Encoding column ");
          buf.append(i);
          buf.append(": ");
          buf.append(encoding.getKind());
          if (encoding.getKind() == OrcProto.ColumnEncoding.Kind.DICTIONARY) {
            buf.append("[");
            buf.append(encoding.getDictionarySize());
            buf.append("]");
          }
          System.out.println(buf);
        }
      }
      rows.close();
    }
  }
}
