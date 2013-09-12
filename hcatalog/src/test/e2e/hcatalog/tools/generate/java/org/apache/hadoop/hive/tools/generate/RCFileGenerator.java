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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.tools.generate;

import java.util.Properties;
import java.util.Random;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DefaultCodec;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;

/**
 * Generate RCFile test data
 *
 */
public class RCFileGenerator {

  private static Configuration conf = new Configuration();
  private static Path basedir;
  private static FileSystem fs;
  private static Properties tbl;
  private static Random rand;

  private static Path getFile(String filename) throws Exception {
    return new Path(basedir, filename);
  }

  private static String[] firstName = {"alice", "bob", "calvin", "david",
    "ethan", "fred", "gabriella", "holly", "irene", "jessica", "katie",
    "luke", "mike", "nick", "oscar", "priscilla", "quinn", "rachel",
    "sarah", "tom", "ulysses", "victor", "wendy", "xavier", "yuri",
    "zach"};

  private static String[] lastName = {"allen", "brown", "carson",
    "davidson", "ellison", "falkner", "garcia", "hernandez", "ichabod",
    "johnson", "king", "laertes", "miller", "nixon", "ovid", "polk",
    "quirinius", "robinson", "steinbeck", "thompson", "underhill",
    "van buren", "white", "xylophone", "young", "zipper"};

  private static String randomName() {
    StringBuffer buf =
        new StringBuffer(firstName[rand.nextInt(firstName.length)]);
    buf.append(' ');
    buf.append(lastName[rand.nextInt(lastName.length)]);
    return buf.toString();
  }

  private static int randomAge() {
    return rand.nextInt(60) + 18;
  }

  private static double randomGpa() {
    return 4 * rand.nextFloat();
  }

  private static String[] registration = {"democrat", "green",
    "independent", "libertarian", "republican", "socialist"};

  private static String randomRegistration() {
    return registration[rand.nextInt(registration.length)];
  }

  private static double randomContribution() {
    return rand.nextFloat() * 1000;
  }

  private static byte[] randomMap() throws Exception {
    int len = rand.nextInt(5) + 1;

    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < len; i++) {
      if (i != 0) buf.append('\u0002');
      buf.append(firstName[rand.nextInt(26)]);
      buf.append('\u0003');
      buf.append(lastName[rand.nextInt(26)]);
    }
    return buf.toString().getBytes("UTF-8");
  }

  private static byte[] randomArray() throws Exception {
    int len = rand.nextInt(5) + 1;

    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < len; i++) {
      if (i != 0) buf.append('\u0002');
      buf.append(Integer.valueOf(randomAge()).toString());
      buf.append('\u0003');
      buf.append(randomName());
    }
    return buf.toString().getBytes("UTF-8");
  }

  private static void usage() {
    System.err.println("Usage: rcfilegen format number_of_rows " +
        "output_file plain_output_file");
    System.err.println("  format one of:  student voter alltypes");
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 4) usage();

    String format = args[0];
    int numRows = Integer.valueOf(args[1]);
    if (numRows < 1) usage();
    String output = args[2];
    String plainOutput = args[3];

    fs = FileSystem.getLocal(conf);
    basedir = new Path(".");

    genData(format, numRows, output, plainOutput);
  }

  private static void genData(String format,
                int numRows,
                String output, String plainOutput) throws Exception {
    int numFields = 0;
    if (format.equals("student")) {
      rand = new Random(numRows);
      numFields = 3;
    } else if (format.equals("voter")) {
      rand = new Random(1000000000 + numRows);
      numFields = 4;
    } else if (format.equals("alltypes")) {
      rand = new Random(2000000000L + numRows);
      numFields = 10;
    }

    RCFileOutputFormat.setColumnNumber(conf, numFields);
    RCFile.Writer writer = new RCFile.Writer(fs, conf, getFile(output),
        null, new DefaultCodec());

    PrintWriter pw = new PrintWriter(new FileWriter(plainOutput));

    for (int j = 0; j < numRows; j++) {
      BytesRefArrayWritable row = new BytesRefArrayWritable(numFields);

      byte[][] fields = null;

      if (format.equals("student")) {
        byte[][] f = {
            randomName().getBytes("UTF-8"),
            Integer.valueOf(randomAge()).toString().getBytes("UTF-8"),
            Double.valueOf(randomGpa()).toString().getBytes("UTF-8")
        };
        fields = f;
      } else if (format.equals("voter")) {
        byte[][] f = {
            randomName().getBytes("UTF-8"),
            Integer.valueOf(randomAge()).toString().getBytes("UTF-8"),
            randomRegistration().getBytes("UTF-8"),
            Double.valueOf(randomContribution()).toString().getBytes("UTF-8")
        };
        fields = f;
      } else if (format.equals("alltypes")) {
        byte[][] f = {
            Integer.valueOf(rand.nextInt(Byte.MAX_VALUE)).toString().getBytes("UTF-8"),
            Integer.valueOf(rand.nextInt(Short.MAX_VALUE)).toString().getBytes("UTF-8"),
            Integer.valueOf(rand.nextInt()).toString().getBytes("UTF-8"),
            Long.valueOf(rand.nextLong()).toString().getBytes("UTF-8"),
            Float.valueOf(rand.nextFloat() * 1000).toString().getBytes("UTF-8"),
            Double.valueOf(rand.nextDouble() * 1000000).toString().getBytes("UTF-8"),
            randomName().getBytes("UTF-8"),
            randomMap(),
            randomArray()
        };
        fields = f;
      }


      for (int i = 0; i < fields.length; i++) {
        BytesRefWritable field = new BytesRefWritable(fields[i], 0,
            fields[i].length);
        row.set(i, field);
        pw.print(new String(fields[i]));
        if (i != fields.length - 1)
          pw.print("\t");
        else
          pw.println();
      }

      writer.append(row);
    }

    writer.close();
    pw.close();
  }
}

