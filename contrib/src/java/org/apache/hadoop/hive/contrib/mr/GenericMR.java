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
package org.apache.hadoop.hive.contrib.mr;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This class attempts to provide a simple framework for writing Hive map/reduce
 * tasks in java.
 * 
 * The main benefit is that it deals with grouping the keys together for reduce
 * tasks.
 * 
 * Additionally, it deals with all system io... and provides something closer to
 * the hadoop m/r.
 * 
 * As an example, here's the wordcount reduce:
 * 
 * new GenericMR().reduce(System.in, System.out, new Reducer() { public void
 * reduce(String key, Iterator<String[]> records, Output output) throws
 * Exception { int count = 0;
 * 
 * while (records.hasNext()) { count += Integer.parseInt(records.next()[1]); }
 * 
 * output.collect(new String[] { key, String.valueOf(count) }); }});
 */
public final class GenericMR {
  public void map(final InputStream in, final OutputStream out,
      final Mapper mapper) throws Exception {
    map(new InputStreamReader(in), new OutputStreamWriter(out), mapper);
  }

  public void map(final Reader in, final Writer out, final Mapper mapper)
      throws Exception {
    handle(in, out, new RecordProcessor() {
      @Override
      public void processNext(RecordReader reader, Output output)
          throws Exception {
        mapper.map(reader.next(), output);
      }
    });
  }

  public void reduce(final InputStream in, final OutputStream out,
      final Reducer reducer) throws Exception {
    reduce(new InputStreamReader(in), new OutputStreamWriter(out), reducer);
  }

  public void reduce(final Reader in, final Writer out, final Reducer reducer)
      throws Exception {
    handle(in, out, new RecordProcessor() {
      @Override
      public void processNext(RecordReader reader, Output output)
          throws Exception {
        reducer.reduce(reader.peek()[0], new KeyRecordIterator(
            reader.peek()[0], reader), output);
      }
    });
  }

  private void handle(final Reader in, final Writer out,
      final RecordProcessor processor) throws Exception {
    final RecordReader reader = new RecordReader(in);
    final OutputStreamOutput output = new OutputStreamOutput(out);

    try {
      while (reader.hasNext()) {
        processor.processNext(reader, output);
      }
    } finally {
      try {
        output.close();
      } finally {
        reader.close();
      }
    }
  }

  private static interface RecordProcessor {
    void processNext(final RecordReader reader, final Output output)
        throws Exception;
  }

  private static final class KeyRecordIterator implements Iterator<String[]> {
    private final String key;
    private final RecordReader reader;

    private KeyRecordIterator(final String key, final RecordReader reader) {
      this.key = key;
      this.reader = reader;
    }

    @Override
    public boolean hasNext() {
      return (reader.hasNext() && key.equals(reader.peek()[0]));
    }

    @Override
    public String[] next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      return reader.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class RecordReader {
    private final BufferedReader reader;
    private String[] next;

    private RecordReader(final InputStream in) {
      this(new InputStreamReader(in));
    }

    private RecordReader(final Reader in) {
      reader = new BufferedReader(in);
      next = readNext();
    }

    private String[] next() {
      final String[] ret = next;

      next = readNext();

      return ret;
    }

    private String[] readNext() {
      try {
        final String line = reader.readLine();
        return (line == null ? null : line.split("\t"));
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    private boolean hasNext() {
      return next != null;
    }

    private String[] peek() {
      return next;
    }

    private void close() throws Exception {
      reader.close();
    }
  }

  private static final class OutputStreamOutput implements Output {
    private final PrintWriter out;

    private OutputStreamOutput(final OutputStream out) {
      this(new OutputStreamWriter(out));
    }

    private OutputStreamOutput(final Writer out) {
      this.out = new PrintWriter(out);
    }

    public void close() throws Exception {
      out.close();
    }

    @Override
    public void collect(String[] record) throws Exception {
      out.println(_join(record, "\t"));
    }

    private static String _join(final String[] record, final String separator) {
      if (record == null || record.length == 0) {
        return "";
      }
      final StringBuilder sb = new StringBuilder();
      for (int i = 0; i < record.length; i++) {
        if (i > 0) {
          sb.append(separator);
        }
        sb.append(record[i]);
      }
      return sb.toString();
    }
  }
}
