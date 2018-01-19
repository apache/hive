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

package org.apache.hadoop.hive.llap.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.common.type.RandomTypeUtil;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestChunkedInputStream {

  static int bufferSize = 128;
  static Random rand = new Random();
  static String alphabet = "abcdefghijklmnopqrstuvwxyz";

  static class StreamTester {
    Exception error = null;

    public Exception getError() {
      return error;
    }

    public void setError(Exception error) {
      this.error = error;
    }
  }

  // Test class to write a series of values to the designated output stream
  static class BasicUsageWriter extends StreamTester implements Runnable {
    TestStreams streams;
    boolean flushCout;
    boolean closePoutEarly;

    public BasicUsageWriter(TestStreams streams, boolean flushCout, boolean closePoutEarly) {
      this.streams = streams;
      this.flushCout = flushCout;
      this.closePoutEarly = closePoutEarly;
    }

    @Override
    public void run() {
      try {
        // Write the items to the output stream.
        for (byte[] value: streams.values) {
          streams.out.write(value, 0, value.length);
        }

        if (flushCout) {
          streams.out.flush();
        }
        if (closePoutEarly) {
          // Close the inner output stream before closing the outer output stream.
          // For chunked output this means we don't write end-of-data indicator.
          streams.pout.close();
        }
        // This will throw error if we close pout early.
        streams.out.close();
      } catch (Exception err) {
        err.printStackTrace();
        this.error = err;
      }
    }
  }

  // Test class to read a series of values to the designated input stream
  static class BasicUsageReader extends StreamTester implements Runnable {
    TestStreams streams;
    boolean allValuesRead = false;

    public BasicUsageReader(TestStreams streams) {
      this.streams = streams;
    }

    // Continue reading from the input stream until the desired number of byte has been read
    void readFully(InputStream in, byte[] readValue, int numBytes) throws IOException {
      int bytesRead = 0;
      while (bytesRead < numBytes) {
        int read = in.read(readValue, bytesRead, numBytes - bytesRead);
        if (read <= 0) {
          throw new IOException("Unexpected read length " + read);
        }
        bytesRead += read;
      }
    }

    @Override
    public void run() {
      try {
        // Read the items from the input stream and confirm they match
        for (byte[] value : streams.values) {
          byte[] readValue = new byte[value.length];
          readFully(streams.in, readValue, readValue.length);
          assertArrayEquals(value, readValue);
        }

        allValuesRead = true;

        // Check that the output is done
        assertEquals(-1, streams.in.read());
      } catch (Exception err) {
        err.printStackTrace();
        this.error = err;
      }
    }
  }

  static class MyFilterInputStream extends FilterInputStream {
    public MyFilterInputStream(InputStream in) {
      super(in);
    }
  }

  // Helper class to set up a ChunkedInput/Output stream for testing
  static class TestStreams {
    PipedOutputStream pout;
    OutputStream out;
    PipedInputStream pin;
    InputStream in;
    List<byte[]> values;

    public TestStreams(boolean useChunkedStream) throws Exception {
      pout = new PipedOutputStream();
      pin = new PipedInputStream(pout);
      if (useChunkedStream) {
        out = new ChunkedOutputStream(pout, bufferSize, "test");
        in = new ChunkedInputStream(pin, "test");
      } else {
        // Test behavior with non-chunked streams
        out = new FilterOutputStream(pout);
        in = new MyFilterInputStream(pin);
      }
    }

    public void close() {
      try {
        pout.close();
      } catch (Exception err) {
        // ignore
      }

      try {
        pin.close();
      } catch (Exception err) {
        // ignore
      }
    }
  }

  static void runTest(Runnable writer, Runnable reader, TestStreams streams) throws Exception {
    Thread writerThread = new Thread(writer);
    Thread readerThread = new Thread(reader);

    writerThread.start();
    readerThread.start();

    writerThread.join();
    readerThread.join();
  }

  @Test
  public void testBasicUsage() throws Exception {
    List<byte[]> values = Arrays.asList(
        new byte[]{(byte) 1},
        new byte[]{(byte) 2},
        RandomTypeUtil.getRandString(rand, alphabet, 99).getBytes(),
        RandomTypeUtil.getRandString(rand, alphabet, 1024).getBytes()
    );

    // Try the basic test with non-chunked stream
    TestStreams nonChunkedStreams = new TestStreams(false);
    nonChunkedStreams.values = values;
    BasicUsageWriter writer1 = new BasicUsageWriter(nonChunkedStreams, false, false);
    BasicUsageReader reader1 = new BasicUsageReader(nonChunkedStreams);
    runTest(writer1, reader1, nonChunkedStreams);
    assertTrue(reader1.allValuesRead);
    assertNull(writer1.getError());
    assertNull(reader1.getError());

    // Try with chunked streams
    TestStreams chunkedStreams = new TestStreams(true);
    chunkedStreams.values = values;
    BasicUsageWriter writer2 = new BasicUsageWriter(chunkedStreams, false, false);
    BasicUsageReader reader2 = new BasicUsageReader(chunkedStreams);
    runTest(writer2, reader2, chunkedStreams);
    assertTrue(reader2.allValuesRead);
    assertTrue(((ChunkedInputStream) chunkedStreams.in).isEndOfData());
    assertNull(writer2.getError());
    assertNull(reader2.getError());
  }

  @Test
  public void testAbruptlyClosedOutput() throws Exception {
    List<byte[]> values = Arrays.asList(
        new byte[]{(byte) 1},
        new byte[]{(byte) 2},
        RandomTypeUtil.getRandString(rand, alphabet, 99).getBytes(),
        RandomTypeUtil.getRandString(rand, alphabet, 1024).getBytes()
    );

    // Close the PipedOutputStream before we close the outermost OutputStream.

    // Try non-chunked stream. There should be no issues assuming we flushed the streams before closing.
    TestStreams nonChunkedStreams = new TestStreams(false);
    nonChunkedStreams.values = values;
    BasicUsageWriter writer1 = new BasicUsageWriter(nonChunkedStreams, true, true);
    BasicUsageReader reader1 = new BasicUsageReader(nonChunkedStreams);
    runTest(writer1, reader1, nonChunkedStreams);
    assertTrue(reader1.allValuesRead);
    assertNull(writer1.getError());
    assertNull(reader1.getError());

    // Try with chunked stream. Here the chunked output didn't get a chance to write the end-of-data
    // indicator, so the chunked input does not know to stop reading.
    TestStreams chunkedStreams = new TestStreams(true);
    chunkedStreams.values = values;
    BasicUsageWriter writer2 = new BasicUsageWriter(chunkedStreams, true, true);
    BasicUsageReader reader2 = new BasicUsageReader(chunkedStreams);
    runTest(writer2, reader2, chunkedStreams);
    assertTrue(reader2.allValuesRead);
    assertFalse(((ChunkedInputStream) chunkedStreams.in).isEndOfData());
    // Closing the chunked output stream early gives an error
    assertNotNull(writer2.getError());
    // In this case we should expect the test to have failed at the very last read() check.
    assertNotNull(reader2.getError());
  }
}
