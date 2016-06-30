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

package org.apache.hadoop.hive.llap;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.DataInputStream;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base LLAP RecordReader to handle receiving of the data from the LLAP daemon.
 */
public class LlapBaseRecordReader<V extends WritableComparable> implements RecordReader<NullWritable, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LlapBaseRecordReader.class);

  protected final DataInputStream din;
  protected final Schema schema;
  protected final Class<V> clazz;

  protected Thread readerThread = null;
  protected final LinkedBlockingQueue<ReaderEvent> readerEvents = new LinkedBlockingQueue<ReaderEvent>();
  protected final long timeout;
  protected final Closeable client;
  private final Closeable socket;

  public LlapBaseRecordReader(InputStream in, Schema schema,
      Class<V> clazz, JobConf job, Closeable client, Closeable socket) {
    din = new DataInputStream(in);
    this.schema = schema;
    this.clazz = clazz;
    this.readerThread = Thread.currentThread();
    this.timeout = 3 * HiveConf.getTimeVar(job,
        HiveConf.ConfVars.LLAP_DAEMON_AM_LIVENESS_CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    this.client = client;
    this.socket = socket;
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public void close() throws IOException {
    Exception caughtException = null;
    try {
      din.close();
    } catch (Exception err) {
      LOG.error("Error closing input stream:" + err.getMessage(), err);
      caughtException = err;
    }
    // Don't close the socket - the stream already does that if needed.

    if (client != null) {
      try {
        client.close();
      } catch (Exception err) {
        LOG.error("Error closing client:" + err.getMessage(), err);
        caughtException = (caughtException == null ? err : caughtException);
      }
    }

    if (caughtException != null) {
      throw new IOException("Exception during close: " + caughtException.getMessage(), caughtException);
    }
  }

  @Override
  public long getPos() {
    // dummy impl
    return 0;
  }

  @Override
  public float getProgress() {
    // dummy impl
    return 0f;
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public V createValue() {
    try {
      return clazz.newInstance();
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public boolean next(NullWritable key, V value) throws IOException {
    try {
      // Need a way to know what thread to interrupt, since this is a blocking thread.
      setReaderThread(Thread.currentThread());

      value.readFields(din);
      return true;
    } catch (EOFException eof) {
      // End of input. There should be a reader event available, or coming soon, so okay to be blocking call.
      ReaderEvent event = getReaderEvent();
      switch (event.getEventType()) {
        case DONE:
          break;
        default:
          throw new IOException("Expected reader event with done status, but got "
              + event.getEventType() + " with message " + event.getMessage());
      }
      return false;
    } catch (IOException io) {
      if (Thread.interrupted()) {
        // Either we were interrupted by one of:
        // 1. handleEvent(), in which case there is a reader (error) event waiting for us in the queue
        // 2. Some other unrelated cause which interrupted us, in which case there may not be a reader event coming.
        // Either way we should not try to block trying to read the reader events queue.
        if (readerEvents.isEmpty()) {
          // Case 2.
          throw io;
        } else {
          // Case 1. Fail the reader, sending back the error we received from the reader event.
          ReaderEvent event = getReaderEvent();
          switch (event.getEventType()) {
            case ERROR:
              throw new IOException("Received reader event error: " + event.getMessage(), io);
            default:
              throw new IOException("Got reader event type " + event.getEventType()
                  + ", expected error event", io);
          }
        }
      } else {
        // If we weren't interrupted, just propagate the error
        throw io;
      }
    }
  }

  /**
   * Define success/error events which are passed to the reader from a different thread.
   * The reader will check for these events on end of input and interruption of the reader thread.
   */
  public static class ReaderEvent {
    public enum EventType {
      DONE,
      ERROR
    }

    protected final EventType eventType;
    protected final String message;

    protected ReaderEvent(EventType type, String message) {
      this.eventType = type;
      this.message = message;
    }

    public static ReaderEvent doneEvent() {
      return new ReaderEvent(EventType.DONE, "");
    }

    public static ReaderEvent errorEvent(String message) {
      return new ReaderEvent(EventType.ERROR, message);
    }

    public EventType getEventType() {
      return eventType;
    }

    public String getMessage() {
      return message;
    }
  }

  public void handleEvent(ReaderEvent event) {
    switch (event.getEventType()) {
      case DONE:
        // Reader will check for the event queue upon the end of the input stream - no need to interrupt.
        readerEvents.add(event);
        break;
      case ERROR:
        readerEvents.add(event);
        if (readerThread == null) {
          throw new RuntimeException("Reader thread is unexpectedly null, during ReaderEvent error " + event.getMessage());
        }
        // Reader is using a blocking socket .. interrupt it.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Interrupting reader thread due to reader event with error " + event.getMessage());
        }
        readerThread.interrupt();
        try {
          socket.close();
        } catch (IOException e) {
          // Leave the client to time out.
          LOG.error("Cannot close the socket on error", e);
        }
        break;
      default:
        throw new RuntimeException("Unhandled ReaderEvent type " + event.getEventType() + " with message " + event.getMessage());
    }
  }

  protected ReaderEvent getReaderEvent() throws IOException {
    try {
      ReaderEvent event = readerEvents.poll(timeout, TimeUnit.MILLISECONDS);
      if (event == null) {
        throw new IOException("Timed out getting readerEvents");
      }
      return event;
    } catch (InterruptedException ie) {
      throw new RuntimeException("Interrupted while getting readerEvents, not expected: " + ie.getMessage(), ie);
    }
  }

  protected synchronized void setReaderThread(Thread readerThread) {
    this.readerThread = readerThread;
  }

  protected synchronized Thread getReaderThread() {
    return readerThread;
  }
}
