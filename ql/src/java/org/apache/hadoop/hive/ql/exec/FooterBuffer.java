package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

public class FooterBuffer {
  private ArrayList<ObjectPair> buffer;
  private int cur;

  public FooterBuffer() {
  }

  public void setCursor(int cur) {
    this.cur = cur;
  }

  /**
   * Initialize footer buffer in order to keep footer records at the end of file.
   *
   * @param job
   *          Current job configuration.
   *
   * @param recordreader
   *          Record reader.
   *
   * @param footerCount
   *          Footer line number of the table files.
   *
   * @param key
   *          Key of current reading record.
   *
   * @param value
   *          Value of current reading record.
   *
   * @return Return true if there are 0 or more records left in the file
   *         after initializing the footer buffer, otherwise return false.
   */
  public boolean initializeBuffer(JobConf job, RecordReader recordreader,
      int footerCount, WritableComparable key, Writable value) throws IOException {

    // Fill the buffer with key value pairs.
    this.buffer = new ArrayList<ObjectPair>();
    while (buffer.size() < footerCount) {
      boolean notEOF = recordreader.next(key, value);
      if (!notEOF) {
        return false;
      }
      ObjectPair tem = new ObjectPair();
      tem.setFirst(ReflectionUtils.copy(job, key, tem.getFirst()));
      tem.setSecond(ReflectionUtils.copy(job, value, tem.getSecond()));
      buffer.add(tem);
    }
    this.cur = 0;
    return true;
  }

  /**
   * Enqueue most recent record read, and dequeue earliest result in the queue.
   *
   * @param job
   *          Current job configuration.
   *
   * @param recordreader
   *          Record reader.
   *
   * @param key
   *          Key of current reading record.
   *
   * @param value
   *          Value of current reading record.
   *
   * @return Return false if reaches the end of file, otherwise return true.
   */
  public boolean updateBuffer(JobConf job, RecordReader recordreader,
      WritableComparable key, Writable value) throws IOException {
    key = ReflectionUtils.copy(job, (WritableComparable)buffer.get(cur).getFirst(), key);
    value = ReflectionUtils.copy(job, (Writable)buffer.get(cur).getSecond(), value);
    boolean notEOF = recordreader.next(buffer.get(cur).getFirst(), buffer.get(cur).getSecond());
    if (notEOF) {
      cur = (++cur) % buffer.size();
    }
    return notEOF;
  }

}
