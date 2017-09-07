/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hive.storage.jdbc.dao.DatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessorFactory;
import org.apache.hive.storage.jdbc.dao.JdbcRecordIterator;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

public class JdbcRecordReader implements RecordReader<LongWritable, MapWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRecordReader.class);
  private DatabaseAccessor dbAccessor = null;
  private JdbcRecordIterator iterator = null;
  private JdbcInputSplit split = null;
  private JobConf conf = null;
  private int pos = 0;


  public JdbcRecordReader(JobConf conf, JdbcInputSplit split) {
    LOGGER.debug("Initializing JdbcRecordReader");
    this.split = split;
    this.conf = conf;
  }


  @Override
  public boolean next(LongWritable key, MapWritable value) throws IOException {
    try {
      LOGGER.debug("JdbcRecordReader.next called");
      if (dbAccessor == null) {
        dbAccessor = DatabaseAccessorFactory.getAccessor(conf);
        iterator = dbAccessor.getRecordIterator(conf, split.getLimit(), split.getOffset());
      }

      if (iterator.hasNext()) {
        LOGGER.debug("JdbcRecordReader has more records to read.");
        key.set(pos);
        pos++;
        Map<String, Object> record = iterator.next();
        if ((record != null) && (!record.isEmpty())) {
          for (Entry<String, Object> entry : record.entrySet()) {
            value.put(new Text(entry.getKey()),
                entry.getValue() == null ? NullWritable.get() : new ObjectWritable(entry.getValue()));
          }
          return true;
        }
        else {
          LOGGER.debug("JdbcRecordReader got null record.");
          return false;
        }
      }
      else {
        LOGGER.debug("JdbcRecordReader has no more records to read.");
        return false;
      }
    }
    catch (Exception e) {
      LOGGER.error("An error occurred while reading the next record from DB.", e);
      return false;
    }
  }


  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }


  @Override
  public MapWritable createValue() {
    return new MapWritable();
  }


  @Override
  public long getPos() throws IOException {
    return pos;
  }


  @Override
  public void close() throws IOException {
    if (iterator != null) {
      iterator.close();
    }
  }


  @Override
  public float getProgress() throws IOException {
    if (split == null) {
      return 0;
    }
    else {
      return split.getLength() > 0 ? pos / (float) split.getLength() : 1.0f;
    }
  }


  public void setDbAccessor(DatabaseAccessor dbAccessor) {
    this.dbAccessor = dbAccessor;
  }


  public void setIterator(JdbcRecordIterator iterator) {
    this.iterator = iterator;
  }

}
