/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.avro;


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;


import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * RecordReader optimized against Avro GenericRecords that returns to record
 * as the value of the k-v pair, as Hive requires.
 */
public class AvroGenericRecordReader implements
        RecordReader<NullWritable, AvroGenericRecordWritable>, JobConfigurable {
  private static final Log LOG = LogFactory.getLog(AvroGenericRecordReader.class);

  final private org.apache.avro.file.FileReader<GenericRecord> reader;
  final private long start;
  final private long stop;
  protected JobConf jobConf;

  public AvroGenericRecordReader(JobConf job, FileSplit split, Reporter reporter) throws IOException {
    this.jobConf = job;
    Schema latest;

    try {
      latest = getSchema(job, split);
    } catch (AvroSerdeException e) {
      throw new IOException(e);
    }

    GenericDatumReader<GenericRecord> gdr = new GenericDatumReader<GenericRecord>();

    if(latest != null) gdr.setExpected(latest);

    this.reader = new DataFileReader<GenericRecord>(new FsInput(split.getPath(), job), gdr);
    this.reader.sync(split.getStart());
    this.start = reader.tell();
    this.stop = split.getStart() + split.getLength();
  }

  /**
   * Attempt to retrieve the reader schema.  We have a couple opportunities
   * to provide this, depending on whether or not we're just selecting data
   * or running with a MR job.
   * @return  Reader schema for the Avro object, or null if it has not been provided.
   * @throws AvroSerdeException
   */
  private Schema getSchema(JobConf job, FileSplit split) throws AvroSerdeException, IOException {
    FileSystem fs = split.getPath().getFileSystem(job);
    // Inside of a MR job, we can pull out the actual properties
    if(AvroSerdeUtils.insideMRJob(job)) {
      MapredWork mapRedWork = Utilities.getMapRedWork(job);

      // Iterate over the Path -> Partition descriptions to find the partition
      // that matches our input split.
      for (Map.Entry<String,PartitionDesc> pathsAndParts: mapRedWork.getPathToPartitionInfo().entrySet()){
        String partitionPath = pathsAndParts.getKey();
        if(pathIsInPartition(split.getPath(), partitionPath)) {
          if(LOG.isInfoEnabled()) {
              LOG.info("Matching partition " + partitionPath +
                      " with input split " + split);
          }

          Properties props = pathsAndParts.getValue().getProperties();
          if(props.containsKey(AvroSerdeUtils.SCHEMA_LITERAL) || props.containsKey(AvroSerdeUtils.SCHEMA_URL)) {
            return AvroSerdeUtils.determineSchemaOrThrowException(props);
          } else
            return null; // If it's not in this property, it won't be in any others
        }
      }
      if(LOG.isInfoEnabled()) LOG.info("Unable to match filesplit " + split + " with a partition.");
    }

    // In "select * from table" situations (non-MR), we can add things to the job
    // It's safe to add this to the job since it's not *actually* a mapred job.
    // Here the global state is confined to just this process.
    String s = job.get(AvroSerdeUtils.AVRO_SERDE_SCHEMA);
    if(s != null) {
      LOG.info("Found the avro schema in the job: " + s);
      return Schema.parse(s);
    }
    // No more places to get the schema from. Give up.  May have to re-encode later.
    return null;
  }

  private boolean pathIsInPartition(Path split, String partitionPath) {
    boolean schemeless = split.toUri().getScheme() == null;
    if (schemeless) {
      String schemelessPartitionPath = new Path(partitionPath).toUri().getPath();
      return split.toString().startsWith(schemelessPartitionPath);
    } else {
      return split.toString().startsWith(partitionPath);
    }
  }


  @Override
  public boolean next(NullWritable nullWritable, AvroGenericRecordWritable record) throws IOException {
    if(!reader.hasNext() || reader.pastSync(stop)) {
      return false;
    }

    GenericData.Record r = (GenericData.Record)reader.next();
    record.setRecord(r);

    return true;
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public AvroGenericRecordWritable createValue() {
    return new AvroGenericRecordWritable();
  }

  @Override
  public long getPos() throws IOException {
    return reader.tell();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public float getProgress() throws IOException {
    return stop == start ? 0.0f
                         : Math.min(1.0f, (getPos() - start) / (float)(stop - start));
  }

  @Override
  public void configure(JobConf jobConf) {
    this.jobConf= jobConf;
  }
}
