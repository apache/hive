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


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.rmi.server.UID;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AvroTableProperties;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * RecordReader optimized against Avro GenericRecords that returns to record
 * as the value of the k-v pair, as Hive requires.
 */
public class AvroGenericRecordReader implements
        RecordReader<NullWritable, AvroGenericRecordWritable>, JobConfigurable {
  private static final Logger LOG = LoggerFactory.getLogger(AvroGenericRecordReader.class);

  final private org.apache.avro.file.FileReader<GenericRecord> reader;
  final private long start;
  final private long stop;
  private ZoneId writerTimezone;
  private Boolean writerProleptic;
  protected JobConf jobConf;
  final private boolean isEmptyInput;
  /**
   * A unique ID for each record reader.
   */
  final private UID recordReaderID;

  public AvroGenericRecordReader(JobConf job, FileSplit split, Reporter reporter) throws IOException {
    this.jobConf = job;
    Schema latest;

    try {
      latest = getSchema(job, split);
    } catch (AvroSerdeException e) {
      throw new IOException(e);
    }

    GenericDatumReader<GenericRecord> gdr = new GenericDatumReader<GenericRecord>();

    if(latest != null) {
      gdr.setExpected(latest);
    }

    if (split.getLength() == 0) {
      this.isEmptyInput = true;
      this.start = 0;
      this.reader = null;
    }
    else {
      this.isEmptyInput = false;
      this.reader = new DataFileReader<GenericRecord>(new FsInput(split.getPath(), job), gdr);
      this.reader.sync(split.getStart());
      this.start = reader.tell();
    }
    this.stop = split.getStart() + split.getLength();
    this.recordReaderID = new UID();

    this.writerTimezone = extractWriterTimezoneFromMetadata(job, split, gdr);
    this.writerProleptic = extractWriterProlepticFromMetadata(job, split, gdr);
  }

  /**
   * Attempt to retrieve the reader schema.  We have a couple opportunities
   * to provide this, depending on whether or not we're just selecting data
   * or running with a MR job.
   * @return  Reader schema for the Avro object, or null if it has not been provided.
   * @throws AvroSerdeException
   */
  private Schema getSchema(JobConf job, FileSplit split) throws AvroSerdeException, IOException {
    // Inside of a MR job, we can pull out the actual properties
    if(AvroSerdeUtils.insideMRJob(job)) {
      MapWork mapWork = Utilities.getMapWork(job);

      // Iterate over the Path -> Partition descriptions to find the partition
      // that matches our input split.
      for (Map.Entry<Path,PartitionDesc> pathsAndParts: mapWork.getPathToPartitionInfo().entrySet()){
        Path partitionPath = pathsAndParts.getKey();
        if(pathIsInPartition(split.getPath(), partitionPath)) {
          LOG.info("Matching partition {} with input split {}", partitionPath,
              split);

          Properties props = pathsAndParts.getValue().getProperties();
          if(props.containsKey(AvroTableProperties.SCHEMA_LITERAL.getPropName()) || props.containsKey(AvroTableProperties.SCHEMA_URL.getPropName())) {
            return AvroSerdeUtils.determineSchemaOrThrowException(job, props);
          }
          else {
            return null; // If it's not in this property, it won't be in any others
          }
        }
      }
      LOG.info("Unable to match filesplit {} with a partition.", split);
    }

    // In "select * from table" situations (non-MR), we can add things to the job
    // It's safe to add this to the job since it's not *actually* a mapred job.
    // Here the global state is confined to just this process.
    String s = job.get(AvroTableProperties.AVRO_SERDE_SCHEMA.getPropName());
    if (StringUtils.isNotBlank(s)) {
      LOG.info("Found the avro schema in the job");
      LOG.debug("Avro schema: {}", s);
      return AvroSerdeUtils.getSchemaFor(s);
    }
    // No more places to get the schema from. Give up.  May have to re-encode later.
    return null;
  }

  private ZoneId extractWriterTimezoneFromMetadata(JobConf job, FileSplit split,
      GenericDatumReader<GenericRecord> gdr) throws IOException {
    if (job == null || gdr == null || split == null || split.getPath() == null) {
      return null;
    }
    try {
      DataFileReader<GenericRecord> dataFileReader =
          new DataFileReader<GenericRecord>(new FsInput(split.getPath(), job), gdr);
      if (dataFileReader.getMeta(AvroSerDe.WRITER_TIME_ZONE) != null) {
        try {
          return ZoneId.of(new String(dataFileReader.getMeta(AvroSerDe.WRITER_TIME_ZONE),
              StandardCharsets.UTF_8));
        } catch (DateTimeException e) {
          throw new RuntimeException("Can't parse writer time zone stored in file metadata", e);
        }
      }
    } catch (IOException e) {
      // Can't access metadata, carry on.
    }
    return null;
  }

  private Boolean extractWriterProlepticFromMetadata(JobConf job, FileSplit split,
      GenericDatumReader<GenericRecord> gdr) throws IOException {
    if (job == null || gdr == null || split == null || split.getPath() == null) {
      return null;
    }
    try {
      DataFileReader<GenericRecord> dataFileReader =
          new DataFileReader<GenericRecord>(new FsInput(split.getPath(), job), gdr);
      if (dataFileReader.getMeta(AvroSerDe.WRITER_PROLEPTIC) != null) {
        try {
          return Boolean.valueOf(new String(dataFileReader.getMeta(AvroSerDe.WRITER_PROLEPTIC),
              StandardCharsets.UTF_8));
        } catch (DateTimeException e) {
          throw new RuntimeException("Can't parse writer proleptic property stored in file metadata", e);
        }
      }
    } catch (IOException e) {
      // Can't access metadata, carry on.
    }
    return null;
  }

  private boolean pathIsInPartition(Path split, Path partitionPath) {
    boolean schemeless = split.toUri().getScheme() == null;
    if (schemeless) {
      Path pathNoSchema = Path.getPathWithoutSchemeAndAuthority(partitionPath);
      return FileUtils.isPathWithinSubtree(split,pathNoSchema);
    } else {
      return FileUtils.isPathWithinSubtree(split,partitionPath);
    }
  }


  @Override
  public boolean next(NullWritable nullWritable, AvroGenericRecordWritable record) throws IOException {
    if(isEmptyInput || !reader.hasNext() || reader.pastSync(stop)) {
      return false;
    }

    GenericData.Record r = (GenericData.Record)reader.next();
    record.setRecord(r);
    record.setRecordReaderID(recordReaderID);
    record.setFileSchema(reader.getSchema());

    return true;
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public AvroGenericRecordWritable createValue() {
    return new AvroGenericRecordWritable(writerTimezone, writerProleptic);
  }

  @Override
  public long getPos() throws IOException {
    return isEmptyInput ? 0 : reader.tell();
  }

  @Override
  public void close() throws IOException {
    if (isEmptyInput == false)
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
