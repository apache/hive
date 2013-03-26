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
package org.apache.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;

/** The abstract class to be implemented by underlying storage drivers to enable data access from Howl through
 *  HowlInputFormat.
 */
public abstract class HCatInputStorageDriver {

  public void initialize(JobContext context, Properties storageDriverArgs) throws IOException {
    // trivial do nothing
  }

  /**
   * Returns the InputFormat to use with this Storage Driver.
   * @param properties the properties containing parameters required for initialization of InputFormat
   * @return the InputFormat instance
   */
  public abstract InputFormat<? extends WritableComparable, ? extends Writable> getInputFormat(Properties howlProperties);


  /**
   * Converts to HowlRecord format usable by HowlInputFormat to convert to required valuetype.
   * Implementers of StorageDriver should look to overwriting this function so as to convert their
   * value type to HowlRecord. Default implementation is provided for StorageDriver implementations
   * on top of an underlying InputFormat that already uses HowlRecord as a tuple
   * @param value the underlying value to convert to HowlRecord
   */
  public abstract HCatRecord convertToHCatRecord(WritableComparable baseKey, Writable baseValue) throws IOException;

  /**
   * Set the data location for the input.
   * @param jobContext the job context object
   * @param location the data location
   * @throws IOException Signals that an I/O exception has occurred.
   *
   * Default implementation for FileInputFormat based Input Formats. Override
   * this for other input formats.
   */
  public void setInputPath(JobContext jobContext, String location) throws IOException{

    // ideally we should just call FileInputFormat.setInputPaths() here - but
    // that won't work since FileInputFormat.setInputPaths() needs
    // a Job object instead of a JobContext which we are handed here

    int length = location.length();
    int curlyOpen = 0;
    int pathStart = 0;
    boolean globPattern = false;
    List<String> pathStrings = new ArrayList<String>();

    for (int i=0; i<length; i++) {
      char ch = location.charAt(i);
      switch(ch) {
      case '{' : {
        curlyOpen++;
        if (!globPattern) {
          globPattern = true;
        }
        break;
      }
      case '}' : {
        curlyOpen--;
        if (curlyOpen == 0 && globPattern) {
          globPattern = false;
        }
        break;
      }
      case ',' : {
        if (!globPattern) {
          pathStrings.add(location.substring(pathStart, i));
          pathStart = i + 1 ;
        }
        break;
      }
      }
    }
    pathStrings.add(location.substring(pathStart, length));

    Path[] paths = StringUtils.stringToPath(pathStrings.toArray(new String[0]));

    Configuration conf = jobContext.getConfiguration();

    FileSystem fs = FileSystem.get(conf);
    Path path = paths[0].makeQualified(fs);
    StringBuilder str = new StringBuilder(StringUtils.escapeString(path.toString()));
    for(int i = 1; i < paths.length;i++) {
      str.append(StringUtils.COMMA_STR);
      path = paths[i].makeQualified(fs);
      str.append(StringUtils.escapeString(path.toString()));
    }

    conf.set("mapred.input.dir", str.toString());
  }

  /**
   * Set the schema of the data as originally published in Howl. The storage driver might validate that this matches with
   * the schema it has (like Zebra) or it will use this to create a HowlRecord matching the output schema.
   * @param jobContext the job context object
   * @param howlSchema the schema published in Howl for this data
   * @param instantiationState
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public abstract void setOriginalSchema(JobContext jobContext, HCatSchema howlSchema) throws IOException;

  /**
   * Set the consolidated schema for the HowlRecord data returned by the storage driver. All tuples returned by the RecordReader should
   * have this schema. Nulls should be inserted for columns not present in the data.
   * @param jobContext the job context object
   * @param howlSchema the schema to use as the consolidated schema
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public abstract void setOutputSchema(JobContext jobContext, HCatSchema howlSchema) throws IOException;

  /**
   * Sets the partition key values for the current partition. The storage driver is passed this so that the storage
   * driver can add the partition key values to the output HowlRecord if the partition key values are not present on disk.
   * @param jobContext the job context object
   * @param partitionValues the partition values having a map with partition key name as key and the HowlKeyValue as value
   * @param instantiationState
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public abstract void setPartitionValues(JobContext jobContext, Map<String,String> partitionValues) throws IOException;

}
