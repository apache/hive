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
package org.apache.hcatalog.pig;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.apache.hcatalog.mapreduce.PartInfo;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;

/**
 * Base class for HCatLoader and HCatEximLoader
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.pig.HCatBaseLoader} instead
 */

abstract class HCatBaseLoader extends LoadFunc implements LoadMetadata, LoadPushDown {

  protected static final String PRUNE_PROJECTION_INFO = "prune.projection.info";

  private RecordReader<?, ?> reader;
  protected String signature;

  HCatSchema outputSchema = null;


  @Override
  public Tuple getNext() throws IOException {
    try {
      HCatRecord hr = (HCatRecord) (reader.nextKeyValue() ? reader.getCurrentValue() : null);
      Tuple t = PigHCatUtil.transformToTuple(hr, outputSchema);
      // TODO : we were discussing an iter interface, and also a LazyTuple
      // change this when plans for that solidifies.
      return t;
    } catch (ExecException e) {
      int errCode = 6018;
      String errMsg = "Error while reading input";
      throw new ExecException(errMsg, errCode,
        PigException.REMOTE_ENVIRONMENT, e);
    } catch (Exception eOther) {
      int errCode = 6018;
      String errMsg = "Error converting read value to tuple";
      throw new ExecException(errMsg, errCode,
        PigException.REMOTE_ENVIRONMENT, eOther);
    }

  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit arg1) throws IOException {
    this.reader = reader;
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job) throws IOException {
    // statistics not implemented currently
    return null;
  }

  @Override
  public List<OperatorSet> getFeatures() {
    return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
  }

  @Override
  public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldsInfo) throws FrontendException {
    // Store the required fields information in the UDFContext so that we
    // can retrieve it later.
    storeInUDFContext(signature, PRUNE_PROJECTION_INFO, requiredFieldsInfo);

    // HCat will always prune columns based on what we ask of it - so the
    // response is true
    return new RequiredFieldResponse(true);
  }

  @Override
  public void setUDFContextSignature(String signature) {
    this.signature = signature;
  }


  // helper methods
  protected void storeInUDFContext(String signature, String key, Object value) {
    UDFContext udfContext = UDFContext.getUDFContext();
    Properties props = udfContext.getUDFProperties(
      this.getClass(), new String[]{signature});
    props.put(key, value);
  }

  /**
   * A utility method to get the size of inputs. This is accomplished by summing the
   * size of all input paths on supported FileSystems. Locations whose size cannot be
   * determined are ignored. Note non-FileSystem and unpartitioned locations will not
   * report their input size by default.
   */
  protected static long getSizeInBytes(InputJobInfo inputJobInfo) throws IOException {
    Configuration conf = new Configuration();
    long sizeInBytes = 0;

    for (PartInfo partInfo : inputJobInfo.getPartitions()) {
      try {
        Path p = new Path(partInfo.getLocation());
        if (p.getFileSystem(conf).isFile(p)) {
          sizeInBytes += p.getFileSystem(conf).getFileStatus(p).getLen();
        } else {
          FileStatus[] fileStatuses = p.getFileSystem(conf).listStatus(p);
          if (fileStatuses != null) {
            for (FileStatus child : fileStatuses) {
              sizeInBytes += child.getLen();
            }
          }
        }
      } catch (IOException e) {
        // Report size to the extent possible.
      }
    }

    return sizeInBytes;
  }
}
