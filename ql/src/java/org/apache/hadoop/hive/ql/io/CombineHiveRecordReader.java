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

package org.apache.hadoop.hive.ql.io;
import java.io.IOException;

import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.hive.ql.exec.ExecMapper;

import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat.CombineHiveInputSplit;
import org.apache.hadoop.hive.shims.HadoopShims.InputSplitShim;

public class CombineHiveRecordReader<K extends WritableComparable, V extends Writable>  
  implements RecordReader<K, V> {
  
  private RecordReader recordReader;
  
  public CombineHiveRecordReader(InputSplit split, Configuration conf, 
                                 Reporter reporter, Integer partition) 
    throws IOException {
    JobConf job = (JobConf)conf;
    CombineHiveInputSplit hsplit = new CombineHiveInputSplit(job, (InputSplitShim)split);
    String inputFormatClassName = hsplit.inputFormatClassName(); 
    Class inputFormatClass = null;
    try {
      inputFormatClass = Class.forName(inputFormatClassName);
    } catch (ClassNotFoundException e) {
      throw new IOException ("CombineHiveRecordReader: class not found " + inputFormatClassName);
    }
    InputFormat inputFormat = CombineHiveInputFormat.getInputFormatFromCache(inputFormatClass, job);
    
    // create a split for the given partition
    FileSplit fsplit = new FileSplit(hsplit.getPaths()[partition],
                                     hsplit.getStartOffsets()[partition],
                                     hsplit.getLengths()[partition],
                                     hsplit.getLocations());
    
    this.recordReader = inputFormat.getRecordReader(fsplit, job, reporter);
  }
  
  public void close() throws IOException { 
    recordReader.close(); 
  }
  
  public K createKey() { 
    return (K)recordReader.createKey();
  }
  
  public V createValue() { 
    return (V)recordReader.createValue();
  }
  
  public long getPos() throws IOException { 
    return recordReader.getPos();
  }
  
  public float getProgress() throws IOException { 
    return recordReader.getProgress();
  }
  
  public boolean  next(K key, V value) throws IOException { 
    if (ExecMapper.getDone())
      return false;
    return recordReader.next(key, value);
  }
}

