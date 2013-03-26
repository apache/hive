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
package org.apache.hcatalog.pig.drivers;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
/**
 * based on {@link PigStorage}
 */
public class LoadFuncBasedInputFormat extends InputFormat<BytesWritable,Tuple> {

  private final LoadFunc loadFunc;
  private static ResourceFieldSchema[] fields;

  public LoadFuncBasedInputFormat(LoadFunc loadFunc, ResourceSchema dataSchema, String location, Configuration conf) throws IOException {

    this.loadFunc = loadFunc;
    fields = dataSchema.getFields();
    
    // Simulate the frontend call sequence for LoadFunc, in case LoadFunc need to store something into UDFContext (as JsonLoader does)
    if (loadFunc instanceof LoadMetadata) {
        ((LoadMetadata)loadFunc).getSchema(location, new Job(conf));
    }
  }

  @Override
  public RecordReader<BytesWritable, Tuple> createRecordReader(
      InputSplit split, TaskAttemptContext taskContext) throws IOException,
      InterruptedException {
    RecordReader<BytesWritable,Tuple> reader = loadFunc.getInputFormat().createRecordReader(split, taskContext);
    return new LoadFuncBasedRecordReader(reader, loadFunc);
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
  InterruptedException {
    try {
      InputFormat<BytesWritable,Tuple> inpFormat = loadFunc.getInputFormat();
      return inpFormat.getSplits(jobContext);

    } catch (InterruptedException    e) {
      throw new IOException(e);
    }
  }

  static class LoadFuncBasedRecordReader extends RecordReader<BytesWritable, Tuple> {

    private Tuple tupleFromDisk;
    private final RecordReader<BytesWritable,Tuple> reader;
    private final LoadFunc loadFunc;
    private final LoadCaster caster;

     /**
      * @param reader
      * @param loadFunc
      * @throws IOException
      */
     public LoadFuncBasedRecordReader(RecordReader<BytesWritable,Tuple> reader, LoadFunc loadFunc) throws IOException {
       this.reader = reader;
       this.loadFunc = loadFunc;
       this.caster = loadFunc.getLoadCaster();
     }

     @Override
     public void close() throws IOException {
       reader.close();
     }

     @Override
     public BytesWritable getCurrentKey() throws IOException,
     InterruptedException {
       return null;
     }

     @Override
     public Tuple getCurrentValue() throws IOException, InterruptedException {

       for(int i = 0; i < tupleFromDisk.size(); i++) {

         Object data = tupleFromDisk.get(i);
         
         // We will do conversion for bytes only for now
         if (data instanceof DataByteArray) {
         
             DataByteArray dba = (DataByteArray) data;
    
             if(dba == null) {
               // PigStorage will insert nulls for empty fields.
              tupleFromDisk.set(i, null);
              continue;
            }
    
             switch(fields[i].getType()) {
    
             case DataType.CHARARRAY:
               tupleFromDisk.set(i, caster.bytesToCharArray(dba.get()));
               break;
    
             case DataType.INTEGER:
               tupleFromDisk.set(i, caster.bytesToInteger(dba.get()));
               break;
    
             case DataType.FLOAT:
               tupleFromDisk.set(i, caster.bytesToFloat(dba.get()));
               break;
    
             case DataType.LONG:
               tupleFromDisk.set(i, caster.bytesToLong(dba.get()));
               break;
    
             case DataType.DOUBLE:
               tupleFromDisk.set(i, caster.bytesToDouble(dba.get()));
               break;
    
             case DataType.MAP:
               tupleFromDisk.set(i, caster.bytesToMap(dba.get()));
               break;
    
             case DataType.BAG:
               tupleFromDisk.set(i, caster.bytesToBag(dba.get(), fields[i]));
               break;
    
             case DataType.TUPLE:
               tupleFromDisk.set(i, caster.bytesToTuple(dba.get(), fields[i]));
               break;
    
             default:
               throw new IOException("Unknown Pig type in data: "+fields[i].getType());
             }
           }
       }

       return tupleFromDisk;
     }


     @Override
     public void initialize(InputSplit split, TaskAttemptContext ctx)
     throws IOException, InterruptedException {

       reader.initialize(split, ctx);
       loadFunc.prepareToRead(reader, null);
     }

     @Override
     public boolean nextKeyValue() throws IOException, InterruptedException {

       // even if we don't need any data from disk, we will need to call
       // getNext() on pigStorage() so we know how many rows to emit in our
       // final output - getNext() will eventually return null when it has
       // read all disk data and we will know to stop emitting final output
       tupleFromDisk = loadFunc.getNext();
       return tupleFromDisk != null;
     }

     @Override
     public float getProgress() throws IOException, InterruptedException {
       return 0;
     }

  }
}
