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
package org.apache.hcatalog.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.mapreduce.HCatSplit;

/**
 * Even though HiveInputSplit expects an InputSplit to wrap, it
 * expects getPath() to work from the underlying split. And since
 * that's populated by HiveInputSplit only if the underlying
 * split is a FileSplit, the HCatSplit that goes to Hive needs
 * to be a FileSplit. And since FileSplit is a class, and 
 * mapreduce.InputSplit is also a class, we can't do the trick
 * where we implement mapred.inputSplit and extend mapred.InputSplit.
 * 
 * Thus, we compose the other HCatSplit, and work with it.
 * 
 * Also, this means that reading HCat through Hive will only work
 * when the underlying InputFormat's InputSplit has implemented
 * a getPath() - either by subclassing FileSplit, or by itself -
 * we make a best effort attempt to call a getPath() via reflection,
 * but if that doesn't work, this isn't going to work.
 *
 */
public class HiveHCatSplitWrapper extends FileSplit implements InputSplit {
  
  Log LOG = LogFactory.getLog(HiveHCatSplitWrapper.class);

  HCatSplit hsplit;
  
  public HiveHCatSplitWrapper() {
    super((Path) null, 0, 0, (String[]) null);
  }
  
  public HiveHCatSplitWrapper(HCatSplit hsplit) {
    this();
    this.hsplit = hsplit;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    hsplit = new HCatSplit();
    hsplit.readFields(input);
  }
  
  @Override
  public void write(DataOutput output) throws IOException {
    hsplit.write(output);
  }
  
  @Override
  public long getLength() {
    return hsplit.getLength();
  }

  @Override
  public String[] getLocations() throws IOException {
    return hsplit.getLocations();
  }

  @Override
  public Path getPath() {
    /**
     * This function is the reason this class exists at all.
     * See class description for why.
     */
    if (hsplit.getBaseSplit() instanceof FileSplit){
      // if baseSplit is a FileSplit, then return that.
      return ((FileSplit)hsplit.getBaseSplit()).getPath();
    } else {
      // use reflection to try and determine if underlying class has a getPath() method that returns a path
      Class<?> c = hsplit.getBaseSplit().getClass();
      try {
        return (Path) (c.getMethod("getPath")).invoke(hsplit.getBaseSplit());
      } catch (Exception e) {
        HCatUtil.logStackTrace(LOG);
        // not much we can do - default exit will return null Path
      }
      
    }
    LOG.error("Returning empty path from getPath(), Hive will not be happy.");
    return new Path(""); // This will cause hive to error, but we can't do anything for that situation.
  }

  public HCatSplit getHCatSplit() {
    return hsplit;
  }
  
}
