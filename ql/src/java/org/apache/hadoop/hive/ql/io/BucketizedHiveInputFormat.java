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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * BucketizedHiveInputFormat serves the similar function as hiveInputFormat but
 * its getSplits() always group splits from one input file into one wrapper
 * split. It is useful for the applications that requires input files to fit in
 * one mapper.
 */
public class BucketizedHiveInputFormat<K extends WritableComparable, V extends Writable>
    extends HiveInputFormat<K, V> {

  public static final Logger LOG = LoggerFactory.getLogger(BucketizedHiveInputFormat.class);

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {

    BucketizedHiveInputSplit hsplit = (BucketizedHiveInputSplit) split;

    String inputFormatClassName = null;
    Class inputFormatClass = null;
    try {
      inputFormatClassName = hsplit.inputFormatClassName();
      inputFormatClass = job.getClassByName(inputFormatClassName);
    } catch (Exception e) {
      throw new IOException("cannot find class " + inputFormatClassName);
    }

    pushProjectionsAndFiltersAndAsOf(job, hsplit.getPath());

    InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);

    BucketizedHiveRecordReader<K, V> rr= new BucketizedHiveRecordReader(inputFormat, hsplit, job,
        reporter);
    rr.initIOContext(hsplit, job, inputFormatClass);
    return rr;
  }

  /**
   * Recursively lists status for all files starting from the directory dir
   * @param job
   * @param dir
   * @return
   * @throws IOException
   */
  protected FileStatus[] listStatus(JobConf job, Path dir) throws IOException {
    ArrayList<FileStatus> result = new ArrayList<FileStatus>();
    List<IOException> errors = new ArrayList<IOException>();

    FileSystem fs = dir.getFileSystem(job);
    FileStatus[] matches = fs.globStatus(dir, FileUtils.HIDDEN_FILES_PATH_FILTER);
    if (matches == null) {
      errors.add(new IOException("Input path does not exist: " + dir));
    } else if (matches.length == 0) {
      errors.add(new IOException("Input Pattern " + dir + " matches 0 files"));
    } else {
      for (FileStatus globStat : matches) {
        FileUtils.listStatusRecursively(fs, globStat, result);
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    LOG.debug("Matches for " + dir + ": " + result);
    LOG.info("Total input paths to process : " + result.size() + " from dir " + dir);
    return result.toArray(new FileStatus[result.size()]);

  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    init(job);

    Path[] dirs = getInputPaths(job);

    JobConf newjob = new JobConf(job);
    ArrayList<InputSplit> result = new ArrayList<InputSplit>();

    int numOrigSplits = 0;
    // for each dir, get all files under the dir, do getSplits to each
    // individual file,
    // and then create a BucketizedHiveInputSplit on it

    ArrayList<Path> currentDir = null;
    for (Path dir : dirs) {
      PartitionDesc part = getPartitionDescFromPath(pathToPartitionInfo, dir);
      // create a new InputFormat instance if this is the first time to see this class
      Class inputFormatClass = part.getInputFileFormatClass();
      InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
      newjob.setInputFormat(inputFormat.getClass());

      ValidWriteIdList mmIds = null;
      if (part.getTableDesc() != null) {
        // This can happen for truncate table case for non-MM tables.
        mmIds = getMmValidWriteIds(newjob, part.getTableDesc(), null);
      }
      // TODO: should this also handle ACID operation, etc.? seems to miss a lot of stuff from HIF.
      List<Path> finalDirs = null, dirsWithMmOriginals = null;
      if (mmIds == null) {
        finalDirs = Lists.newArrayList(dir);
      } else {
        finalDirs = new ArrayList<>();
        dirsWithMmOriginals = new ArrayList<>();
        processPathsForMmRead(
            Lists.newArrayList(dir), newjob, mmIds, finalDirs, dirsWithMmOriginals);
      }
      if (finalDirs.isEmpty() && (dirsWithMmOriginals == null || dirsWithMmOriginals.isEmpty())) {
        continue; // No valid inputs - possible in MM case.
      }

      for (Path finalDir : finalDirs) {
        FileStatus[] listStatus = listStatus(newjob, finalDir);
        for (FileStatus status : listStatus) {
          numOrigSplits = addBHISplit(
              status, inputFormat, inputFormatClass, numOrigSplits, newjob, result);
        }
      }
      if (dirsWithMmOriginals != null) {
        for (Path originalsDir : dirsWithMmOriginals) {
          FileSystem fs = originalsDir.getFileSystem(job);
          FileStatus[] listStatus = fs.listStatus(dir, FileUtils.HIDDEN_FILES_PATH_FILTER);
          for (FileStatus status : listStatus) {
            if (status.isDirectory()) continue;
            numOrigSplits = addBHISplit(
                status, inputFormat, inputFormatClass, numOrigSplits, newjob, result);
          }
        }
      }
    }

    LOG.info(result.size() + " bucketized splits generated from "
        + numOrigSplits + " original splits.");
    return result.toArray(new BucketizedHiveInputSplit[result.size()]);
  }

  private int addBHISplit(FileStatus status, InputFormat inputFormat, Class inputFormatClass,
      int numOrigSplits, JobConf newjob, ArrayList<InputSplit> result) throws IOException {
    LOG.info("block size: " + status.getBlockSize());
    LOG.info("file length: " + status.getLen());
    FileInputFormat.setInputPaths(newjob, status.getPath());
    InputSplit[] iss = inputFormat.getSplits(newjob, 0);
    if (iss != null && iss.length > 0) {
      numOrigSplits += iss.length;
      result.add(new BucketizedHiveInputSplit(iss, inputFormatClass
          .getName()));
    }
    return numOrigSplits;
  }
}
