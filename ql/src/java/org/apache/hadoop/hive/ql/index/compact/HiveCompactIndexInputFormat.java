package org.apache.hadoop.hive.ql.index.compact;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.IOPrepareCache;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

public class HiveCompactIndexInputFormat extends HiveInputFormat {

  public static final Log l4j = LogFactory.getLog("HiveIndexInputFormat");

  public HiveCompactIndexInputFormat() {
    super();
  }

  public InputSplit[] doGetSplits(JobConf job, int numSplits) throws IOException {

    super.init(job);

    Path[] dirs = FileInputFormat.getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    JobConf newjob = new JobConf(job);
    ArrayList<InputSplit> result = new ArrayList<InputSplit>();

    // for each dir, get the InputFormat, and do getSplits.
    for (Path dir : dirs) {
      PartitionDesc part = HiveFileFormatUtils
          .getPartitionDescFromPathRecursively(pathToPartitionInfo, dir,
              IOPrepareCache.get().allocatePartitionDescMap());
      // create a new InputFormat instance if this is the first time to see this
      // class
      Class inputFormatClass = part.getInputFileFormatClass();
      InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
      Utilities.copyTableJobPropertiesToConf(part.getTableDesc(), newjob);

      FileInputFormat.setInputPaths(newjob, dir);
      newjob.setInputFormat(inputFormat.getClass());
      InputSplit[] iss = inputFormat.getSplits(newjob, numSplits / dirs.length);
      for (InputSplit is : iss) {
        result.add(new HiveInputSplit(is, inputFormatClass.getName()));
      }
    }
    return result.toArray(new HiveInputSplit[result.size()]);
  }
  
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    String indexFileStr = job.get("hive.index.compact.file");
    l4j.info("index_file is " + indexFileStr);

    HiveCompactIndexResult hiveIndexResult = null;
    if (indexFileStr != null) {
      try {
        hiveIndexResult = new HiveCompactIndexResult(indexFileStr, job);
      } catch (HiveException e) {
        l4j.error("Unable to read index..");
        throw new IOException(e);
      }

      Set<String> inputFiles = hiveIndexResult.buckets.keySet();
      Iterator<String> iter = inputFiles.iterator();
      boolean first = true;
      StringBuilder newInputPaths = new StringBuilder();
      while(iter.hasNext()) {
        String path = iter.next();
        if (path.trim().equalsIgnoreCase(""))
          continue;
        if (!first) {
          newInputPaths.append(",");
        } else {
          first = false;
        }
        newInputPaths.append(path);
      }

      FileInputFormat.setInputPaths(job, newInputPaths.toString());
    } else {
      return super.getSplits(job, numSplits);
    }
    
    HiveInputSplit[] splits = (HiveInputSplit[]) this.doGetSplits(job, numSplits);

    ArrayList<HiveInputSplit> newSplits = new ArrayList<HiveInputSplit>(
        numSplits);
    for (HiveInputSplit split : splits) {
      l4j.info("split start : " + split.getStart());
      l4j.info("split end : " + (split.getStart() + split.getLength()));

      try {
        if (hiveIndexResult.contains(split)) {
          // we may miss a sync here
          HiveInputSplit newSplit = split;
          if (split.inputFormatClassName().contains("RCFile")
              || split.inputFormatClassName().contains("SequenceFile")) {
            if (split.getStart() > SequenceFile.SYNC_INTERVAL) {
              newSplit = new HiveInputSplit(new FileSplit(split.getPath(), split
                  .getStart()
                  - SequenceFile.SYNC_INTERVAL, split.getLength()
                  + SequenceFile.SYNC_INTERVAL, split.getLocations()), split
                  .inputFormatClassName());
            }
          }
          newSplits.add(newSplit);
        }
      } catch (HiveException e) {
        throw new RuntimeException(
            "Unable to get metadata for input table split" + split.getPath());
      }
    }
    InputSplit retA[] = newSplits.toArray((new FileSplit[newSplits.size()]));
    l4j.info("Number of input splits: " + splits.length + " new input splits: "
        + retA.length);
    return retA;
  }
}
