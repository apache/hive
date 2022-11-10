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
package org.apache.hadoop.hive.ql.exec.repl;

import jodd.exception.UncheckedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType.ALL_OPEN_FILES;

public class ReplicationMigrationTool implements Tool {

  protected static transient Logger LOG = LoggerFactory.getLogger(ReplicationMigrationTool.class);
  private Configuration conf;
  private String help = "\tSample Usage: \n"
      + "hive --replMigration -dumpFilePath <path to external table info file> [-dirLevelCheck] "
      + "[-fileLevelCheck] [-verifyOpenFiles] [-verifyChecksum] [-filters] [-conf] [-queueSize] [-numThreads]"
      + " [-checksumQueueSize] [-checksumNumThreads] [-help]\n"
      + "-dumpFilePath: The fully qualified path to the external table info file. \n"
      + "-dirLevelCheck: Validate at directory level."
      + "-fileLevelCheck: Validate at file level."
      + "-verifyOpenFiles: Validate there is no open files on the source path. \n"
      + "-verifyChecksum: Whether the checksum needs to be validated for each file. Can not be used with "
      + "-dirLevelCheck. Will fail in case the source & target are in different encryption zones or uses different "
      + "checksum algorithm.\n"
      + "-filters: Comma separated list of filters, Can not be used along with -dirLevelCheck. \n"
      + "-conf: Semi-Colon separated list of additional configurations in key1=value1;key2=value2 format. \n"
      + "-queueSize: Queue size for the thread pool executor for table level validation. Default:200"
      + "-numThreads: Number of threads for thread pool executor for table level validation. Default:10"
      + "-checksumQueueSize: Queue size for the thread pool executor for checksum computation. Default:200"
      + "-checksumNumThreads: Number of threads for thread pool executor for checksum computation. Default:5"
      + "-help: Prints the help message.\n"
      +"Note: The dumpFilePath for a scheduled query can be fetched using the beeline query: \n"
      + "select * from sys.replication_metrics where policy_name=‘<policy name>’ order by scheduled_execution_id desc"
      + " limit 1;";

  @Override
  public int run(String[] args) throws Exception {
    List<String> argsList = new ArrayList<String>(Arrays.asList(args));
    boolean isHelp = StringUtils.popOption("-help", argsList);
    // If the help option is specified, print the help message and exit.
    if (isHelp) {
      System.out.println(help);
      return 0;
    }

    // Extract the options provided.
    final String dumpPath = StringUtils.popOptionWithArgument("-dumpFilePath", argsList);
    boolean isValidateChecksum = StringUtils.popOption("-verifyChecksum", argsList);
    boolean checkAtDirLevel = StringUtils.popOption("-dirLevelCheck", argsList);
    boolean checkAtFileLevel = StringUtils.popOption("-fileLevelCheck", argsList);
    boolean checkOpenFiles = StringUtils.popOption("-verifyOpenFiles", argsList);
    // Get the patterns to be filtered.
    List<Pattern> filtersPattern = getFilterPatterns(argsList);
    // Extract the extra configs if provided.
    extractAndSetConfigs(argsList);

    // Initialise the thread pool executor as per the options provided, else use a queue size of 200 and number of
    // threads as default 10
    int queueSize = getParamValue(argsList, "-queueSize", 200);
    int numThreads = getParamValue(argsList, "-numThreads", 10);
    ThreadPoolExecutor threadPool = getThreadPoolExecutor(queueSize, numThreads);


    LOG.info("Using a ThreadPoolExecutor with {} threads and {} as Queue size.", numThreads, queueSize);

    // Initialise a ThreadPoolExecutor for checksum computation, if verifyChecksum is specified, using the parameters
    // provided, or using a default queue size of 200 and number of threads as 5.
    ThreadPoolExecutor threadPoolChecksum = null;
    if (isValidateChecksum) {
      // Get checksum TPE parameters.
      int checksumQueueSize = getParamValue(argsList, "-checksumQueueSize", 200);
      int checksumNumThreads = getParamValue(argsList, "-checksumNumThreads", 5);

      threadPoolChecksum = getThreadPoolExecutor(checksumQueueSize, checksumNumThreads);

      LOG.info("Using a ThreadPoolExecutor with {} threads and {} as Queue size for checksum computation.",
          checksumNumThreads, checksumQueueSize);
    }

    // All the arguments have been parsed, verify the combination of arguments passed and if no irrelevant option is
    // passed.
    validateArguments(argsList, dumpPath, isValidateChecksum, checkAtFileLevel);

    Path dump = getExternalTableFileListPath(dumpPath);
    FileSystem fs = dump.getFileSystem(conf);
    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(dump)));
    ArrayList<Future<Boolean>> futures = new ArrayList();
    long startTime = System.currentTimeMillis();
    try {
      String line;
      line = br.readLine();
      while (line != null) {
        LOG.debug("Line read from {} is {} :", dumpPath, line);
        DirectoryProcessor dProcessor =
            new DirectoryProcessor(line, checkAtDirLevel, checkAtFileLevel, checkOpenFiles, isValidateChecksum,
                filtersPattern, threadPoolChecksum);
        Future<Boolean> future = threadPool.submit(dProcessor);
        futures.add(future);
        line = br.readLine();
      }

      // Get All the futures.
      int failed = 0;
      for (Future<Boolean> f : futures) {
        if (!f.get()) {
          failed++;
        }
      }

      LOG.error("Total {} paths failed", failed);
      threadPool.shutdown();

      System.out.println("Completed verification. Source & Target are " + (failed == 0 ? "in Sync." : "not in Sync."));
      System.out.println("Time Taken: " + (System.currentTimeMillis() - startTime) + " ms");
    } catch (UnsupportedOperationException e) {
      System.err.println(e.getMessage());
      System.err.println(help);
      return -1;
    } finally {
      br.close();
    }
    return 0;
  }

  // Utility method to validate the arguments specified.
  private void validateArguments(List<String> argsList, String dumpPath, boolean isValidateChecksum,
      boolean checkAtFileLevel) {
    if (!checkAtFileLevel && isValidateChecksum) {
      throw new UnsupportedOperationException("-verifyChecksum can not be used without fileLevelCheck");
    }
    if (dumpPath == null || dumpPath.isEmpty()) {
      throw new UnsupportedOperationException("-dumpFilePath is not specified");
    }
    // Check if any unrelated option wasn't passed.
    if (!argsList.isEmpty()) {
      throw new UnsupportedOperationException("Invalid Arguments: " + argsList.toString());
    }
  }

  @NotNull
  private ThreadPoolExecutor getThreadPoolExecutor(int queueSize, int numThreads) {
    // Create a blocking queue of the size specified.
    final BlockingQueue queue = new ArrayBlockingQueue<Runnable>(queueSize);

    // Create the thread pool executor.
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, queue);

    // Create a rejection handler, which can wait until the thread pool executor can accept any further tasks.
    threadPoolExecutor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        // this will block if the queue is full
        try {
          executor.getQueue().put(r);
        } catch (InterruptedException e) {
          throw new UncheckedException(e);
        }
      }
    });
    return threadPoolExecutor;
  }

  // Extracts the integer value for a param, if the value specified is not positive or isn't explicitly specified
  // return the default value
  private int getParamValue(List<String> argsList, String parameterName, int defaultValue) {
    String queueSizeString = StringUtils.popOptionWithArgument(parameterName, argsList);
    int queueSize = defaultValue;
    if (queueSizeString != null) {
      queueSize = Integer.parseInt(queueSizeString);
      if (queueSize < 0) {
        // Using default value.
        queueSize = defaultValue;
      }
    }
    return queueSize;
  }

  @NotNull
  private List<Pattern> getFilterPatterns(List<String> argsList) {
    List<Pattern> filtersPattern = new ArrayList<>();
    String filter = StringUtils.popOptionWithArgument("-filters", argsList);
    if (filter != null && !filter.isEmpty()) {
      String[] filters = filter.split(",");
      for (String filterEntry : filters) {
        Pattern pattern = Pattern.compile(filterEntry);
        filtersPattern.add(pattern);
      }
    }
    return filtersPattern;
  }

  // Extracts the configuration specified using -conf option and set it in the configuration object.
  private void extractAndSetConfigs(List<String> argsList) throws IOException {
    final String configs = StringUtils.popOptionWithArgument("-conf", argsList);
    if (configs != null && !configs.isEmpty()) {
      String[] configList = configs.split(";");
      for (String confPair : configList) {
        String[] confArr = confPair.split("=");
        if (confArr.length != 2) {
          throw new IOException("Invalid Configuration " + confPair);
        } else {
          conf.set(confArr[0], confArr[1]);
        }
      }
    }
  }

  private boolean validateOpenFilesAtSource(Path srcPath, FileSystem srcFileSystem) throws IOException {
    // OpenFiles API is HDFS specific, so this can be checked only if the source filesystem is of
    // DistributedFileSystem.
    if (srcFileSystem instanceof DistributedFileSystem) {
      DistributedFileSystem srcDFS = (DistributedFileSystem) srcPath.getFileSystem(conf);
      // If there is even single open file we can abort.
      if (srcDFS.listOpenFiles(EnumSet.of(ALL_OPEN_FILES), Path.getPathWithoutSchemeAndAuthority(srcPath).toString())
          .hasNext()) {
        System.out.println("There are open files in " + srcPath);
        return false;
      } else {
        LOG.error("Open file check is ignored since the source filesystem is not of type of "
            + "DistributedFileSystem. The source file system is of " + srcFileSystem.getClass() + " type.");
      }
    }
    return true;
  }

  private boolean validateAtDirectoryLevel(Path srcPath, Path trgPath, FileSystem srcFileSystem,
      FileSystem tgtFileSystem) throws IOException {
    ContentSummary srcContentSummary = srcFileSystem.getContentSummary(srcPath);
    ContentSummary tgtContentSummary = tgtFileSystem.getContentSummary(trgPath);
    if (srcContentSummary.getLength() != tgtContentSummary.getLength()) {
      System.err.println("Directory Size mismatch in source directory " + srcPath + " and target directory " + trgPath);
      return false;
    }
    if (srcContentSummary.getDirectoryCount() != tgtContentSummary.getDirectoryCount()) {
      System.err
          .println("Directory Count mismatch in source directory " + srcPath + " and target directory " + trgPath);
      return false;
    }
    LOG.debug("Directory count matched for {} and {}", srcPath, trgPath);
    if (srcContentSummary.getFileCount() != tgtContentSummary.getFileCount()) {
      System.err.println("File Count mismatch in source directory " + srcPath + " and target directory " + trgPath);
      return false;
    }
    LOG.debug("File count matched for {} and {}", srcPath, trgPath);
    return true;
  }

  public boolean validateAtFileLevel(Path srcDir, Path tgtDir, FileSystem srcFs, FileSystem tgtFs,
      List<Pattern> filtersPattern, boolean verifyChecksum, ThreadPoolExecutor threadPoolChecksum) throws Exception {
    // Get recursive list of files for the source directory.
    RemoteIterator<LocatedFileStatus> srcListing = srcFs.listFiles(srcDir, true);
    // Get recursive list of files for the target directory.
    RemoteIterator<LocatedFileStatus> tgtListing = tgtFs.listFiles(tgtDir, true);

    ArrayList<Future<Boolean>> futures = new ArrayList();
    boolean response = true;
    // Compare one by one the source files and the target file, the list should be sorted, so if there isn't any
    // entry missing or mismatch(error case), everything should match in sequence.
    while (srcListing.hasNext() && tgtListing.hasNext()) {
      LocatedFileStatus sourceFile = srcListing.next();

      // Check if the entry is a filtered entry, if the entry is a filtered entry we can skip comparing and proceed
      // further, since that entry won't be there in the target, considering the same filter would be passed during
      // replication.
      if (filtersPattern != null && !isCopied(sourceFile.getPath(), filtersPattern)) {
        LOG.info("Entry: {} is filtered.", sourceFile.getPath());
        continue;
      }
      LocatedFileStatus tgtFile = tgtListing.next();
      LOG.info("Comparing {} and {}", sourceFile.getPath(), tgtFile.getPath());
      String sourcePath = Path.getPathWithoutSchemeAndAuthority(sourceFile.getPath()).toString()
          .replaceFirst(Path.getPathWithoutSchemeAndAuthority(srcDir).toString(), "/");
      String targetPath = Path.getPathWithoutSchemeAndAuthority(tgtFile.getPath()).toString()
          .replaceFirst(Path.getPathWithoutSchemeAndAuthority(tgtDir).toString(), "/");
      if (!sourcePath.equals(targetPath)) {
        System.err.println(
            "Entries mismatch between source: " + srcDir + " and target: " + tgtDir + " for " + "sourceFile: "
                + sourceFile.getPath() + " at target with " + tgtFile.getPath() + " Mismatched subPaths: " + "source: "
                + sourcePath + " and target: " + targetPath + "Either Source Or Target has an extra/less files.");
        // Get the response from all the checksum tasks scheduled, in order to prevent the client being terminating
        // abruptly.
        validateChecksumStatus(srcDir, tgtDir, futures);
        // Entries have mismatched, the dir needs to copied again, return false, and print the source dir, so it can
        // be copied entirely.
        return false;
      }
      if (sourceFile.getLen() != tgtFile.getLen()) {
        System.err.println("File Size mismatch in source directory " + srcDir + " and target directory " + tgtDir + " "
            + "for source: " + sourceFile.getPath() + " and target " + tgtFile.getPath());
        // Try further if only some files have been modified, trying out if there are no addition/deletion of files in
        // directory on moving ahead.
        response = false;
      }
      LOG.debug("Length matched for {} and {}", sourceFile.getPath(), tgtFile.getPath());
      // Verify checksum of the file.
      if (verifyChecksum) {
        ChecksumVerifier checksumVerifier =
            new ChecksumVerifier(sourceFile.getPath(), tgtFile.getPath(), srcFs, tgtFs, srcDir, tgtDir);
        Future<Boolean> future = threadPoolChecksum.submit(checksumVerifier);
        futures.add(future);
      }
    }
    // Check if entries are left in the target.
    if (tgtListing.hasNext()) {
      validateChecksumStatus(srcDir, tgtDir, futures);
      System.err.println("Extra entry at target: " + tgtListing.next().getPath());
      return false;
    }
    LOG.debug("No target entries remaining for {} and {}", srcDir, tgtDir);

    if (validateChecksumStatus(srcDir, tgtDir, futures)) {
      return false;
    }
    // Check if entries are present in the source.
    while (srcListing.hasNext()) {
      LocatedFileStatus sourceFile = srcListing.next();
      if (filtersPattern != null && !isCopied(sourceFile.getPath(), filtersPattern)) {
        LOG.info("Entry: {} is filtered.", sourceFile.getPath());
        continue;
      } else {
        System.err.println("Extra entry at source: " + sourceFile.getPath());
        return false;
      }
    }
    LOG.debug("No source entries remaining for {} and {}", srcDir, tgtDir);
    return response;
  }

  // Gets the status of all the scheduled checksum tasks, and returns true if all are success.
  private boolean validateChecksumStatus(Path srcDir, Path tgtDir, ArrayList<Future<Boolean>> futures)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    int numFailed = 0;
    int numSuccess = 0;
    for (Future<Boolean> f : futures) {
      if (!f.get()) {
        numFailed++;
      } else {
        numSuccess++;
      }
    }

    if (numFailed > 0) {
      LOG.warn("{} files failed checksum validation for source: {} and target: {}, numSuccess: {} numFailed: {}",
          srcDir, tgtDir, numSuccess, numFailed);
      return true;
    }
    return false;
  }

  // Validates whether the path is filtered or not.
  private boolean isCopied(Path path, List<Pattern> filters) {
    for (Pattern filter : filters) {
      if (filter.matcher(path.toString()).matches()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Gets the path of the external table info file, allows the user to directly specify the path and tolerates if he
   * provides the dump path with /hive suffix or not to counter the difference between the dump path output in
   * metrics vs out of repl dump command which doesn't have /hive as suffix.
   * @param dumpPath
   * @return
   */
  private Path getExternalTableFileListPath(String dumpPath) {
    if (dumpPath.endsWith("/_file_list_external")) {
      return new Path(dumpPath);
    } else if (dumpPath.endsWith("/hive")) {
      return new Path(dumpPath, "_file_list_external");
    } else {
      return new Path(dumpPath, "hive/_file_list_external");
    }
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public static void main(String args[]) throws Exception {
    Configuration conf = new Configuration();
    ReplicationMigrationTool inst = new ReplicationMigrationTool();
    inst.setConf(conf);
    int result = ToolRunner.run(inst, args);
    System.exit(result);
  }

  /**
   * Validates the content of the directory.
   */
  private class DirectoryProcessor implements Callable<Boolean> {

    String line;
    boolean checkOpenFiles;
    boolean isValidateChecksum;
    boolean checkAtDirLevel;
    boolean checkAtFileLevel;
    List<Pattern> filtersPattern;
    ThreadPoolExecutor threadPoolChecksum;

    DirectoryProcessor(String line, boolean checkAtDirLevel, boolean checkAtFileLevel, boolean checkOpenFiles,
        boolean isValidateChecksum, List<Pattern> filtersPattern, ThreadPoolExecutor threadPoolChecksum) {
      this.line = line;
      this.checkAtDirLevel = checkAtDirLevel;
      this.checkAtFileLevel = checkAtFileLevel;
      this.checkOpenFiles = checkOpenFiles;
      this.isValidateChecksum = isValidateChecksum;
      this.filtersPattern = filtersPattern;
      this.threadPoolChecksum = threadPoolChecksum;
    }

    @Override
    public Boolean call() throws Exception {

      String[] lineComponents = line.split("#");
      Path srcPath = new Path(lineComponents[0]);
      Path trgPath = new Path(lineComponents[1]);
      try {
        FileSystem srcFileSystem = srcPath.getFileSystem(conf);
        FileSystem tgtFileSystem = trgPath.getFileSystem(conf);
        // Check if there are any open file is the source cluster.
        if (checkOpenFiles) {
          LOG.debug("Validating {} and {} for open files at source", srcPath, trgPath);
          if (!validateOpenFilesAtSource(srcPath, srcFileSystem)) {
            return false;
          }
        }
        // Verify dir count, file count & length.
        if (checkAtDirLevel) {
          LOG.debug("Validating {} and {} at directory level", srcPath, trgPath);
          if (!validateAtDirectoryLevel(srcPath, trgPath, srcFileSystem, tgtFileSystem)) {
            return false;
          }
        }
        if (checkAtFileLevel) {
          LOG.debug("Validating {} and {} at file level", srcPath, trgPath);
          if (!validateAtFileLevel(srcPath, trgPath, srcFileSystem, tgtFileSystem, filtersPattern, isValidateChecksum,
              threadPoolChecksum)) {
            return false;
          }
        }
        LOG.info(srcPath + " and " + trgPath + " are in Sync");
      } catch (Exception e) {
        LOG.error("Failed to verify source: {} with target: {}", e);
        System.err
            .println("Failed to verify source: " + srcPath + " with target: " + trgPath + " error:" + e.getMessage());
        return false;
      }
      return true;
    }
  }

  /**
   * Utility class to verify checksums from source cluster & target cluster.
   */
  private class ChecksumVerifier implements Callable<Boolean> {

    private final Path srcDir;
    private final Path tgtDir;
    private Path sourcePath;
    private Path targetPath;
    private FileSystem sourceFs;
    private FileSystem targetFs;

    ChecksumVerifier(Path sourcePath, Path targetPath, FileSystem sourceFs, FileSystem targetFs, Path srcDir,
        Path tgtDir) {
      this.sourcePath = sourcePath;
      this.targetPath = targetPath;
      this.sourceFs = sourceFs;
      this.targetFs = targetFs;
      this.srcDir = srcDir;
      this.tgtDir = tgtDir;
    }

    @Override
    public Boolean call() throws Exception {
      LOG.debug("Verifying checksum for source: {} and target: {}", sourcePath, targetPath);
      try {
        boolean response = sourceFs.getFileChecksum(sourcePath).equals(targetFs.getFileChecksum(targetPath));
        if (!response) {
          System.err.println(
              "File Checksum mismatch in source directory " + srcDir + " and target directory " + tgtDir + " "
                  + "for source: " + sourcePath + " and target " + targetPath);
        }

        return response;
      } catch (Exception e) {
        System.out
            .println("Failed Checksum for: " + sourcePath + " and target: " + targetPath + " reason:" + e.getMessage());
        LOG.error("Failed Checksum for: {} and target: {}", sourcePath, targetPath, e);
        return false;
      }
    }
  }
}

