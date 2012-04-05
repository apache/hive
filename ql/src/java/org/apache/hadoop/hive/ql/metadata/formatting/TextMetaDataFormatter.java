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

package org.apache.hadoop.hive.ql.metadata.formatting;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.shims.ShimLoader;

/**
 * Format table and index information for human readability using
 * simple lines of text.
 */
public class TextMetaDataFormatter implements MetaDataFormatter {
    private static final Log LOG = LogFactory.getLog("hive.ql.exec.DDLTask");

    private static final int separator = Utilities.tabCode;
    private static final int terminator = Utilities.newLineCode;

    /**
     * Write an error message.
     */
    public void error(OutputStream out, String msg, int errorCode)
        throws HiveException
    {
        try {
            out.write(msg.getBytes("UTF-8"));
            out.write(terminator);
        } catch (Exception e) {
            throw new HiveException(e);
        }
    }

    /**
     * Write a log warn message.
     */
    public void logWarn(OutputStream out, String msg, int errorCode)
        throws HiveException
    {
        LOG.warn(msg);
    }

    /**
     * Write a log info message.
     */
    public void logInfo(OutputStream out, String msg, int errorCode)
        throws HiveException
    {
        LOG.info(msg);
    }

    /**
     * Write a console error message.
     */
    public void consoleError(LogHelper console, String msg, int errorCode) {
        console.printError(msg);
    }

    /**
     * Write a console error message.
     */
    public void consoleError(LogHelper console, String msg, String detail,
                             int errorCode)
    {
        console.printError(msg, detail);
    }

    /**
     * Show a list of tables.
     */
    public void showTables(DataOutputStream out, Set<String> tables)
        throws HiveException
    {
        Iterator<String> iterTbls = tables.iterator();

        try {
            while (iterTbls.hasNext()) {
                // create a row per table name
                out.writeBytes(iterTbls.next());
                out.write(terminator);
            }
        } catch (IOException e) {
           throw new HiveException(e);
        }
    }

    public void describeTable(DataOutputStream outStream,
                              String colPath, String tableName,
                              Table tbl, Partition part, List<FieldSchema> cols,
                              boolean isFormatted, boolean isExt)
         throws HiveException
   {
       try {
         if (colPath.equals(tableName)) {
           if (!isFormatted) {
             outStream.writeBytes(MetaDataFormatUtils.displayColsUnformatted(cols));
           } else {
             outStream.writeBytes(
               MetaDataFormatUtils.getAllColumnsInformation(cols,
                 tbl.isPartitioned() ? tbl.getPartCols() : null));
           }
         } else {
           if (isFormatted) {
             outStream.writeBytes(MetaDataFormatUtils.getAllColumnsInformation(cols));
           } else {
             outStream.writeBytes(MetaDataFormatUtils.displayColsUnformatted(cols));
           }
         }

         if (tableName.equals(colPath)) {

           if (isFormatted) {
             if (part != null) {
               outStream.writeBytes(MetaDataFormatUtils.getPartitionInformation(part));
             } else {
               outStream.writeBytes(MetaDataFormatUtils.getTableInformation(tbl));
             }
           }

           // if extended desc table then show the complete details of the table
           if (isExt) {
             // add empty line
             outStream.write(terminator);
             if (part != null) {
               // show partition information
               outStream.writeBytes("Detailed Partition Information");
               outStream.write(separator);
               outStream.writeBytes(part.getTPartition().toString());
               outStream.write(separator);
               // comment column is empty
               outStream.write(terminator);
             } else {
               // show table information
               outStream.writeBytes("Detailed Table Information");
               outStream.write(separator);
               outStream.writeBytes(tbl.getTTable().toString());
               outStream.write(separator);
               outStream.write(terminator);
             }
           }
         }
       } catch (IOException e) {
           throw new HiveException(e);
       }
    }

    public void showTableStatus(DataOutputStream outStream,
                                Hive db,
                                HiveConf conf,
                                List<Table> tbls,
                                Map<String, String> part,
                                Partition par)
        throws HiveException
    {
        try {
            Iterator<Table> iterTables = tbls.iterator();
            while (iterTables.hasNext()) {
              // create a row per table name
              Table tbl = iterTables.next();
              String tableName = tbl.getTableName();
              String tblLoc = null;
              String inputFormattCls = null;
              String outputFormattCls = null;
              if (part != null) {
                if (par != null) {
                  if (par.getLocation() != null) {
                    tblLoc = par.getDataLocation().toString();
                  }
                  inputFormattCls = par.getInputFormatClass().getName();
                  outputFormattCls = par.getOutputFormatClass().getName();
                }
              } else {
                if (tbl.getPath() != null) {
                  tblLoc = tbl.getDataLocation().toString();
                }
                inputFormattCls = tbl.getInputFormatClass().getName();
                outputFormattCls = tbl.getOutputFormatClass().getName();
              }

              String owner = tbl.getOwner();
              List<FieldSchema> cols = tbl.getCols();
              String ddlCols = MetaStoreUtils.getDDLFromFieldSchema("columns", cols);
              boolean isPartitioned = tbl.isPartitioned();
              String partitionCols = "";
              if (isPartitioned) {
                partitionCols = MetaStoreUtils.getDDLFromFieldSchema(
                    "partition_columns", tbl.getPartCols());
              }

              outStream.writeBytes("tableName:" + tableName);
              outStream.write(terminator);
              outStream.writeBytes("owner:" + owner);
              outStream.write(terminator);
              outStream.writeBytes("location:" + tblLoc);
              outStream.write(terminator);
              outStream.writeBytes("inputformat:" + inputFormattCls);
              outStream.write(terminator);
              outStream.writeBytes("outputformat:" + outputFormattCls);
              outStream.write(terminator);
              outStream.writeBytes("columns:" + ddlCols);
              outStream.write(terminator);
              outStream.writeBytes("partitioned:" + isPartitioned);
              outStream.write(terminator);
              outStream.writeBytes("partitionColumns:" + partitionCols);
              outStream.write(terminator);
              // output file system information
              Path tblPath = tbl.getPath();
              List<Path> locations = new ArrayList<Path>();
              if (isPartitioned) {
                if (par == null) {
                  for (Partition curPart : db.getPartitions(tbl)) {
                    if (curPart.getLocation() != null) {
                      locations.add(new Path(curPart.getLocation()));
                    }
                  }
                } else {
                  if (par.getLocation() != null) {
                    locations.add(new Path(par.getLocation()));
                  }
                }
              } else {
                if (tblPath != null) {
                  locations.add(tblPath);
                }
              }
              if (!locations.isEmpty()) {
                writeFileSystemStats(outStream, conf, locations, tblPath, false, 0);
              }

              outStream.write(terminator);
            }
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    private void writeFileSystemStats(DataOutputStream outStream,
                                      HiveConf conf,
                                      List<Path> locations,
                                      Path tblPath, boolean partSpecified, int indent)
        throws IOException
    {
      long totalFileSize = 0;
      long maxFileSize = 0;
      long minFileSize = Long.MAX_VALUE;
      long lastAccessTime = 0;
      long lastUpdateTime = 0;
      int numOfFiles = 0;

      boolean unknown = false;
      FileSystem fs = tblPath.getFileSystem(conf);
      // in case all files in locations do not exist
      try {
        FileStatus tmpStatus = fs.getFileStatus(tblPath);
        lastAccessTime = ShimLoader.getHadoopShims().getAccessTime(tmpStatus);
        lastUpdateTime = tmpStatus.getModificationTime();
        if (partSpecified) {
          // check whether the part exists or not in fs
          tmpStatus = fs.getFileStatus(locations.get(0));
        }
      } catch (IOException e) {
        LOG.warn(
            "Cannot access File System. File System status will be unknown: ", e);
        unknown = true;
      }

      if (!unknown) {
        for (Path loc : locations) {
          try {
            FileStatus status = fs.getFileStatus(tblPath);
            FileStatus[] files = fs.listStatus(loc);
            long accessTime = ShimLoader.getHadoopShims().getAccessTime(status);
            long updateTime = status.getModificationTime();
            // no matter loc is the table location or part location, it must be a
            // directory.
            if (!status.isDir()) {
              continue;
            }
            if (accessTime > lastAccessTime) {
              lastAccessTime = accessTime;
            }
            if (updateTime > lastUpdateTime) {
              lastUpdateTime = updateTime;
            }
            for (FileStatus currentStatus : files) {
              if (currentStatus.isDir()) {
                continue;
              }
              numOfFiles++;
              long fileLen = currentStatus.getLen();
              totalFileSize += fileLen;
              if (fileLen > maxFileSize) {
                maxFileSize = fileLen;
              }
              if (fileLen < minFileSize) {
                minFileSize = fileLen;
              }
              accessTime = ShimLoader.getHadoopShims().getAccessTime(
                  currentStatus);
              updateTime = currentStatus.getModificationTime();
              if (accessTime > lastAccessTime) {
                lastAccessTime = accessTime;
              }
              if (updateTime > lastUpdateTime) {
                lastUpdateTime = updateTime;
              }
            }
          } catch (IOException e) {
            // ignore
          }
        }
      }
      String unknownString = "unknown";

      for (int k = 0; k < indent; k++) {
        outStream.writeBytes(Utilities.INDENT);
      }
      outStream.writeBytes("totalNumberFiles:");
      outStream.writeBytes(unknown ? unknownString : "" + numOfFiles);
      outStream.write(terminator);

      for (int k = 0; k < indent; k++) {
        outStream.writeBytes(Utilities.INDENT);
      }
      outStream.writeBytes("totalFileSize:");
      outStream.writeBytes(unknown ? unknownString : "" + totalFileSize);
      outStream.write(terminator);

      for (int k = 0; k < indent; k++) {
        outStream.writeBytes(Utilities.INDENT);
      }
      outStream.writeBytes("maxFileSize:");
      outStream.writeBytes(unknown ? unknownString : "" + maxFileSize);
      outStream.write(terminator);

      for (int k = 0; k < indent; k++) {
        outStream.writeBytes(Utilities.INDENT);
      }
      outStream.writeBytes("minFileSize:");
      if (numOfFiles > 0) {
        outStream.writeBytes(unknown ? unknownString : "" + minFileSize);
      } else {
        outStream.writeBytes(unknown ? unknownString : "" + 0);
      }
      outStream.write(terminator);

      for (int k = 0; k < indent; k++) {
        outStream.writeBytes(Utilities.INDENT);
      }
      outStream.writeBytes("lastAccessTime:");
      outStream.writeBytes((unknown || lastAccessTime < 0) ? unknownString : ""
          + lastAccessTime);
      outStream.write(terminator);

      for (int k = 0; k < indent; k++) {
        outStream.writeBytes(Utilities.INDENT);
      }
      outStream.writeBytes("lastUpdateTime:");
      outStream.writeBytes(unknown ? unknownString : "" + lastUpdateTime);
      outStream.write(terminator);
  }

    /**
     * Show the table partitions.
     */
    public void showTablePartitons(DataOutputStream outStream, List<String> parts)
        throws HiveException
    {
        try {
            for (String part : parts) {
                outStream.writeBytes(part);
                outStream.write(terminator);
            }
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    /**
     * Show the list of databases
     */
    public void showDatabases(DataOutputStream outStream, List<String> databases)
        throws HiveException
        {
        try {
            for (String database : databases) {
                // create a row per database name
                outStream.writeBytes(database);
                outStream.write(terminator);
              }
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    /**
     * Describe a database
     */
    public void showDatabaseDescription(DataOutputStream outStream,
                                        String database,
                                        String comment,
                                        String location,
                                        Map<String, String> params)
        throws HiveException
    {
        try {
            outStream.writeBytes(database);
            outStream.write(separator);
            if (comment != null)
                outStream.writeBytes(comment);
            outStream.write(separator);
            if (location != null)
                outStream.writeBytes(location);
            outStream.write(separator);
            if (params != null && !params.isEmpty()) {
                outStream.writeBytes(params.toString());
            }
            outStream.write(terminator);
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }
}
