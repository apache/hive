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

package org.apache.hadoop.hive.ql.exec;

import static org.apache.commons.lang.StringUtils.join;
import static org.apache.hadoop.util.StringUtils.stringifyException;

import java.io.BufferedWriter;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.CheckResult;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreChecker;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.CreateTableLikeDesc;
import org.apache.hadoop.hive.ql.plan.CreateViewDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DescFunctionDesc;
import org.apache.hadoop.hive.ql.plan.DescTableDesc;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;
import org.apache.hadoop.hive.ql.plan.MsckDesc;
import org.apache.hadoop.hive.ql.plan.ShowFunctionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowTableStatusDesc;
import org.apache.hadoop.hive.ql.plan.ShowTablesDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.shims.ShimLoader;

/**
 * DDLTask implementation.
 *
 **/
public class DDLTask extends Task<DDLWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog("hive.ql.exec.DDLTask");

  transient HiveConf conf;
  private static final int separator = Utilities.tabCode;
  private static final int terminator = Utilities.newLineCode;

  public DDLTask() {
    super();
  }

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext ctx) {
    super.initialize(conf, queryPlan, ctx);
    this.conf = conf;
  }

  @Override
  public int execute() {

    // Create the db
    Hive db;
    try {
      db = Hive.get(conf);

      CreateTableDesc crtTbl = work.getCreateTblDesc();
      if (crtTbl != null) {
        return createTable(db, crtTbl);
      }

      CreateTableLikeDesc crtTblLike = work.getCreateTblLikeDesc();
      if (crtTblLike != null) {
        return createTableLike(db, crtTblLike);
      }

      DropTableDesc dropTbl = work.getDropTblDesc();
      if (dropTbl != null) {
        return dropTable(db, dropTbl);
      }

      AlterTableDesc alterTbl = work.getAlterTblDesc();
      if (alterTbl != null) {
        return alterTable(db, alterTbl);
      }

      CreateViewDesc crtView = work.getCreateViewDesc();
      if (crtView != null) {
        return createView(db, crtView);
      }

      AddPartitionDesc addPartitionDesc = work.getAddPartitionDesc();
      if (addPartitionDesc != null) {
        return addPartition(db, addPartitionDesc);
      }

      MsckDesc msckDesc = work.getMsckDesc();
      if (msckDesc != null) {
        return msck(db, msckDesc);
      }

      DescTableDesc descTbl = work.getDescTblDesc();
      if (descTbl != null) {
        return describeTable(db, descTbl);
      }

      DescFunctionDesc descFunc = work.getDescFunctionDesc();
      if (descFunc != null) {
        return describeFunction(descFunc);
      }

      ShowTablesDesc showTbls = work.getShowTblsDesc();
      if (showTbls != null) {
        return showTables(db, showTbls);
      }

      ShowTableStatusDesc showTblStatus = work.getShowTblStatusDesc();
      if (showTblStatus != null) {
        return showTableStatus(db, showTblStatus);
      }

      ShowFunctionsDesc showFuncs = work.getShowFuncsDesc();
      if (showFuncs != null) {
        return showFunctions(showFuncs);
      }

      ShowPartitionsDesc showParts = work.getShowPartsDesc();
      if (showParts != null) {
        return showPartitions(db, showParts);
      }

    } catch (InvalidTableException e) {
      console.printError("Table " + e.getTableName() + " does not exist");
      LOG.debug(stringifyException(e));
      return 1;
    } catch (HiveException e) {
      console.printError("FAILED: Error in metadata: " + e.getMessage(), "\n"
          + stringifyException(e));
      LOG.debug(stringifyException(e));
      return 1;
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + stringifyException(e));
      return (1);
    }
    assert false;
    return 0;
  }

  /**
   * Add a partition to a table.
   *
   * @param db
   *          Database to add the partition to.
   * @param addPartitionDesc
   *          Add this partition.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   */
  private int addPartition(Hive db, AddPartitionDesc addPartitionDesc) throws HiveException {

    Table tbl = db.getTable(addPartitionDesc.getDbName(), addPartitionDesc
        .getTableName());

    validateAlterTableType(tbl, AlterTableDesc.AlterTableTypes.ADDPARTITION);

    // If the add partition was created with IF NOT EXISTS, then we should
    // not throw an error if the specified part does exist.
    Partition checkPart = db.getPartition(tbl, addPartitionDesc.getPartSpec(), false);
    if (checkPart != null && addPartitionDesc.getIfNotExists()) {
      return 0;
    }

    if (addPartitionDesc.getLocation() == null) {
      db.createPartition(tbl, addPartitionDesc.getPartSpec());
    } else {
      // set partition path relative to table
      db.createPartition(tbl, addPartitionDesc.getPartSpec(), new Path(tbl
          .getPath(), addPartitionDesc.getLocation()));
    }

    Partition part = db
        .getPartition(tbl, addPartitionDesc.getPartSpec(), false);
    work.getOutputs().add(new WriteEntity(part));

    return 0;
  }

  private void validateAlterTableType(
    Table tbl, AlterTableDesc.AlterTableTypes alterType)  throws HiveException {

    if (tbl.isView()) {
      switch (alterType) {
      case ADDPROPS:
        // allow this form
        break;
      default:
        throw new HiveException(
          "Cannot use this form of ALTER TABLE on a view");
      }
    }

    if (tbl.isNonNative()) {
      throw new HiveException("Cannot use ALTER TABLE on a non-native table");
    }
  }

  /**
   * MetastoreCheck, see if the data in the metastore matches what is on the
   * dfs. Current version checks for tables and partitions that are either
   * missing on disk on in the metastore.
   *
   * @param db
   *          The database in question.
   * @param msckDesc
   *          Information about the tables and partitions we want to check for.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   */
  private int msck(Hive db, MsckDesc msckDesc) {
    CheckResult result = new CheckResult();
    List<String> repairOutput = new ArrayList<String>();
    try {
      HiveMetaStoreChecker checker = new HiveMetaStoreChecker(db);
      checker.checkMetastore(MetaStoreUtils.DEFAULT_DATABASE_NAME, msckDesc
          .getTableName(), msckDesc.getPartSpecs(), result);
      if (msckDesc.isRepairPartitions()) {
        Table table = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME,
            msckDesc.getTableName());
        for (CheckResult.PartitionResult part : result.getPartitionsNotInMs()) {
          try {
            db.createPartition(table, Warehouse.makeSpecFromName(part
                .getPartitionName()));
            repairOutput.add("Repair: Added partition to metastore "
                + msckDesc.getTableName() + ':' + part.getPartitionName());
          } catch (Exception e) {
            LOG.warn("Repair error, could not add partition to metastore: ", e);
          }
        }
      }
    } catch (HiveException e) {
      LOG.warn("Failed to run metacheck: ", e);
      return 1;
    } catch (IOException e) {
      LOG.warn("Failed to run metacheck: ", e);
      return 1;
    } finally {
      BufferedWriter resultOut = null;
      try {
        Path resFile = new Path(msckDesc.getResFile());
        FileSystem fs = resFile.getFileSystem(conf);
        resultOut = new BufferedWriter(new OutputStreamWriter(fs
            .create(resFile)));

        boolean firstWritten = false;
        firstWritten |= writeMsckResult(result.getTablesNotInMs(),
            "Tables not in metastore:", resultOut, firstWritten);
        firstWritten |= writeMsckResult(result.getTablesNotOnFs(),
            "Tables missing on filesystem:", resultOut, firstWritten);
        firstWritten |= writeMsckResult(result.getPartitionsNotInMs(),
            "Partitions not in metastore:", resultOut, firstWritten);
        firstWritten |= writeMsckResult(result.getPartitionsNotOnFs(),
            "Partitions missing from filesystem:", resultOut, firstWritten);
        for (String rout : repairOutput) {
          if (firstWritten) {
            resultOut.write(terminator);
          } else {
            firstWritten = true;
          }
          resultOut.write(rout);
        }
      } catch (IOException e) {
        LOG.warn("Failed to save metacheck output: ", e);
        return 1;
      } finally {
        if (resultOut != null) {
          try {
            resultOut.close();
          } catch (IOException e) {
            LOG.warn("Failed to close output file: ", e);
            return 1;
          }
        }
      }
    }

    return 0;
  }

  /**
   * Write the result of msck to a writer.
   *
   * @param result
   *          The result we're going to write
   * @param msg
   *          Message to write.
   * @param out
   *          Writer to write to
   * @param wrote
   *          if any previous call wrote data
   * @return true if something was written
   * @throws IOException
   *           In case the writing fails
   */
  private boolean writeMsckResult(List<? extends Object> result, String msg,
      Writer out, boolean wrote) throws IOException {

    if (!result.isEmpty()) {
      if (wrote) {
        out.write(terminator);
      }

      out.write(msg);
      for (Object entry : result) {
        out.write(separator);
        out.write(entry.toString());
      }
      return true;
    }

    return false;
  }

  /**
   * Write a list of partitions to a file.
   *
   * @param db
   *          The database in question.
   * @param showParts
   *          These are the partitions we're interested in.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int showPartitions(Hive db, ShowPartitionsDesc showParts) throws HiveException {
    // get the partitions for the table and populate the output
    String tabName = showParts.getTabName();
    Table tbl = null;
    List<String> parts = null;

    tbl = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tabName);

    if (!tbl.isPartitioned()) {
      console.printError("Table " + tabName + " is not a partitioned table");
      return 1;
    }
    if (showParts.getPartSpec() != null) {
      parts = db.getPartitionNames(MetaStoreUtils.DEFAULT_DATABASE_NAME,
          tbl.getTableName(), showParts.getPartSpec(), (short) -1);
    } else {
      parts = db.getPartitionNames(MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl
          .getTableName(), (short) -1);
    }

    // write the results in the file
    try {
      Path resFile = new Path(showParts.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      DataOutput outStream = fs.create(resFile);
      Iterator<String> iterParts = parts.iterator();

      while (iterParts.hasNext()) {
        // create a row per partition name
        outStream.writeBytes(iterParts.next());
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.info("show partitions: " + stringifyException(e));
      throw new HiveException(e.toString());
    } catch (IOException e) {
      LOG.info("show partitions: " + stringifyException(e));
      throw new HiveException(e.toString());
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }

    return 0;
  }

  /**
   * Write a list of the tables in the database to a file.
   *
   * @param db
   *          The database in question.
   * @param showTbls
   *          These are the tables we're interested in.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int showTables(Hive db, ShowTablesDesc showTbls) throws HiveException {
    // get the tables for the desired pattenn - populate the output stream
    List<String> tbls = null;
    if (showTbls.getPattern() != null) {
      LOG.info("pattern: " + showTbls.getPattern());
      tbls = db.getTablesByPattern(showTbls.getPattern());
      LOG.info("results : " + tbls.size());
    } else {
      tbls = db.getAllTables();
    }

    // write the results in the file
    try {
      Path resFile = new Path(showTbls.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      DataOutput outStream = fs.create(resFile);
      SortedSet<String> sortedTbls = new TreeSet<String>(tbls);
      Iterator<String> iterTbls = sortedTbls.iterator();

      while (iterTbls.hasNext()) {
        // create a row per table name
        outStream.writeBytes(iterTbls.next());
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("show table: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show table: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    return 0;
  }

  /**
   * Write a list of the user defined functions to a file.
   *
   * @param showFuncs
   *          are the functions we're interested in.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int showFunctions(ShowFunctionsDesc showFuncs) throws HiveException {
    // get the tables for the desired pattenn - populate the output stream
    Set<String> funcs = null;
    if (showFuncs.getPattern() != null) {
      LOG.info("pattern: " + showFuncs.getPattern());
      funcs = FunctionRegistry.getFunctionNames(showFuncs.getPattern());
      LOG.info("results : " + funcs.size());
    } else {
      funcs = FunctionRegistry.getFunctionNames();
    }

    // write the results in the file
    try {
      Path resFile = new Path(showFuncs.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      DataOutput outStream = fs.create(resFile);
      SortedSet<String> sortedFuncs = new TreeSet<String>(funcs);
      Iterator<String> iterFuncs = sortedFuncs.iterator();

      while (iterFuncs.hasNext()) {
        // create a row per table name
        outStream.writeBytes(iterFuncs.next());
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("show function: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show function: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    return 0;
  }

  /**
   * Shows a description of a function.
   *
   * @param descFunc
   *          is the function we are describing
   * @throws HiveException
   */
  private int describeFunction(DescFunctionDesc descFunc) throws HiveException {
    String funcName = descFunc.getName();

    // write the results in the file
    try {
      Path resFile = new Path(descFunc.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      DataOutput outStream = fs.create(resFile);

      // get the function documentation
      Description desc = null;
      Class<?> funcClass = null;
      FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(funcName);
      if (functionInfo != null) {
        funcClass = functionInfo.getFunctionClass();
      }
      if (funcClass != null) {
        desc = funcClass.getAnnotation(Description.class);
      }
      if (desc != null) {
        outStream.writeBytes(desc.value().replace("_FUNC_", funcName));
        if (descFunc.isExtended()) {
          Set<String> synonyms = FunctionRegistry.getFunctionSynonyms(funcName);
          if (synonyms.size() > 0) {
            outStream.writeBytes("\nSynonyms: " + join(synonyms, ", "));
          }
          if (desc.extended().length() > 0) {
            outStream.writeBytes("\n"
                + desc.extended().replace("_FUNC_", funcName));
          }
        }
      } else {
        if (funcClass != null) {
          outStream.writeBytes("There is no documentation for function '"
              + funcName + "'");
        } else {
          outStream.writeBytes("Function '" + funcName + "' does not exist.");
        }
      }

      outStream.write(terminator);

      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("describe function: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("describe function: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    return 0;
  }

  /**
   * Write the status of tables to a file.
   *
   * @param db
   *          The database in question.
   * @param showTblStatus
   *          tables we are interested in
   * @return Return 0 when execution succeeds and above 0 if it fails.
   */
  private int showTableStatus(Hive db, ShowTableStatusDesc showTblStatus) throws HiveException {
    // get the tables for the desired pattenn - populate the output stream
    List<Table> tbls = new ArrayList<Table>();
    Map<String, String> part = showTblStatus.getPartSpec();
    Partition par = null;
    if (part != null) {
      Table tbl = db.getTable(showTblStatus.getDbName(), showTblStatus
          .getPattern());
      par = db.getPartition(tbl, part, false);
      if (par == null) {
        throw new HiveException("Partition " + part + " for table "
            + showTblStatus.getPattern() + " does not exist.");
      }
      tbls.add(tbl);
    } else {
      LOG.info("pattern: " + showTblStatus.getPattern());
      List<String> tblStr = db.getTablesForDb(showTblStatus.getDbName(),
          showTblStatus.getPattern());
      SortedSet<String> sortedTbls = new TreeSet<String>(tblStr);
      Iterator<String> iterTbls = sortedTbls.iterator();
      while (iterTbls.hasNext()) {
        // create a row per table name
        String tblName = iterTbls.next();
        Table tbl = db.getTable(showTblStatus.getDbName(), tblName);
        tbls.add(tbl);
      }
      LOG.info("results : " + tblStr.size());
    }

    // write the results in the file
    try {
      Path resFile = new Path(showTblStatus.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      DataOutput outStream = fs.create(resFile);

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
            tblLoc = par.getDataLocation().toString();
            inputFormattCls = par.getInputFormatClass().getName();
            outputFormattCls = par.getOutputFormatClass().getName();
          }
        } else {
          tblLoc = tbl.getDataLocation().toString();
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
        Path tablLoc = tbl.getPath();
        List<Path> locations = new ArrayList<Path>();
        if (isPartitioned) {
          if (par == null) {
            for (Partition curPart : db.getPartitions(tbl)) {
              locations.add(new Path(curPart.getTPartition().getSd()
                  .getLocation()));
            }
          } else {
            locations.add(new Path(par.getTPartition().getSd().getLocation()));
          }
        } else {
          locations.add(tablLoc);
        }
        writeFileSystemStats(outStream, locations, tablLoc, false, 0);

        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.info("show table status: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.info("show table status: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e);
    }
    return 0;
  }

  /**
   * Write the description of a table to a file.
   *
   * @param db
   *          The database in question.
   * @param descTbl
   *          This is the table we're interested in.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int describeTable(Hive db, DescTableDesc descTbl) throws HiveException {
    String colPath = descTbl.getTableName();
    String tableName = colPath.substring(0,
        colPath.indexOf('.') == -1 ? colPath.length() : colPath.indexOf('.'));

    // describe the table - populate the output stream
    Table tbl = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName,
        false);
    Partition part = null;
    try {
      Path resFile = new Path(descTbl.getResFile());
      if (tbl == null) {
        FileSystem fs = resFile.getFileSystem(conf);
        DataOutput outStream = (DataOutput) fs.open(resFile);
        String errMsg = "Table " + tableName + " does not exist";
        outStream.write(errMsg.getBytes("UTF-8"));
        ((FSDataOutputStream) outStream).close();
        return 0;
      }
      if (descTbl.getPartSpec() != null) {
        part = db.getPartition(tbl, descTbl.getPartSpec(), false);
        if (part == null) {
          FileSystem fs = resFile.getFileSystem(conf);
          DataOutput outStream = (DataOutput) fs.open(resFile);
          String errMsg = "Partition " + descTbl.getPartSpec() + " for table "
              + tableName + " does not exist";
          outStream.write(errMsg.getBytes("UTF-8"));
          ((FSDataOutputStream) outStream).close();
          return 0;
        }
        tbl = part.getTable();
      }
    } catch (FileNotFoundException e) {
      LOG.info("describe table: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.info("describe table: " + stringifyException(e));
      return 1;
    }

    try {

      LOG.info("DDLTask: got data for " + tbl.getTableName());

      List<FieldSchema> cols = null;
      if (colPath.equals(tableName)) {
        cols = tbl.getCols();
        if (part != null) {
          cols = part.getCols();
        }
      } else {
        cols = Hive.getFieldsFromDeserializer(colPath, tbl.getDeserializer());
      }
      Path resFile = new Path(descTbl.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      DataOutput outStream = fs.create(resFile);
      Iterator<FieldSchema> iterCols = cols.iterator();
      while (iterCols.hasNext()) {
        // create a row per column
        FieldSchema col = iterCols.next();
        outStream.writeBytes(col.getName());
        outStream.write(separator);
        outStream.writeBytes(col.getType());
        outStream.write(separator);
        outStream.writeBytes(col.getComment() == null ? "" : col.getComment());
        outStream.write(terminator);
      }

      if (tableName.equals(colPath)) {
        // also return the partitioning columns
        List<FieldSchema> partCols = tbl.getPartCols();
        Iterator<FieldSchema> iterPartCols = partCols.iterator();
        while (iterPartCols.hasNext()) {
          FieldSchema col = iterPartCols.next();
          outStream.writeBytes(col.getName());
          outStream.write(separator);
          outStream.writeBytes(col.getType());
          outStream.write(separator);
          outStream
              .writeBytes(col.getComment() == null ? "" : col.getComment());
          outStream.write(terminator);
        }

        // if extended desc table then show the complete details of the table
        if (descTbl.isExt()) {
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
            // comment column is empty
            outStream.write(terminator);
          }
        }
      }

      LOG.info("DDLTask: written data for " + tbl.getTableName());
      ((FSDataOutputStream) outStream).close();

    } catch (FileNotFoundException e) {
      LOG.info("describe table: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.info("describe table: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e);
    }

    return 0;
  }

  private void writeFileSystemStats(DataOutput outStream, List<Path> locations,
      Path tabLoc, boolean partSpecified, int indent) throws IOException {
    long totalFileSize = 0;
    long maxFileSize = 0;
    long minFileSize = Long.MAX_VALUE;
    long lastAccessTime = 0;
    long lastUpdateTime = 0;
    int numOfFiles = 0;

    boolean unknown = false;
    FileSystem fs = tabLoc.getFileSystem(conf);
    // in case all files in locations do not exist
    try {
      FileStatus tmpStatus = fs.getFileStatus(tabLoc);
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
          FileStatus status = fs.getFileStatus(tabLoc);
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
   * Alter a given table.
   *
   * @param db
   *          The database in question.
   * @param alterTbl
   *          This is the table we're altering.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int alterTable(Hive db, AlterTableDesc alterTbl) throws HiveException {
    // alter the table
    Table tbl = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, alterTbl
        .getOldName());

    validateAlterTableType(tbl, alterTbl.getOp());

    if (tbl.isView()) {
      if (!alterTbl.getExpectView()) {
        throw new HiveException("Cannot alter a view with ALTER TABLE");
      }
    } else {
      if (alterTbl.getExpectView()) {
        throw new HiveException("Cannot alter a base table with ALTER VIEW");
      }
    }

    Table oldTbl = tbl.copy();

    if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.RENAME) {
      tbl.setTableName(alterTbl.getNewName());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDCOLS) {
      List<FieldSchema> newCols = alterTbl.getNewCols();
      List<FieldSchema> oldCols = tbl.getCols();
      if (tbl.getSerializationLib().equals(
          "org.apache.hadoop.hive.serde.thrift.columnsetSerDe")) {
        console
            .printInfo("Replacing columns for columnsetSerDe and changing to LazySimpleSerDe");
        tbl.setSerializationLib(LazySimpleSerDe.class.getName());
        tbl.getTTable().getSd().setCols(newCols);
      } else {
        // make sure the columns does not already exist
        Iterator<FieldSchema> iterNewCols = newCols.iterator();
        while (iterNewCols.hasNext()) {
          FieldSchema newCol = iterNewCols.next();
          String newColName = newCol.getName();
          Iterator<FieldSchema> iterOldCols = oldCols.iterator();
          while (iterOldCols.hasNext()) {
            String oldColName = iterOldCols.next().getName();
            if (oldColName.equalsIgnoreCase(newColName)) {
              console.printError("Column '" + newColName + "' exists");
              return 1;
            }
          }
          oldCols.add(newCol);
        }
        tbl.getTTable().getSd().setCols(oldCols);
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.RENAMECOLUMN) {
      List<FieldSchema> oldCols = tbl.getCols();
      List<FieldSchema> newCols = new ArrayList<FieldSchema>();
      Iterator<FieldSchema> iterOldCols = oldCols.iterator();
      String oldName = alterTbl.getOldColName();
      String newName = alterTbl.getNewColName();
      String type = alterTbl.getNewColType();
      String comment = alterTbl.getNewColComment();
      boolean first = alterTbl.getFirst();
      String afterCol = alterTbl.getAfterCol();
      FieldSchema column = null;

      boolean found = false;
      int position = -1;
      if (first) {
        position = 0;
      }

      int i = 1;
      while (iterOldCols.hasNext()) {
        FieldSchema col = iterOldCols.next();
        String oldColName = col.getName();
        if (oldColName.equalsIgnoreCase(newName)
            && !oldColName.equalsIgnoreCase(oldName)) {
          console.printError("Column '" + newName + "' exists");
          return 1;
        } else if (oldColName.equalsIgnoreCase(oldName)) {
          col.setName(newName);
          if (type != null && !type.trim().equals("")) {
            col.setType(type);
          }
          if (comment != null) {
            col.setComment(comment);
          }
          found = true;
          if (first || (afterCol != null && !afterCol.trim().equals(""))) {
            column = col;
            continue;
          }
        }

        if (afterCol != null && !afterCol.trim().equals("")
            && oldColName.equalsIgnoreCase(afterCol)) {
          position = i;
        }

        i++;
        newCols.add(col);
      }

      // did not find the column
      if (!found) {
        console.printError("Column '" + oldName + "' does not exist");
        return 1;
      }
      // after column is not null, but we did not find it.
      if ((afterCol != null && !afterCol.trim().equals("")) && position < 0) {
        console.printError("Column '" + afterCol + "' does not exist");
        return 1;
      }

      if (position >= 0) {
        newCols.add(position, column);
      }

      tbl.getTTable().getSd().setCols(newCols);
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.REPLACECOLS) {
      // change SerDe to LazySimpleSerDe if it is columnsetSerDe
      if (tbl.getSerializationLib().equals(
          "org.apache.hadoop.hive.serde.thrift.columnsetSerDe")) {
        console
            .printInfo("Replacing columns for columnsetSerDe and changing to LazySimpleSerDe");
        tbl.setSerializationLib(LazySimpleSerDe.class.getName());
      } else if (!tbl.getSerializationLib().equals(
          MetadataTypedColumnsetSerDe.class.getName())
          && !tbl.getSerializationLib().equals(LazySimpleSerDe.class.getName())
          && !tbl.getSerializationLib().equals(ColumnarSerDe.class.getName())
          && !tbl.getSerializationLib().equals(DynamicSerDe.class.getName())) {
        console.printError("Replace columns is not supported for this table. "
            + "SerDe may be incompatible.");
        return 1;
      }
      tbl.getTTable().getSd().setCols(alterTbl.getNewCols());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDPROPS) {
      tbl.getTTable().getParameters().putAll(alterTbl.getProps());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDSERDEPROPS) {
      tbl.getTTable().getSd().getSerdeInfo().getParameters().putAll(
          alterTbl.getProps());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDSERDE) {
      tbl.setSerializationLib(alterTbl.getSerdeName());
      if ((alterTbl.getProps() != null) && (alterTbl.getProps().size() > 0)) {
        tbl.getTTable().getSd().getSerdeInfo().getParameters().putAll(
            alterTbl.getProps());
      }
      tbl.setFields(Hive.getFieldsFromDeserializer(tbl.getTableName(), tbl
          .getDeserializer()));
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDFILEFORMAT) {
      tbl.getTTable().getSd().setInputFormat(alterTbl.getInputFormat());
      tbl.getTTable().getSd().setOutputFormat(alterTbl.getOutputFormat());
      if (alterTbl.getSerdeName() != null) {
        tbl.setSerializationLib(alterTbl.getSerdeName());
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDCLUSTERSORTCOLUMN) {
      // validate sort columns and bucket columns
      List<String> columns = Utilities.getColumnNamesFromFieldSchema(tbl
          .getCols());
      Utilities.validateColumnNames(columns, alterTbl.getBucketColumns());
      if (alterTbl.getSortColumns() != null) {
        Utilities.validateColumnNames(columns, Utilities
            .getColumnNamesFromSortCols(alterTbl.getSortColumns()));
      }
      tbl.getTTable().getSd().setBucketCols(alterTbl.getBucketColumns());
      tbl.getTTable().getSd().setNumBuckets(alterTbl.getNumberBuckets());
      tbl.getTTable().getSd().setSortCols(alterTbl.getSortColumns());
    } else {
      console.printError("Unsupported Alter commnad");
      return 1;
    }

    // set last modified by properties
    try {
      tbl.setProperty("last_modified_by", conf.getUser());
    } catch (IOException e) {
      console.printError("Unable to get current user: " + e.getMessage(),
          stringifyException(e));
      return 1;
    }
    tbl.setProperty("last_modified_time", Long.toString(System
        .currentTimeMillis() / 1000));

    try {
      tbl.checkValidity();
    } catch (HiveException e) {
      console.printError("Invalid table columns : " + e.getMessage(),
          stringifyException(e));
      return 1;
    }

    try {
      db.alterTable(alterTbl.getOldName(), tbl);
    } catch (InvalidOperationException e) {
      console.printError("Invalid alter operation: " + e.getMessage());
      LOG.info("alter table: " + stringifyException(e));
      return 1;
    } catch (HiveException e) {
      return 1;
    }

    // This is kind of hacky - the read entity contains the old table, whereas
    // the write entity
    // contains the new table. This is needed for rename - both the old and the
    // new table names are
    // passed
    work.getInputs().add(new ReadEntity(oldTbl));
    work.getOutputs().add(new WriteEntity(tbl));
    return 0;
  }

  /**
   * Drop a given table.
   *
   * @param db
   *          The database in question.
   * @param dropTbl
   *          This is the table we're dropping.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int dropTable(Hive db, DropTableDesc dropTbl) throws HiveException {
    // We need to fetch the table before it is dropped so that it can be passed
    // to
    // post-execution hook
    Table tbl = null;
    try {
      tbl = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, dropTbl
          .getTableName());
    } catch (InvalidTableException e) {
      // drop table is idempotent
    }

    if (tbl != null) {
      if (tbl.isView()) {
        if (!dropTbl.getExpectView()) {
          throw new HiveException("Cannot drop a view with DROP TABLE");
        }
      } else {
        if (dropTbl.getExpectView()) {
          throw new HiveException("Cannot drop a base table with DROP VIEW");
        }
      }
    }

    if (dropTbl.getPartSpecs() == null) {
      // drop the table
      db
          .dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, dropTbl
          .getTableName());
      if (tbl != null) {
        work.getOutputs().add(new WriteEntity(tbl));
      }
    } else {
      // get all partitions of the table
      List<String> partitionNames = db.getPartitionNames(
          MetaStoreUtils.DEFAULT_DATABASE_NAME, dropTbl.getTableName(),
          (short) -1);
      Set<Map<String, String>> partitions = new HashSet<Map<String, String>>();
      for (int i = 0; i < partitionNames.size(); i++) {
        try {
          partitions.add(Warehouse.makeSpecFromName(partitionNames.get(i)));
        } catch (MetaException e) {
          LOG.warn("Unrecognized partition name from metastore: "
              + partitionNames.get(i));
        }
      }
      // drop partitions in the list
      List<Partition> partsToDelete = new ArrayList<Partition>();
      for (Map<String, String> partSpec : dropTbl.getPartSpecs()) {
        Iterator<Map<String, String>> it = partitions.iterator();
        while (it.hasNext()) {
          Map<String, String> part = it.next();
          // test if partSpec matches part
          boolean match = true;
          for (Map.Entry<String, String> item : partSpec.entrySet()) {
            if (!item.getValue().equals(part.get(item.getKey()))) {
              match = false;
              break;
            }
          }
          if (match) {
            partsToDelete.add(db.getPartition(tbl, part, false));
            it.remove();
          }
        }
      }

      // drop all existing partitions from the list
      for (Partition partition : partsToDelete) {
        console.printInfo("Dropping the partition " + partition.getName());
        db.dropPartition(MetaStoreUtils.DEFAULT_DATABASE_NAME, dropTbl
            .getTableName(), partition.getValues(), true); // drop data for the
        // partition
        work.getOutputs().add(new WriteEntity(partition));
      }
    }

    return 0;
  }

  /**
   * Check if the given serde is valid.
   */
  private void validateSerDe(String serdeName) throws HiveException {
    try {
      Deserializer d = SerDeUtils.lookupDeserializer(serdeName);
      if (d != null) {
        System.out.println("Found class for " + serdeName);
      }
    } catch (SerDeException e) {
      throw new HiveException("Cannot validate serde: " + serdeName, e);
    }
  }

  /**
   * Create a new table.
   *
   * @param db
   *          The database in question.
   * @param crtTbl
   *          This is the table we're creating.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int createTable(Hive db, CreateTableDesc crtTbl) throws HiveException {
    // create the table
    Table tbl = new Table(crtTbl.getTableName());
    if (crtTbl.getPartCols() != null) {
      tbl.setPartCols(crtTbl.getPartCols());
    }
    if (crtTbl.getNumBuckets() != -1) {
      tbl.setNumBuckets(crtTbl.getNumBuckets());
    }

    if (crtTbl.getStorageHandler() != null) {
      tbl.setProperty(
        org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_STORAGE,
        crtTbl.getStorageHandler());
    }
    HiveStorageHandler storageHandler = tbl.getStorageHandler();

    if (crtTbl.getSerName() != null) {
      tbl.setSerializationLib(crtTbl.getSerName());
    } else {
      if (crtTbl.getFieldDelim() != null) {
        tbl.setSerdeParam(Constants.FIELD_DELIM, crtTbl.getFieldDelim());
        tbl.setSerdeParam(Constants.SERIALIZATION_FORMAT, crtTbl
            .getFieldDelim());
      }
      if (crtTbl.getFieldEscape() != null) {
        tbl.setSerdeParam(Constants.ESCAPE_CHAR, crtTbl.getFieldEscape());
      }

      if (crtTbl.getCollItemDelim() != null) {
        tbl
            .setSerdeParam(Constants.COLLECTION_DELIM, crtTbl
            .getCollItemDelim());
      }
      if (crtTbl.getMapKeyDelim() != null) {
        tbl.setSerdeParam(Constants.MAPKEY_DELIM, crtTbl.getMapKeyDelim());
      }
      if (crtTbl.getLineDelim() != null) {
        tbl.setSerdeParam(Constants.LINE_DELIM, crtTbl.getLineDelim());
      }
    }

    if (crtTbl.getSerdeProps() != null) {
      Iterator<Entry<String, String>> iter = crtTbl.getSerdeProps().entrySet()
        .iterator();
      while (iter.hasNext()) {
        Entry<String, String> m = iter.next();
        tbl.setSerdeParam(m.getKey(), m.getValue());
      }
    }
    if (crtTbl.getTblProps() != null) {
      tbl.getTTable().getParameters().putAll(crtTbl.getTblProps());
    }

    /*
     * We use LazySimpleSerDe by default.
     *
     * If the user didn't specify a SerDe, and any of the columns are not simple
     * types, we will have to use DynamicSerDe instead.
     */
    if (crtTbl.getSerName() == null) {
      if (storageHandler == null) {
        LOG.info("Default to LazySimpleSerDe for table " + crtTbl.getTableName());
        tbl.setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      } else {
        String serDeClassName = storageHandler.getSerDeClass().getName();
        LOG.info("Use StorageHandler-supplied " + serDeClassName
          + " for table " + crtTbl.getTableName());
        tbl.setSerializationLib(serDeClassName);
      }
    } else {
      // let's validate that the serde exists
      validateSerDe(crtTbl.getSerName());
    }

    if (crtTbl.getCols() != null) {
      tbl.setFields(crtTbl.getCols());
    }
    if (crtTbl.getBucketCols() != null) {
      tbl.setBucketCols(crtTbl.getBucketCols());
    }
    if (crtTbl.getSortCols() != null) {
      tbl.setSortCols(crtTbl.getSortCols());
    }
    if (crtTbl.getComment() != null) {
      tbl.setProperty("comment", crtTbl.getComment());
    }
    if (crtTbl.getLocation() != null) {
      tbl.setDataLocation(new Path(crtTbl.getLocation()).toUri());
    }

    tbl.setInputFormatClass(crtTbl.getInputFormat());
    tbl.setOutputFormatClass(crtTbl.getOutputFormat());

    tbl.getTTable().getSd().setInputFormat(
      tbl.getInputFormatClass().getName());
    tbl.getTTable().getSd().setOutputFormat(
      tbl.getOutputFormatClass().getName());

    if (crtTbl.isExternal()) {
      tbl.setProperty("EXTERNAL", "TRUE");
      tbl.setTableType(TableType.EXTERNAL_TABLE);
    }

    // If the sorted columns is a superset of bucketed columns, store this fact.
    // It can be later used to
    // optimize some group-by queries. Note that, the order does not matter as
    // long as it in the first
    // 'n' columns where 'n' is the length of the bucketed columns.
    if ((tbl.getBucketCols() != null) && (tbl.getSortCols() != null)) {
      List<String> bucketCols = tbl.getBucketCols();
      List<Order> sortCols = tbl.getSortCols();

      if ((sortCols.size() > 0) && (sortCols.size() >= bucketCols.size())) {
        boolean found = true;

        Iterator<String> iterBucketCols = bucketCols.iterator();
        while (iterBucketCols.hasNext()) {
          String bucketCol = iterBucketCols.next();
          boolean colFound = false;
          for (int i = 0; i < bucketCols.size(); i++) {
            if (bucketCol.equals(sortCols.get(i).getCol())) {
              colFound = true;
              break;
            }
          }
          if (colFound == false) {
            found = false;
            break;
          }
        }
        if (found) {
          tbl.setProperty("SORTBUCKETCOLSPREFIX", "TRUE");
        }
      }
    }

    int rc = setGenericTableAttributes(tbl);
    if (rc != 0) {
      return rc;
    }

    // create the table
    db.createTable(tbl, crtTbl.getIfNotExists());
    work.getOutputs().add(new WriteEntity(tbl));
    return 0;
  }

  /**
   * Create a new table like an existing table.
   *
   * @param db
   *          The database in question.
   * @param crtTbl
   *          This is the table we're creating.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int createTableLike(Hive db, CreateTableLikeDesc crtTbl) throws HiveException {
    // Get the existing table
    Table tbl = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, crtTbl
        .getLikeTableName());

    tbl.setTableName(crtTbl.getTableName());

    if (crtTbl.isExternal()) {
      tbl.setProperty("EXTERNAL", "TRUE");
    } else {
      tbl.setProperty("EXTERNAL", "FALSE");
    }

    if (crtTbl.getLocation() != null) {
      tbl.setDataLocation(new Path(crtTbl.getLocation()).toUri());
    } else {
      tbl.unsetDataLocation();
    }

    // create the table
    db.createTable(tbl, crtTbl.getIfNotExists());
    work.getOutputs().add(new WriteEntity(tbl));
    return 0;
  }

  /**
   * Create a new view.
   *
   * @param db
   *          The database in question.
   * @param crtView
   *          This is the view we're creating.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int createView(Hive db, CreateViewDesc crtView) throws HiveException {
    Table tbl = new Table(crtView.getViewName());
    tbl.setTableType(TableType.VIRTUAL_VIEW);
    tbl.setSerializationLib(null);
    tbl.clearSerDeInfo();
    tbl.setViewOriginalText(crtView.getViewOriginalText());
    tbl.setViewExpandedText(crtView.getViewExpandedText());
    tbl.setFields(crtView.getSchema());
    if (crtView.getComment() != null) {
      tbl.setProperty("comment", crtView.getComment());
    }
    if (crtView.getTblProps() != null) {
      tbl.getTTable().getParameters().putAll(crtView.getTblProps());
    }

    int rc = setGenericTableAttributes(tbl);
    if (rc != 0) {
      return rc;
    }

    db.createTable(tbl, crtView.getIfNotExists());
    work.getOutputs().add(new WriteEntity(tbl));
    return 0;
  }

  private int setGenericTableAttributes(Table tbl) {
    try {
      tbl.setOwner(conf.getUser());
    } catch (IOException e) {
      console.printError("Unable to get current user: " + e.getMessage(),
          stringifyException(e));
      return 1;
    }
    // set create time
    tbl.setCreateTime((int) (System.currentTimeMillis() / 1000));
    return 0;
  }

  @Override
  public int getType() {
    return StageType.DDL;
  }

  @Override
  public String getName() {
    return "DDL";
  }

}
