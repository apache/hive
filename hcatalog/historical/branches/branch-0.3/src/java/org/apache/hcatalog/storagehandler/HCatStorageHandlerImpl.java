package org.apache.hcatalog.storagehandler;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecordSerDe;
import org.apache.hcatalog.mapred.HCatMapredInputFormat;
import org.apache.hcatalog.mapred.HCatMapredOutputFormat;
import org.apache.hcatalog.mapreduce.HCatInputStorageDriver;
import org.apache.hcatalog.mapreduce.HCatOutputStorageDriver;
import org.apache.hcatalog.mapreduce.OutputJobInfo;
import org.apache.hcatalog.storagehandler.HCatStorageHandler.DummyInputFormat;
import org.apache.hcatalog.storagehandler.HCatStorageHandler.DummyOutputFormat;

public class HCatStorageHandlerImpl extends HCatStorageHandler {

  Class isd;
  Class osd;

  Log LOG = LogFactory.getLog(HCatStorageHandlerImpl.class);
  
  @Override
  public Class<? extends HCatInputStorageDriver> getInputStorageDriver() {
    return isd;
  }

  @Override
  public Class<? extends HCatOutputStorageDriver> getOutputStorageDriver() {
    return osd;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider()
      throws HiveException {
    return new DummyHCatAuthProvider();
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData)
      throws MetaException {
    // do nothing special
  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    // do nothing special
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // do nothing special
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // do nothing special
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // do nothing special
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    // Information about the table and the job to be performed
    // We pass them on into the mepredif / mapredof

    Properties tprops = tableDesc.getProperties();

    if(LOG.isDebugEnabled()){
      LOG.debug("HCatStorageHandlerImpl configureTableJobProperties:");
      HCatUtil.logStackTrace(LOG);
      HCatUtil.logMap(LOG, "jobProperties", jobProperties);
      if (tprops!= null){
        HCatUtil.logEntrySet(LOG, "tableprops", tprops.entrySet());
      }
      LOG.debug("tablename : "+tableDesc.getTableName());
    }
    
    // copy existing table props first
    for (Entry e : tprops.entrySet()){
      jobProperties.put((String)e.getKey(), (String)e.getValue());
    }
    
    // try to set input format related properties
    try {
      HCatMapredInputFormat.setTableDesc(tableDesc,jobProperties);
    } catch (IOException ioe){
      // ok, things are probably not going to work, but we
      // can't throw out exceptions per interface. So, we log.
      LOG.error("HCatInputFormat init fail " + ioe.getMessage());
      LOG.error(ioe.getStackTrace());
    }

    // try to set output format related properties
    try {
      HCatMapredOutputFormat.setTableDesc(tableDesc,jobProperties);
    } catch (IOException ioe){
      // ok, things are probably not going to work, but we
      // can't throw out exceptions per interface. So, we log.
      LOG.error("HCatOutputFormat init fail " + ioe.getMessage());
      LOG.error(ioe.getStackTrace());
    }
  }

  @Override
  public Configuration getConf() {
    return null;
  }

  @Override
  public void setConf(Configuration conf) {
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
      return HCatRecordSerDe.class;
  }

  @Override
  public final Class<? extends InputFormat> getInputFormatClass() {
      return HCatMapredInputFormat.class;
  }
  
  @Override
  public final Class<? extends OutputFormat> getOutputFormatClass() {
      return HCatMapredOutputFormat.class;
  }

}
