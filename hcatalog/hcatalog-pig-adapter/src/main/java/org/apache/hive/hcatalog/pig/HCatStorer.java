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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.pig;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatContext;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HCatStorer.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HCatStorer extends HCatBaseStorer {
  private static final Logger LOG = LoggerFactory.getLogger(HCatStorer.class);

  // Signature for wrapped storer, see comments in LoadFuncBasedInputDriver.initialize
  final public static String INNER_SIGNATURE = "hcatstorer.inner.signature";
  final public static String INNER_SIGNATURE_PREFIX = "hcatstorer_inner_signature";
  // A hash map which stores job credentials. The key is a signature passed by Pig, which is
  //unique to the store func and out file name (table, in our case).
  private static Map<String, Credentials> jobCredentials = new HashMap<String, Credentials>();
  private final static Options validOptions = new Options();
  static {
    try {
      populateValidOptions();
    }
    catch(Throwable t) {
      LOG.error("Failed to build option list: ", t);
      throw new RuntimeException(t);
    }
  }
  private final static CommandLineParser parser = new GnuParser();

  /**
   * @param optString may empty str (not null), in which case it's no-op
   */
  public HCatStorer(String partSpecs, String pigSchema, String optString) throws Exception {
    super(partSpecs, pigSchema);
    String[] optsArr = optString.split(" ");
    CommandLine configuredOptions;
    try {
      configuredOptions = parser.parse(validOptions, optsArr);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "[-" + ON_OOR_VALUE_OPT + "]", validOptions );
      throw e;
    }
    Properties udfProps = UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{sign});
    //downstream code expects it to be set to a valid value
    udfProps.put(ON_OORA_VALUE_PROP, configuredOptions.getOptionValue(ON_OOR_VALUE_OPT, getDefaultValue().name()));
    if(LOG.isDebugEnabled()) {
      LOG.debug("setting " + configuredOptions.getOptionValue(ON_OOR_VALUE_OPT));
    }
    isValidOOROption((String)udfProps.get(ON_OORA_VALUE_PROP));
  }
  public HCatStorer(String partSpecs, String pigSchema) throws Exception {
    this(partSpecs, pigSchema, "");
  }
  public HCatStorer(String partSpecs) throws Exception {
    this(partSpecs, null, "");
  }

  public HCatStorer() throws Exception {
    this(null, null, "");
  }

  @Override
  public OutputFormat getOutputFormat() throws IOException {
    return new HCatOutputFormat();
  }

  /**
   * makes a list of all options that HCatStorer understands
   */
  private static void populateValidOptions() {
    validOptions.addOption(ON_OOR_VALUE_OPT, true, 
      "Controls how store operation handles Pig values which are out of range for the target column" +
      "in Hive table.  Default is to throw an exception.");
  }
  /**
   * check that onOutOfRangeValue handling is configured properly
   * @throws FrontendException
   */
  private static void isValidOOROption(String optVal) throws FrontendException {
    boolean found = false;
    for(OOR_VALUE_OPT_VALUES v : OOR_VALUE_OPT_VALUES.values()) {
      if(v.name().equalsIgnoreCase(optVal)) {
        found = true;
        break;
      }
    }
    if(!found) {
      throw new FrontendException("Unexpected value for '" + ON_OOR_VALUE_OPT + "' found: " + optVal);
    }
  }
  /**
   * @param location databaseName.tableName
   */
  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    Configuration config = job.getConfiguration();
    config.set(INNER_SIGNATURE, INNER_SIGNATURE_PREFIX + "_" + sign);
    Properties udfProps = UDFContext.getUDFContext().getUDFProperties(
      this.getClass(), new String[]{sign});
    String[] userStr = location.split("\\.");

    if (udfProps.containsKey(HCatConstants.HCAT_PIG_STORER_LOCATION_SET)) {
      for (Enumeration<Object> emr = udfProps.keys(); emr.hasMoreElements(); ) {
        PigHCatUtil.getConfigFromUDFProperties(udfProps, config, emr.nextElement().toString());
      }
      Credentials crd = jobCredentials.get(INNER_SIGNATURE_PREFIX + "_" + sign);
      if (crd != null) {
        job.getCredentials().addAll(crd);
      }
    } else {
      Job clone = new Job(job.getConfiguration());
      OutputJobInfo outputJobInfo;
      if (userStr.length == 2) {
        outputJobInfo = OutputJobInfo.create(userStr[0], userStr[1], partitions);
      } else if (userStr.length == 1) {
        outputJobInfo = OutputJobInfo.create(null, userStr[0], partitions);
      } else {
        throw new FrontendException("location " + location
          + " is invalid. It must be of the form [db.]table",
          PigHCatUtil.PIG_EXCEPTION_CODE);
      }
      Schema schema = (Schema) ObjectSerializer.deserialize(udfProps.getProperty(PIG_SCHEMA));
      if (schema != null) {
        pigSchema = schema;
      }
      if (pigSchema == null) {
        throw new FrontendException(
          "Schema for data cannot be determined.",
          PigHCatUtil.PIG_EXCEPTION_CODE);
      }
      String externalLocation = (String) udfProps.getProperty(HCatConstants.HCAT_PIG_STORER_EXTERNAL_LOCATION);
      if (externalLocation != null) {
        outputJobInfo.setLocation(externalLocation);
      }
      try {
        HCatOutputFormat.setOutput(job, outputJobInfo);
      } catch (HCatException he) {
        // pass the message to the user - essentially something about
        // the table
        // information passed to HCatOutputFormat was not right
        throw new PigException(he.getMessage(),
          PigHCatUtil.PIG_EXCEPTION_CODE, he);
      }
      HCatSchema hcatTblSchema = HCatOutputFormat.getTableSchema(job.getConfiguration());
      try {
        doSchemaValidations(pigSchema, hcatTblSchema);
      } catch (HCatException he) {
        throw new FrontendException(he.getMessage(), PigHCatUtil.PIG_EXCEPTION_CODE, he);
      }
      computedSchema = convertPigSchemaToHCatSchema(pigSchema, hcatTblSchema);
      HCatOutputFormat.setSchema(job, computedSchema);
      udfProps.setProperty(COMPUTED_OUTPUT_SCHEMA, ObjectSerializer.serialize(computedSchema));

      // We will store all the new /changed properties in the job in the
      // udf context, so the the HCatOutputFormat.setOutput and setSchema
      // methods need not be called many times.
      for (Entry<String, String> keyValue : job.getConfiguration()) {
        String oldValue = clone.getConfiguration().getRaw(keyValue.getKey());
        if ((oldValue == null) || (keyValue.getValue().equals(oldValue) == false)) {
          udfProps.put(keyValue.getKey(), keyValue.getValue());
        }
      }
      //Store credentials in a private hash map and not the udf context to
      // make sure they are not public.
      jobCredentials.put(INNER_SIGNATURE_PREFIX + "_" + sign, job.getCredentials());
      udfProps.put(HCatConstants.HCAT_PIG_STORER_LOCATION_SET, true);
    }
  }

  @Override
  public void storeSchema(ResourceSchema schema, String arg1, Job job) throws IOException {
    ShimLoader.getHadoopShims().getHCatShim().commitJob(getOutputFormat(), job);
  }

  @Override
  public void cleanupOnFailure(String location, Job job) throws IOException {
    ShimLoader.getHadoopShims().getHCatShim().abortJob(getOutputFormat(), job);
  }
}
