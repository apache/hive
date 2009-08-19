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

import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.exec.Utilities.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;

import org.apache.commons.lang.StringUtils;

/**
 * Alternate implementation (to ExecDriver) of spawning a mapreduce task that runs it from
 * a separate jvm. The primary issue with this is the inability to control logging from
 * a separate jvm in a consistent manner
 **/
public class MapRedTask extends Task<mapredWork> implements Serializable {
    
  private static final long serialVersionUID = 1L;

  public MapRedTask() {
    super();
  }
  
  public int execute() {

    try {
      // enable assertion
      String hadoopExec = conf.getVar(HiveConf.ConfVars.HADOOPBIN);
      String hiveJar = conf.getJar();

      String libJarsOption;
      {
        String addedJars = ExecDriver.getResourceFiles(conf, SessionState.ResourceType.JAR);
        conf.setVar(ConfVars.HIVEADDEDJARS, addedJars);

        String auxJars = conf.getAuxJars();
        // Put auxjars and addedjars together into libjars
        if (StringUtils.isEmpty(addedJars)) {
          if (StringUtils.isEmpty(auxJars)) {
            libJarsOption = " ";
          } else {
            libJarsOption = " -libjars " + auxJars + " ";
          }
        } else {
          if (StringUtils.isEmpty(auxJars)) {
            libJarsOption = " -libjars " + addedJars + " ";
          } else {
            libJarsOption = " -libjars " + addedJars + "," + auxJars + " ";
          }   
        }
      }

      // Generate the hiveConfArgs after potentially adding the jars
      String hiveConfArgs = ExecDriver.generateCmdLine(conf);
      
      mapredWork plan = getWork();

      File planFile = File.createTempFile("plan", ".xml");
      LOG.info("Generating plan file " + planFile.toString());
      FileOutputStream out = new FileOutputStream(planFile);
      Utilities.serializeMapRedWork(plan, out);

      String isSilent = "true".equalsIgnoreCase(System.getProperty("test.silent"))
                        ? "-silent" : "";

      String jarCmd;
      if(ShimLoader.getHadoopShims().usesJobShell()) {
        jarCmd = libJarsOption + hiveJar + " " + ExecDriver.class.getName();
      } else {
        jarCmd = hiveJar + " " + ExecDriver.class.getName() + libJarsOption;
      }

      String cmdLine = hadoopExec + " jar " + jarCmd + 
        " -plan " + planFile.toString() + " " + isSilent + " " + hiveConfArgs; 
      
      String files = ExecDriver.getResourceFiles(conf, SessionState.ResourceType.FILE);
      if(!files.isEmpty()) {
        cmdLine = cmdLine + " -files " + files;
      }

      LOG.info("Executing: " + cmdLine);
      Process executor = null;

      // The user can specify the hadoop memory
      int hadoopMem = conf.getIntVar(HiveConf.ConfVars.HIVEHADOOPMAXMEM);

      if (hadoopMem == 0) 
        executor = Runtime.getRuntime().exec(cmdLine);
      // user specified the memory - only applicable for local mode
      else {
        Map<String, String> variables = System.getenv();
        String[] env = new String[variables.size() + 1];
        int pos = 0;
        
        for (Map.Entry<String, String> entry : variables.entrySet())  
        {  
          String name = entry.getKey();  
          String value = entry.getValue();  
          env[pos++] = name + "=" + value;  
        }  
        
        env[pos] = new String("HADOOP_HEAPSIZE=" + hadoopMem);
        executor = Runtime.getRuntime().exec(cmdLine, env);
      }

      StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out);
      StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, System.err);
      
      outPrinter.start();
      errPrinter.start();
    
      int exitVal = executor.waitFor();

      if(exitVal != 0) {
        LOG.error("Execution failed with exit status: " + exitVal);
      } else {
        LOG.info("Execution completed successfully");
      }

      return exitVal;
    }
    catch (Exception e) {
      e.printStackTrace();
      LOG.error("Exception: " + e.getMessage());
      return (1);
    }
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public boolean hasReduce() {
    mapredWork w = getWork();
    return w.getReducer() != null;
  }
}
