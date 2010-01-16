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

package org.apache.hadoop.hive.ql.plan;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;

/**
 * Conditional task resolution interface. This is invoked at run time to get the task to invoke. 
 * Developers can plug in their own resolvers
 */
public class ConditionalResolverMergeFiles implements ConditionalResolver, Serializable {
  private static final long serialVersionUID = 1L;
  public ConditionalResolverMergeFiles() {  
  }
  
  public static class ConditionalResolverMergeFilesCtx implements Serializable {
    private static final long serialVersionUID = 1L;
    List<Task<? extends Serializable>> listTasks;
    private String dir;
    
    public ConditionalResolverMergeFilesCtx() {      
    }
    
    /**
     * @param dir
     */
    public ConditionalResolverMergeFilesCtx(List<Task<? extends Serializable>> listTasks, String dir) {
      this.listTasks = listTasks;
      this.dir = dir;
    }
    
    /**
     * @return the dir
     */
    public String getDir() {
      return dir;
    }

    /**
     * @param dir the dir to set
     */
    public void setDir(String dir) {
      this.dir = dir;
    }
    
    /**
     * @return the listTasks
     */
    public List<Task<? extends Serializable>> getListTasks() {
      return listTasks;
    }

    /**
     * @param listTasks the listTasks to set
     */
    public void setListTasks(List<Task<? extends Serializable>> listTasks) {
      this.listTasks = listTasks;
    }
  }
  
	public List<Task<? extends Serializable>> getTasks(HiveConf conf, Object objCtx) {
    ConditionalResolverMergeFilesCtx ctx = (ConditionalResolverMergeFilesCtx)objCtx;
    String dirName = ctx.getDir();
    
    List<Task<? extends Serializable>> resTsks = new ArrayList<Task<? extends Serializable>>();
    // check if a map-reduce job is needed to merge the files
    // If the current size is smaller than the target, merge
    long trgtSize = conf.getLongVar(HiveConf.ConfVars.HIVEMERGEMAPFILESSIZE);
    long avgConditionSize = conf.getLongVar(HiveConf.ConfVars.HIVEMERGEMAPFILESAVGSIZE);
		trgtSize = trgtSize > avgConditionSize ? trgtSize : avgConditionSize;
    
    try {
      // If the input file does not exist, replace it by a empty file
      Path dirPath = new Path(dirName);
      FileSystem inpFs = dirPath.getFileSystem(conf);
    
      if (inpFs.exists(dirPath)) {
        FileStatus[] fStats = inpFs.listStatus(dirPath);
        long totalSz = 0;
        for (FileStatus fStat : fStats) 
          totalSz += fStat.getLen();
      
        long currAvgSz = totalSz / fStats.length;
        if ((currAvgSz < avgConditionSize) && (fStats.length > 1)) {
          // also set the number of reducers
          Task<? extends Serializable> tsk = ctx.getListTasks().get(1);
          mapredWork work = (mapredWork)tsk.getWork();
     
          int maxReducers = conf.getIntVar(HiveConf.ConfVars.MAXREDUCERS);
          int reducers = (int)((totalSz + trgtSize - 1) / trgtSize);
          reducers = Math.max(1, reducers);
          reducers = Math.min(maxReducers, reducers);
          work.setNumReduceTasks(reducers);
          resTsks.add(tsk);
          return resTsks;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    resTsks.add(ctx.getListTasks().get(0));
    return resTsks;
  }
}
