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

package org.apache.hive.hplsql;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.antlr.v4.runtime.ParserRuleContext;

public class Ftp implements Runnable {  
  String host;
  String user;
  String pwd;
  String dir;
  String targetDir;
  String filePattern;
  boolean subdir = false;
  boolean local = false;
  boolean newOnly = false;
  int sessions = 1;
  
  int fileCnt = 0;
  int dirCnt = 0;
  long ftpSizeInBytes = 0;
  
  FTPClient ftp = null;
  ConcurrentLinkedQueue<String> filesQueue = new ConcurrentLinkedQueue<String>();
  Hashtable<String, FTPFile> filesMap = new Hashtable<String, FTPFile>();
  
  AtomicInteger currentFileCnt = new AtomicInteger(1);
  AtomicInteger currentThreadCnt = new AtomicInteger(0);
  AtomicInteger fileCntSuccess = new AtomicInteger(0);
  AtomicLong bytesTransferredAll = new AtomicLong(0);

  Exec exec;
  boolean trace = false;
  boolean info = false;
  
  Ftp(Exec e) {
    exec = e;  
    trace = exec.getTrace();
    info = exec.getInfo();
  }
  
  /**
   * Run COPY FROM FTP command
   */
  Integer run(HplsqlParser.Copy_from_ftp_stmtContext ctx) {
    trace(ctx, "COPY FROM FTP");
    initOptions(ctx);
    ftp = openConnection(ctx);
    if (ftp != null) {
      Timer timer = new Timer();
      timer.start();
      if (info) {
        info(ctx, "Retrieving directory listing");
      }
      retrieveFileList(dir);
      timer.stop();
      if (info) {
        info(ctx, "Files to copy: " + Utils.formatSizeInBytes(ftpSizeInBytes) + ", " + Utils.formatCnt(fileCnt, "file") + ", " + Utils.formatCnt(dirCnt, "subdirectory", "subdirectories") + " scanned (" + timer.format() + ")");
      }
      if (fileCnt > 0) {
        copyFiles(ctx);
      }
    }  
    return 0;
  }
  
  /**
   * Copy the specified files from FTP
   */
  void copyFiles(HplsqlParser.Copy_from_ftp_stmtContext ctx) {
    Timer timer = new Timer();
    timer.start();
    if (fileCnt > 1 && sessions > 1) {
      if (sessions > fileCnt) {
        sessions = fileCnt;
      }
      try {
        Thread threads[] = new Thread[sessions];
        for (int i = 0; i < sessions; i++) {
          threads[i] = new Thread(this);
          threads[i].start();
        }
        for (int i = 0; i < sessions; i++) {
          threads[i].join(); 
        }
      }
      catch(Exception e) { 
      }
    }
    else {            // Transfer files in the single session      
      run();      
    }
    if (info) {
      long elapsed = timer.stop();
      long bytesAll = bytesTransferredAll.get();
      info(ctx, "Transfer complete: " + Utils.formatSizeInBytes(bytesAll) + ", " + fileCntSuccess.get() + " files ok, " + (fileCnt - fileCntSuccess.get()) + " failed, "+ Utils.formatTime(elapsed) + ", " + Utils.formatBytesPerSec(bytesAll, elapsed));
    } 
  }
  
  /**
   * Run a thread to transfer files
   */
  public void run() {
    byte[] data = null;
    Timer timer = new Timer();
    FTPClient ftp = this.ftp;
    if (currentThreadCnt.getAndIncrement() > 0) {
      ftp = openConnection(null);
    }    
    while(true) {
      String file = filesQueue.poll();
      if (file == null) {
        break;
      }
      int num = currentFileCnt.getAndIncrement();
      FTPFile ftpFile = filesMap.get(file);
      long ftpSizeInBytes = ftpFile.getSize(); 
      String fmtSizeInBytes = Utils.formatSizeInBytes(ftpSizeInBytes);
      String targetFile = getTargetFileName(file); 
      if (info) {
        info(null, "  " + file + " - started (" + num + " of " + fileCnt + ", " + fmtSizeInBytes +")");
      }
      try {
        InputStream in = ftp.retrieveFileStream(file);
        OutputStream out = null;
        java.io.File targetLocalFile = null;
        File targetHdfsFile = null;
        if (local) {
          targetLocalFile = new java.io.File(targetFile);
          if (!targetLocalFile.exists()) {            
            targetLocalFile.getParentFile().mkdirs();            
            targetLocalFile.createNewFile();
          }
          out = new FileOutputStream(targetLocalFile, false /*append*/);
        }
        else {
          targetHdfsFile = new File();
          out = targetHdfsFile.create(targetFile, true /*overwrite*/);
        }  
        if (data == null) {
          data = new byte[3*1024*1024];
        }
        int bytesRead = -1;
        long bytesReadAll = 0;
        long start = timer.start();
        long prev = start;
        long readTime = 0;
        long writeTime = 0;
        long cur, cur2, cur3;
        while (true) {
          cur = timer.current();
          bytesRead = in.read(data);
          cur2 = timer.current();
          readTime += (cur2 - cur); 
          if (bytesRead == -1) {
            break;
          }        
          out.write(data, 0, bytesRead);
          out.flush();
          cur3 = timer.current();
          writeTime += (cur3 - cur2); 
          bytesReadAll += bytesRead;
          if (info) {
            cur = timer.current();
            if (cur - prev > 13000) {
              long elapsed = cur - start;
              info(null, "  " + file + " - in progress (" + Utils.formatSizeInBytes(bytesReadAll) + " of " + fmtSizeInBytes + ", " + Utils.formatPercent(bytesReadAll, ftpSizeInBytes) + ", " + Utils.formatTime(elapsed) + ", " + Utils.formatBytesPerSec(bytesReadAll, elapsed) + ", " + Utils.formatBytesPerSec(bytesReadAll, readTime) + " read, " + Utils.formatBytesPerSec(bytesReadAll, writeTime) + " write)");
              prev = cur;
            }
          }          
        }
        if (ftp.completePendingCommand()) {
          in.close();
          cur = timer.current();
          out.close();
          readTime += (timer.current() - cur); 
          bytesTransferredAll.addAndGet(bytesReadAll);
          fileCntSuccess.incrementAndGet();
          if (info) {
            long elapsed = timer.stop();
            info(null, "  " + file + " - complete (" + Utils.formatSizeInBytes(bytesReadAll) + ", " + Utils.formatTime(elapsed) + ", " + Utils.formatBytesPerSec(bytesReadAll, elapsed) + ", " + Utils.formatBytesPerSec(bytesReadAll, readTime) + " read, " + Utils.formatBytesPerSec(bytesReadAll, writeTime) + " write)");
          }
        } 
        else {
          in.close();
          out.close();
          if (info) {
            info(null, "  " + file + " - failed");
          }
          exec.signal(Signal.Type.SQLEXCEPTION, "File transfer failed: " + file);
        }
      }
      catch(IOException e) {
        exec.signal(e);
      }
    }
    try {
      if (ftp.isConnected()) {
        ftp.logout();
        ftp.disconnect();
      }
    } 
    catch (IOException e) {      
    }
  }
  
  /**
   * Get the list of files to transfer
   */
  void retrieveFileList(String dir) {
    if (info) {
      if (dir == null || dir.isEmpty()) {
        info(null, "  Listing the current working FTP directory");
      }
      else {
        info(null, "  Listing " + dir);
      }
    }
    try {
      FTPFile[] files = ftp.listFiles(dir);
      ArrayList<FTPFile> dirs = new ArrayList<FTPFile>();
      for (FTPFile file : files) {
        String name = file.getName();
        if (file.isFile()) {
          if (filePattern == null || Pattern.matches(filePattern, name)) {
            if (dir != null && !dir.isEmpty()) {
              if (dir.endsWith("/")) {
                name = dir + name;
              }
              else {
                name = dir + "/" + name;
              }
            }
            if (!newOnly || !isTargetExists(name)) {
              fileCnt++;
              ftpSizeInBytes += file.getSize();
              filesQueue.add(name);
              filesMap.put(name, file);
            }
          }
        }
        else {
          if (subdir && !name.equals(".") && !name.equals("..")) {
            dirCnt++;
            dirs.add(file);
          }
        }
      }
      if (subdir) {
        for (FTPFile d : dirs) {
          String sd = d.getName();
          if (dir != null && !dir.isEmpty()) {
            if (dir.endsWith("/")) {
              sd = dir + sd;
            }
            else {
              sd = dir + "/" + sd;
            }
          }
          retrieveFileList(sd);
        }
      }
    }
    catch (IOException e) {      
      exec.signal(e);
    }
  }
  
  /**
   * Open and initialize FTP
   */
  FTPClient openConnection(HplsqlParser.Copy_from_ftp_stmtContext ctx) {
	  FTPClient ftp = new FTPClient();
	  Timer timer = new Timer();
	  timer.start();
	  try {
	    ftp.connect(host);
	    ftp.enterLocalPassiveMode();
	    ftp.setFileType(FTP.BINARY_FILE_TYPE);
	    if (!ftp.login(user, pwd)) {    
	      if (ftp.isConnected()) {
	        ftp.disconnect();
	      }
	      exec.signal(Signal.Type.SQLEXCEPTION, "Cannot login to FTP server: " + host);
	      return null;
	    }
	    timer.stop();
	    if (info) {
	      info(ctx, "Connected to ftp: " + host + " (" + timer.format() + ")");
	    }
    }
	  catch (IOException e) {      
	    exec.signal(e);
	  }
	  return ftp;
  }
  
  /**
   * Check if the file already exists in the target file system
   */
  boolean isTargetExists(String name) {
    String target = getTargetFileName(name);
    try {
      if (local) {
        if (new java.io.File(target).exists()) {
          return true;
        }
      }
      else if (new File().exists(target)) {
        return true;
      }
    }
    catch(Exception e) {      
    }
    return false;        
  }
  
  /**
   * Get the target file relative path and name
   */
  String getTargetFileName(String file) {
    String outFile = file;
    // Remove source dir from file
    if (dir != null) {
      if (targetDir != null) {
        outFile = targetDir + file.substring(dir.length()); 
      }
      else {
        outFile = file.substring(dir.length());
      }
    }
    else if (targetDir != null) {
      outFile = targetDir + "/" + file;
    }
    return outFile;
  }
  
  /**
   * Initialize COPY FROM FTP command options
   */
  void initOptions(HplsqlParser.Copy_from_ftp_stmtContext ctx) {
    host = evalPop(ctx.expr()).toString();
    user = "anonymous";
    pwd = "";
    int cnt = ctx.copy_ftp_option().size();
    for (int i = 0; i < cnt; i++) {
      HplsqlParser.Copy_ftp_optionContext option = ctx.copy_ftp_option(i);
      if (option.T_USER() != null) {
        user = evalPop(option.expr()).toString();
      }
      else if (option.T_PWD() != null) {
        pwd = evalPop(option.expr()).toString();
      }
      else if (option.T_DIR() != null) {
        if (option.file_name() != null) {
          dir = option.file_name().getText();
        }
        else {
          dir = evalPop(option.expr()).toString();
        }
      }
      else if (option.T_FILES() != null) {
        filePattern = evalPop(option.expr()).toString();
      }
      else if (option.T_NEW() != null) {
        newOnly = true;
      }
      else if (option.T_SUBDIR() != null) {
        subdir = true;
      }
      else if (option.T_SESSIONS() != null) {
        sessions = evalPop(option.expr()).intValue();
      }
      else if (option.T_TO() != null) {
        if (option.file_name() != null) {
          targetDir = option.file_name().getText();
        }
        else {
          targetDir = evalPop(option.expr()).toString();
        }
        if (option.T_LOCAL() != null) {
          local = true;
        }
      }
    }
  }

  /**
   * Evaluate the expression and pop value from the stack
   */
  Var evalPop(ParserRuleContext ctx) {
    exec.visit(ctx);
    if (!exec.stack.isEmpty()) { 
      return exec.stackPop();
    }
    return Var.Empty;
  }

  /**
   * Trace and information
   */
  public void trace(ParserRuleContext ctx, String message) {
    exec.trace(ctx, message);
  }
  
  public void info(ParserRuleContext ctx, String message) {
    exec.info(ctx, message);
  }
}
