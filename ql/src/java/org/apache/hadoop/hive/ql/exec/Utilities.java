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


import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.beans.*;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.plan.PlanUtils.ExpressionTypes;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

@SuppressWarnings("nls")
public class Utilities {

  /**
   * The object in the reducer are composed of these top level fields
   */

  public static enum ReduceField { KEY, VALUE, ALIAS };
  private static volatile mapredWork gWork = null;
  static final private Log LOG = LogFactory.getLog(Utilities.class.getName());

  public static void clearMapRedWork (Configuration job) {
    try {
      Path planPath = new Path(HiveConf.getVar(job, HiveConf.ConfVars.PLAN));
      FileSystem fs = FileSystem.get(job);
      if(fs.exists(planPath)) {
           try {
             fs.delete(planPath, true);
           } catch (IOException e) {
             e.printStackTrace();
           }
      }
    } catch (Exception e) {
    } finally {
      // where a single process works with multiple plans - we must clear
      // the cache before working with the next plan.
      gWork = null;
    }
  }

  public static mapredWork getMapRedWork (Configuration job) {
    try {
      if(gWork == null) {
        synchronized (Utilities.class) {
          if(gWork != null)
            return (gWork);
          InputStream in = new FileInputStream("HIVE_PLAN");
          mapredWork ret = deserializeMapRedWork(in, job);
          gWork = ret;
        }
        gWork.initialize();
      }
      return (gWork);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException (e);
    }
  }

  public static List<String> getFieldSchemaString(List<FieldSchema> fl) {
    if (fl == null) {
      return null;
    }
    
    ArrayList<String> ret = new ArrayList<String>();
    for(FieldSchema f: fl) {
      ret.add(f.getName() + " " + f.getType() + 
              (f.getComment() != null ? (" " + f.getComment()) : ""));
    }
    return ret;
  }
  
  /**
   * Java 1.5 workaround. 
   * From http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=5015403
   */
  public static class EnumDelegate extends DefaultPersistenceDelegate {
    @Override
      protected Expression instantiate(Object oldInstance, Encoder out) {
      return new Expression(Enum.class,
                            "valueOf",
                            new Object[] { oldInstance.getClass(), ((Enum<?>) oldInstance).name() });
    }
    protected boolean mutatesTo(Object oldInstance, Object newInstance) {
      return oldInstance == newInstance;
    }
  }

  public static void setMapRedWork (Configuration job, mapredWork w) {
    try {
      // use the default file system of the job
      FileSystem fs = FileSystem.get(job);
      Path planPath = new Path(HiveConf.getVar(job, HiveConf.ConfVars.SCRATCHDIR),
                               "plan."+randGen.nextInt());
      FSDataOutputStream out = fs.create(planPath);
      serializeMapRedWork(w, out);
      HiveConf.setVar(job, HiveConf.ConfVars.PLAN, planPath.toString());
      // Set up distributed cache
      DistributedCache.createSymlink(job);
      String uriWithLink = planPath.toUri().toString() + "#HIVE_PLAN";
      DistributedCache.addCacheFile(new URI(uriWithLink), job);
      // Cache the object in this process too so lookups don't hit the file system
      synchronized (Utilities.class) {
        gWork = w;
        gWork.initialize();
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException (e);
    }
  }

  public static void serializeTasks(Task<? extends Serializable> t, OutputStream out) {
    XMLEncoder e = new XMLEncoder(out);
    // workaround for java 1.5
    e.setPersistenceDelegate( ExpressionTypes.class, new EnumDelegate() );
    e.setPersistenceDelegate( groupByDesc.Mode.class, new EnumDelegate());
    e.setPersistenceDelegate( Operator.ProgressCounter.class, new EnumDelegate());

    e.writeObject(t);
    e.close();
  }

  /**
   * Serialize the plan object to an output stream.
   * DO NOT use this to write to standard output since it closes the output stream
   * DO USE mapredWork.toXML() instead
   */
  public static void serializeMapRedWork(mapredWork w, OutputStream out) {
    XMLEncoder e = new XMLEncoder(out);
    // workaround for java 1.5
    e.setPersistenceDelegate( ExpressionTypes.class, new EnumDelegate() );
    e.setPersistenceDelegate( groupByDesc.Mode.class, new EnumDelegate());
    e.writeObject(w);
    e.close();
  }

  public static mapredWork deserializeMapRedWork (InputStream in, Configuration conf) {
    XMLDecoder d = new XMLDecoder(in, null, null, conf.getClassLoader());
    mapredWork ret = (mapredWork)d.readObject();
    d.close();
    return (ret);
  }

  public static class Tuple<T, V> {
    private T one;
    private V two;

    public Tuple(T one, V two) {
      this.one = one;
      this.two = two;
    }
    public T getOne() {return this.one;}
    public V getTwo() {return this.two;}
  }

  public static tableDesc defaultTd;
  static {
    // by default we expect ^A separated strings
    // This tableDesc does not provide column names.  We should always use
    // PlanUtils.getDefaultTableDesc(String separatorCode, String columns)
    // or getBinarySortableTableDesc(List<FieldSchema> fieldSchemas) when 
    // we know the column names.
    defaultTd = PlanUtils.getDefaultTableDesc("" + Utilities.ctrlaCode);
  }

  public final static int newLineCode = 10;
  public final static int tabCode = 9;
  public final static int ctrlaCode = 1;
  
  public final static String INDENT = "  ";
  
  // Note: When DDL supports specifying what string to represent null,
  // we should specify "NULL" to represent null in the temp table, and then 
  // we can make the following translation deprecated.  
  public static String nullStringStorage = "\\N";
  public static String nullStringOutput = "NULL";

  public static Random randGen = new Random();
  
  /**
   * Gets the task id if we are running as a Hadoop job.
   * Gets a random number otherwise.
   */ 
  public static String getTaskId(Configuration hconf) {
    String taskid = (hconf == null) ? null : hconf.get("mapred.task.id");
    if((taskid == null) || taskid.equals("")) {
      return (""+randGen.nextInt());
    } else {
      return taskid.replaceAll("task_[0-9]+_", "");
    }
  }

  public static HashMap makeMap(Object ... olist) {
    HashMap ret = new HashMap ();
    for(int i=0; i<olist.length; i += 2) {
      ret.put(olist[i], olist[i+1]);
    }
    return (ret);
  }

  public static Properties makeProperties(String ... olist) {
    Properties ret = new Properties ();
    for(int i=0; i<olist.length; i += 2) {
      ret.setProperty(olist[i], olist[i+1]);
    }
    return (ret);
  }

  public static ArrayList makeList(Object ... olist) {
    ArrayList ret = new ArrayList ();
    for(int i=0; i<olist.length; i++) {
      ret.add(olist[i]);
    }
    return (ret);
  }



  public static class StreamPrinter extends Thread {
    InputStream is;
    String type;
    PrintStream os;
    
    public StreamPrinter(InputStream is, String type, PrintStream os) {
      this.is = is;
      this.type = type;
      this.os = os;
    }
    
    public void run() {
      try {
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line=null;
        if(type != null) {
          while ( (line = br.readLine()) != null)
            os.println(type + ">" + line);
        } else {
          while ( (line = br.readLine()) != null)
            os.println(line);
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();  
      }
    }
  }

  public static tableDesc getTableDesc(Table tbl) {
    return (new tableDesc (tbl.getDeserializer().getClass(), tbl.getInputFormatClass(), tbl.getOutputFormatClass(), tbl.getSchema()));
  }


  public static partitionDesc getPartitionDesc(Partition part) throws HiveException {
    return (new partitionDesc (part));
  }

  public static void addMapWork(mapredWork mr, Table tbl, String alias, Operator<?> work) {
    mr.addMapWork(tbl.getDataLocation().getPath(), alias, work, 
                  new partitionDesc(getTableDesc(tbl), null));
  }

  private static String getOpTreeSkel_helper(Operator<?> op, String indent) {
    if (op == null)
      return "";
  
    StringBuffer sb = new StringBuffer();
    sb.append(indent);
    sb.append(op.toString());
    sb.append("\n");
    if (op.getChildOperators() != null)
      for(Object child: op.getChildOperators()) {
        sb.append(getOpTreeSkel_helper((Operator<?>)child, indent + "  "));
      }

    return sb.toString();
  }

  public static String getOpTreeSkel(Operator<?> op) {
    return getOpTreeSkel_helper(op, "");
  }

  private static boolean isWhitespace( int c ) {
    if( c == -1 ) { return false; }
    return Character.isWhitespace( ( char )c );
  }

  public static boolean contentsEqual( InputStream is1, InputStream is2, boolean ignoreWhitespace )
    throws IOException {
    try {
      if((is1 == is2) || (is1 == null && is2 == null))
          return true;
 
      if(is1 == null || is2 == null)
        return false;
 
      while( true ) {
        int c1 = is1.read();
        while( ignoreWhitespace && isWhitespace( c1 ) )
          c1 = is1.read();
        int c2 = is2.read();
        while( ignoreWhitespace && isWhitespace( c2 ) )
          c2 = is2.read();
        if( c1 == -1 && c2 == -1 )
          return true;
        if( c1 != c2 )
          break;
      }
    } catch( FileNotFoundException e ) {
      e.printStackTrace();
    }
    return false;
  }


  /**
   * convert "From src insert blah blah" to "From src insert ... blah"
   */
  public static String abbreviate(String str, int max) {
    str = str.trim();

    int len = str.length();
    int suffixlength = 20;

    if(len <= max)
      return str;


    suffixlength = Math.min(suffixlength, (max-3)/2);
    String rev = StringUtils.reverse(str);
    
    // get the last few words 
    String suffix = WordUtils.abbreviate(rev, 0, suffixlength, "");
    suffix = StringUtils.reverse(suffix);

    // first few ..
    String prefix = StringUtils.abbreviate(str, max-suffix.length());

    return prefix+suffix;
  }

  public final static String NSTR = "";
  public static enum streamStatus {EOF, TERMINATED}
  public static streamStatus readColumn(DataInput in, OutputStream out) throws IOException {

    while (true) {
      int b;
      try {
        b = (int)in.readByte();
      } catch (EOFException e) {
        return streamStatus.EOF;
      }

      if (b == Utilities.newLineCode) {
        return streamStatus.TERMINATED;
      }

      out.write(b);
    }
    // Unreachable
  }

  /**
   * Convert an output stream to a compressed output stream based on codecs 
   * and compression options specified in the Job Configuration.
   * @param jc Job Configuration
   * @param out Output Stream to be converted into compressed output stream
   * @return compressed output stream
   */
  public static OutputStream createCompressedStream(JobConf jc,
                                                    OutputStream out)
    throws IOException {
    boolean isCompressed = FileOutputFormat.getCompressOutput(jc);
    return createCompressedStream(jc, out, isCompressed);
  }
  
  /**
   * Convert an output stream to a compressed output stream based on codecs
   * codecs in the Job Configuration. Caller specifies directly whether file is 
   * compressed or not
   * @param jc Job Configuration
   * @param out Output Stream to be converted into compressed output stream
   * @param isCompressed whether the output stream needs to be compressed or not
   * @return compressed output stream
   */
  public static OutputStream createCompressedStream(JobConf jc,
                                                    OutputStream out,
                                                    boolean isCompressed)
    throws IOException {
    if(isCompressed) {
      Class<? extends CompressionCodec> codecClass =
        FileOutputFormat.getOutputCompressorClass(jc, DefaultCodec.class);
      CompressionCodec codec = (CompressionCodec)
        ReflectionUtils.newInstance(codecClass, jc);
      return codec.createOutputStream(out);
    } else {
      return (out);
    }
  }

  /**
   * Based on compression option and configured output codec - get extension
   * for output file. This is only required for text files - not sequencefiles
   * @param jc Job Configuration
   * @param isCompressed Whether the output file is compressed or not
   * @return the required file extension (example: .gz)
   */
  public static String getFileExtension(JobConf jc, boolean isCompressed) {
    if(!isCompressed) {
      return "";
    } else {
      Class<? extends CompressionCodec> codecClass =
        FileOutputFormat.getOutputCompressorClass(jc, DefaultCodec.class);
      CompressionCodec codec = (CompressionCodec)
        ReflectionUtils.newInstance(codecClass, jc);
      return codec.getDefaultExtension();
    }
  }

  /**
   * Create a sequencefile output stream based on job configuration
   * @param jc Job configuration
   * @param fs File System to create file in
   * @param file Path to be created
   * @param keyClass Java Class for key
   * @param valClass Java Class for value
   * @return output stream over the created sequencefile
   */
  public static SequenceFile.Writer createSequenceWriter(JobConf jc, FileSystem fs,
                                                         Path file, Class<?> keyClass,
                                                         Class<?> valClass)
    throws IOException {
    boolean isCompressed = SequenceFileOutputFormat.getCompressOutput(jc);
    return createSequenceWriter(jc, fs, file, keyClass, valClass, isCompressed);
  }

  /**
   * Create a sequencefile output stream based on job configuration
   * Uses user supplied compression flag (rather than obtaining it from the Job Configuration)
   * @param jc Job configuration
   * @param fs File System to create file in
   * @param file Path to be created
   * @param keyClass Java Class for key
   * @param valClass Java Class for value
   * @return output stream over the created sequencefile
   */
  public static SequenceFile.Writer createSequenceWriter(JobConf jc, FileSystem fs,
                                                         Path file, Class<?> keyClass,
                                                         Class<?> valClass,
                                                         boolean isCompressed)
    throws IOException {
    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    Class codecClass = null;
    if (isCompressed) {
      compressionType = SequenceFileOutputFormat.getOutputCompressionType(jc);
      codecClass = SequenceFileOutputFormat.getOutputCompressorClass(jc, DefaultCodec.class);
      codec = (CompressionCodec) 
        ReflectionUtils.newInstance(codecClass, jc);
    }
    return (SequenceFile.createWriter(fs, jc, file,
                                      keyClass, valClass, compressionType, codec));

  }
  
  /**
   * Create a RCFile output stream based on job configuration Uses user supplied
   * compression flag (rather than obtaining it from the Job Configuration)
   * 
   * @param jc
   *          Job configuration
   * @param fs
   *          File System to create file in
   * @param file
   *          Path to be created
   * @return output stream over the created rcfile
   */
  public static RCFile.Writer createRCFileWriter(JobConf jc, FileSystem fs,
      Path file, boolean isCompressed) throws IOException {
    CompressionCodec codec = null;
    Class<?> codecClass = null;
    if (isCompressed) {
      codecClass = FileOutputFormat.getOutputCompressorClass(jc,
          DefaultCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, jc);
    }
    return new RCFile.Writer(fs, jc, file, null, codec);
  }

  /**
   * Shamelessly cloned from GenericOptionsParser
   */
  public static String realFile(String newFile, Configuration conf) throws IOException {
    Path path = new Path(newFile);
    URI pathURI =  path.toUri();
    FileSystem fs;

    if (pathURI.getScheme() == null) {
      fs = FileSystem.getLocal(conf);
    } else {
      fs = path.getFileSystem(conf);
    }

    if (!fs.exists(path)) {
      return null;
    }

    try {
      fs.close();
    } catch(IOException e){};

    String file = path.makeQualified(fs).toString();
    // For compatibility with hadoop 0.17, change file:/a/b/c to file:///a/b/c 
    if (StringUtils.startsWith(file, "file:/")
        && !StringUtils.startsWith(file, "file:///")) {
      file = "file:///" + file.substring("file:/".length());
    }
    return file;
  }

  public static List<String> mergeUniqElems(List<String> src, List<String> dest) {
    if (dest == null) return src;
    if (src == null) return dest;
    int pos = 0;

    while (pos < dest.size()) {
      if (!src.contains(dest.get(pos)))
        src.add(dest.get(pos));
      pos++;
    }

    return src;
  }

  private static final String tmpPrefix = "_tmp.";

  public static Path toTempPath(Path orig) {
    if(orig.getName().indexOf(tmpPrefix) == 0)
      return orig;
    return new Path(orig.getParent(), tmpPrefix + orig.getName());
  }

  /**
   * Given a path, convert to a temporary path
   */
  public static Path toTempPath(String orig) {
    return toTempPath(new Path(orig));
  }
  
  /**
   * Detect if the supplied file is a temporary path
   */
  public static boolean isTempPath(FileStatus file) {
    String name = file.getPath().getName();
    // in addition to detecting hive temporary files, we also check hadoop
    // temporary folders that used to show up in older releases
    return (name.startsWith("_task") || name.startsWith(tmpPrefix));
  }

  /**
   * Rename src to dst, or in the case dst already exists, move files in src 
   * to dst.  If there is an existing file with the same name, the new file's 
   * name will be appended with "_1", "_2", etc.
   * @param fs the FileSystem where src and dst are on.  
   * @param src the src directory
   * @param dst the target directory
   * @throws IOException 
   */
  static public void rename(FileSystem fs, Path src, Path dst)
    throws IOException, HiveException {
    if (!fs.rename(src, dst)) {
      throw new HiveException ("Unable to move: " + src + " to: " + dst);
    }
  }  
  /**
   * Rename src to dst, or in the case dst already exists, move files in src 
   * to dst.  If there is an existing file with the same name, the new file's 
   * name will be appended with "_1", "_2", etc.
   * @param fs the FileSystem where src and dst are on.  
   * @param src the src directory
   * @param dst the target directory
   * @throws IOException 
   */
  static public void renameOrMoveFiles(FileSystem fs, Path src, Path dst)
    throws IOException, HiveException {
    if (!fs.exists(dst)) {
      if (!fs.rename(src, dst)) {
        throw new HiveException ("Unable to move: " + src + " to: " + dst);
      }
    } else {
      // move file by file
      FileStatus[] files = fs.listStatus(src);
      for (int i=0; i<files.length; i++) {
        Path srcFilePath = files[i].getPath();
        String fileName = srcFilePath.getName();
        Path dstFilePath = new Path(dst, fileName);
        if (fs.exists(dstFilePath)) {
          int suffix = 0;
          do {
            suffix++;
            dstFilePath = new Path(dst, fileName + "_" + suffix);
          } while (fs.exists(dstFilePath));
        }
        if (!fs.rename(srcFilePath, dstFilePath)) {
          throw new HiveException ("Unable to move: " + src + " to: " + dst);
        }
      }
    }
  }
  
  /** The first group will contain the task id.
   *  The second group is the optional extension.
   *  The file name looks like: "24931_r_000000_0" or "24931_r_000000_0.gz"
   */
  static Pattern fileNameTaskIdRegex = Pattern.compile("^.*_([0-9]*)_[0-9](\\..*)?$");
  
  /**
   * Get the task id from the filename.
   * E.g., get "000000" out of "24931_r_000000_0" or "24931_r_000000_0.gz"
   */
  public static String getTaskIdFromFilename(String filename) {
    Matcher m = fileNameTaskIdRegex.matcher(filename);
    if (!m.matches()) {
      LOG.warn("Unable to get task id from file name: " + filename + ". Using full filename as task id.");
      return filename;
    } else {
      String taskId = m.group(1);
      LOG.debug("TaskId for " + filename + " = " + taskId);
      return taskId;
    }
  }
  /**
   * Remove all temporary files and duplicate (double-committed) files from a given directory.
   */
  public static void removeTempOrDuplicateFiles(FileSystem fs, Path path) throws IOException {
    if(path == null)
      return;

    FileStatus items[] = fs.listStatus(path);
    if(items == null)
      return;

    HashMap<String, FileStatus> taskIdToFile = new HashMap<String, FileStatus>();
    for(FileStatus one: items) {
      if(isTempPath(one)) {
        if(!fs.delete(one.getPath(), true)) {
          throw new IOException ("Unable to delete tmp file: " + one.getPath());
        }
      } else {
        String taskId = getTaskIdFromFilename(one.getPath().getName());
        FileStatus otherFile = taskIdToFile.get(taskId);
        if (otherFile == null) {
          taskIdToFile.put(taskId, one);
        } else {
          if(!fs.delete(one.getPath(), true)) {
            throw new IOException ("Unable to delete duplicate file: "
                + one.getPath() + ". Existing file: " + otherFile.getPath());
          } else {
            LOG.warn("Duplicate taskid file removed: " + one.getPath() 
                + ". Existing file: " + otherFile.getPath());
          }
        }
      }
    }
  }

  public static String getNameMessage(Exception e) {
    return e.getClass().getName() + "(" +  e.getMessage() + ")";
  }

  /**
   * Add new elements to the classpath
   * 
   * @param newPaths
   *          Array of classpath elements
   */
  public static ClassLoader addToClassPath(ClassLoader cloader, String[] newPaths) throws Exception {
    URLClassLoader loader = (URLClassLoader)cloader;
    List<URL> curPath = Arrays.asList(loader.getURLs());
    ArrayList<URL> newPath = new ArrayList<URL>();

    // get a list with the current classpath components
    for(URL onePath: curPath) {
      newPath.add(onePath);
    }
    curPath = newPath;

    for (String onestr : newPaths) {
      // special processing for hadoop-17. file:// needs to be removed
      if (StringUtils.indexOf(onestr, "file://") == 0)
        onestr = StringUtils.substring(onestr, 7);

      URL oneurl = (new File(onestr)).toURL();
      if (!curPath.contains(oneurl)) {
        curPath.add(oneurl);
      }
    }

    return new URLClassLoader(curPath.toArray(new URL[0]), loader);
  }

  /**
   * remove elements from the classpath
   * 
   * @param pathsToRemove
   *          Array of classpath elements
   */
  public static void removeFromClassPath(String[] pathsToRemove) throws Exception {
    Thread curThread = Thread.currentThread();
    URLClassLoader loader = (URLClassLoader) curThread.getContextClassLoader();
    Set<URL> newPath = new HashSet<URL>(Arrays.asList(loader.getURLs()));

    for (String onestr : pathsToRemove) {
      // special processing for hadoop-17. file:// needs to be removed
      if (StringUtils.indexOf(onestr, "file://") == 0)
        onestr = StringUtils.substring(onestr, 7);

      URL oneurl = (new File(onestr)).toURL();
      newPath.remove(oneurl);
    }

    loader = new URLClassLoader(newPath.toArray(new URL[0]));
    curThread.setContextClassLoader(loader);
  }

  public static String formatBinaryString(byte[] array, int start, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = start; i < start + length; i++) {
      sb.append("x");
      sb.append(array[i] < 0 ? array[i] + 256 : array[i] + 0);
    }
    return sb.toString();
  }
  /**
   * Gets the default notification interval to send progress updates to the
   * tracker. Useful for operators that may not output data for a while.
   * 
   * @param hconf
   * @return the interval in miliseconds
   */
  public static int getDefaultNotificationInterval(Configuration hconf) {
    int notificationInterval;
    Integer expInterval = Integer.decode(hconf.get("mapred.tasktracker.expiry.interval"));
    
    if (expInterval != null) {
      notificationInterval = expInterval.intValue() / 2;
    } else {
      // 5 minutes
      notificationInterval = 5 * 60 * 1000;
    }
    return notificationInterval;
  }
}
