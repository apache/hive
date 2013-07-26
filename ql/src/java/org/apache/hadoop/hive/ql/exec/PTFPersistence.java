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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.SoftReference;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

/*
 * contains all the classes to support persisting a PTF partition,
 */
public class PTFPersistence {

  @SuppressWarnings("unchecked")
  public static ByteBasedList createList(String clsName, int capacity) throws HiveException
  {
    try
    {
      Class<? extends ByteBasedList> cls = (Class<? extends ByteBasedList>) Class.forName(clsName);
      Constructor<? extends ByteBasedList> cons = cls.getConstructor(Integer.TYPE);
      return cons.newInstance(capacity);
    }
    catch(Exception e)
    {
      throw new HiveException(e);
    }
  }

  public static class ByteBasedList
  {
    int startOffset;

    /*
     * (offset,size) of Writables.
     * entry i at position i << 1
     * this array is resizable.
     */
    int[] offsetsArray;

    /*
     * contains actual bytes of Writables.
     * not resizable
     */
    byte[] bytes;
    int bytesUsed;

    int currentSize;
    ReentrantReadWriteLock lock;
    volatile long lastModified;


    public ByteBasedList(int startOffset, int capacity)
    {
      this.startOffset = startOffset;
      bytes = new byte[capacity];
      offsetsArray = new int[INCREMENT_SIZE];
      bytesUsed = 0;
      currentSize = 0;
      lock = new ReentrantReadWriteLock();
      lastModified = System.nanoTime();
    }

    public ByteBasedList()
    {
      this(0, MEDIUM_SIZE);
    }

    protected void reset(int startOffset)  throws HiveException {
      PTFPersistence.lock(lock.writeLock());
      try
      {
        this.startOffset = startOffset;
        bytesUsed = 0;
        currentSize = 0;
        Arrays.fill(offsetsArray, 0);
        lastModified = System.nanoTime();
      }
      finally {
        lock.writeLock().unlock();
      }
    }

    public ByteBasedList(int capacity)
    {
      this(0, capacity);
    }

    /*
     * internal api; used by {@link PersistentByteBasedList} to setup BBList from a file.
     */
    protected ByteBasedList(File file)
    {
      lock = new ReentrantReadWriteLock();
    }

    private void ensureCapacity(int wlen) throws ListFullException
    {
      if ( bytesUsed + wlen > bytes.length)
      {
        throw new ListFullException();
      }

      if ( (2 * currentSize + 1) > offsetsArray.length )
      {
        int[] na = new int[offsetsArray.length + INCREMENT_SIZE];
        System.arraycopy(offsetsArray, 0, na, 0, offsetsArray.length);
        offsetsArray = na;
      }
    }

    private int index(int i) throws HiveException
    {
      int j = i - startOffset;
      j = j << 1;
      if ( j >  2 * currentSize )
      {
        throw new HiveException(String.format("index invalid %d", i));
      }
      return j;
    }

    private void write(Writable w) throws HiveException, IOException
    {
      DataOStream dos = PTFPersistence.dos.get();
      ByteArrayOS bos = dos.getUnderlyingStream();
      bos.reset();
      w.write(dos);
      ensureCapacity(bos.len());
      int i = currentSize * 2;
      System.arraycopy(bos.bytearray(), 0, bytes, bytesUsed, bos.len());
      offsetsArray[i] = bytesUsed;
      offsetsArray[i+1] = bos.len();
      currentSize += 1;
      bytesUsed += bos.len();
      lastModified = System.nanoTime();
    }


    public int size() throws HiveException
    {
      PTFPersistence.lock(lock.readLock());
      try
      {
        return currentSize;
      }
      finally
      {
        lock.readLock().unlock();
      }
    }

    public void get(int i, Writable wObj) throws HiveException
    {
      PTFPersistence.lock(lock.readLock());
      try
      {
        i = index(i);
        DataIStream dis = PTFPersistence.dis.get();
        ByteArrayIS bis = dis.getUnderlyingStream();
        bis.setBuffer(bytes, offsetsArray[i], offsetsArray[i+1]);
        wObj.readFields(dis);
      }
      catch(IOException ie)
      {
        throw new HiveException(ie);
      }
      finally
      {
        lock.readLock().unlock();
      }
    }

    public void append(Writable obj) throws HiveException
    {
      PTFPersistence.lock(lock.writeLock());
      try
      {
        write(obj);
      }
      catch(IOException ie)
      {
        throw new HiveException(ie);
      }
      finally
      {
        lock.writeLock().unlock();
      }

    }

    public Object get(int i, Deserializer deserializer, Writable wObj) throws HiveException
    {
      try
      {
        get(i, wObj);
        return deserializer.deserialize(wObj);
      }
      catch(SerDeException ie)
      {
        throw new HiveException(ie);
      }
    }

    public void append(Object obj, ObjectInspector OI, Serializer serializer) throws HiveException
    {
      try
      {
        append(serializer.serialize(obj, OI));
      }
      catch(SerDeException ie)
      {
        throw new HiveException(ie);
      }
    }

    public Iterator<Writable> iterator(Writable wObj) throws HiveException
    {
      return new WIterator(wObj, startOffset);
    }

    public Iterator<Object> iterator(Deserializer deserializer, Writable wObj)  throws HiveException
    {
      return new OIterator(deserializer, wObj);
    }

    public void dump(StringBuilder bldr, Writable wObj) throws IOException, HiveException
    {
      bldr.append("[");
      Iterator<Writable> wi = iterator(wObj);
      while(wi.hasNext())
      {
        wObj = wi.next();
        bldr.append(wObj).append(", ");
      }
      bldr.append("]\n");
    }

    public void dump(StringBuilder bldr, Deserializer deserializer, Writable wObj) throws IOException, HiveException
    {
      bldr.append("[");
      Iterator<Object> oi = iterator(deserializer, wObj);
      while(oi.hasNext())
      {
        bldr.append(oi.next()).append(", ");
      }
      bldr.append("]\n");
    }

    public void close() {
      bytes = null;
      offsetsArray = null;
    }

    class WIterator implements Iterator<Writable>
    {
      Writable wObj;
      long checkTime;
      int i;

      WIterator(Writable wObj, int offset)
      {
        this.wObj = wObj;
        checkTime = lastModified;
        i = offset;
      }

      @Override
      public boolean hasNext()
      {
        return i < currentSize;
      }

      @Override
      public Writable next()
      {
         if (checkTime != lastModified) {
          throw new ConcurrentModificationException();
        }
         try
         {
           get(i++, wObj);
           return wObj;
         }
         catch(HiveException be)
         {
           throw new RuntimeException(be);
         }
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    }

    class OIterator implements Iterator<Object>
    {
      Deserializer deserializer;
      Iterator<Writable> wi;

      OIterator(Deserializer deserializer, Writable wObj) throws HiveException
      {
        wi = iterator(wObj);
        this.deserializer = deserializer;
      }

      @Override
      public boolean hasNext()
      {
        return wi.hasNext();
      }

      @Override
      public Object next()
      {
        Writable wObj = wi.next();
        try
        {
          return deserializer.deserialize(wObj);
        }catch(SerDeException se)
         {
           throw new RuntimeException(se);
         }
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    }

    public static class ListFullException extends HiveException
    {
      private static final long serialVersionUID = 4745303310812778989L;

      public ListFullException()
      {
        super();
      }

      public ListFullException(String message, Throwable cause)
      {
        super(message, cause);
      }

      public ListFullException(String message)
      {
        super(message);
      }

      public ListFullException(Throwable cause)
      {
        super(cause);
      }

    }

    private static final int INCREMENT_SIZE = (int) Math.pow(2, 16);

    public static final int SMALL_SIZE =  (int) Math.pow(2, 6 +10);                // 64KB
    public static final int MEDIUM_SIZE = (int) Math.pow(2, (10 + 10 + 3));            // 8 MB
    public static final int LARGE_SIZE = (int) Math.pow(2, (6 + 10 + 10));         // 64 MB

  }

  public static class PartitionedByteBasedList extends ByteBasedList
  {
    private static final ShutdownHook hook = new ShutdownHook();

    static {
      Runtime.getRuntime().addShutdownHook(hook);
    }

    ArrayList<ByteBasedList> partitions;
    ArrayList<Integer> partitionOffsets;
    ArrayList<File> reusableFiles;
    File dir;
    int batchSize;

    public PartitionedByteBasedList(int batchSize) throws HiveException
    {
      this.batchSize = batchSize;
      currentSize = 0;
      hook.register(dir = PartitionedByteBasedList.createTempDir());

      partitions = new ArrayList<ByteBasedList>();
      partitionOffsets = new ArrayList<Integer>();
      reusableFiles = new ArrayList<File>();
      addPartition();
    }

    public PartitionedByteBasedList() throws HiveException
    {
      this(ByteBasedList.LARGE_SIZE);
    }

    @Override
    protected void reset(int startOffset) throws HiveException {
      PTFPersistence.lock(lock.writeLock());
      currentSize = 0;
      try {
        for (int i = 0; i < partitions.size() - 1; i++) {
          ByteBasedList p = partitions.get(i);
          reusableFiles.add(((PersistentByteBasedList)p).getFile());
        }
        ByteBasedList memstore = partitions.get(partitions.size() - 1);
        memstore.reset(0);

        partitions.clear();
        partitionOffsets.clear();

        partitions.add(memstore);
        partitionOffsets.add(0);
      }
      finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public void close() {
      super.close();
      reusableFiles.clear();
      partitionOffsets.clear();
      for (ByteBasedList partition : partitions) {
        partition.close();
      }
      partitions.clear();
      try {
        PartitionedByteBasedList.deleteRecursively(dir);
      } catch (Exception e) {
      }
      hook.unregister(dir);
    }

    private void addPartition() throws HiveException
    {
      try
      {
        if ( partitions.size() > 0 )
        {
          int idx = partitions.size() - 1;
          ByteBasedList bl = partitions.get(idx);
          File f;
          if ( reusableFiles.size() > 0 ) {
            f = reusableFiles.remove(0);
          }
          else {
            f = File.createTempFile("wdw", null, dir);
          }
          PersistentByteBasedList.store(bl, f);
          partitions.set(idx, new PersistentByteBasedList(f, bl));

        }
        ByteBasedList bl = new ByteBasedList(currentSize, batchSize);
        partitions.add(bl);
        partitionOffsets.add(currentSize);
      }
      catch(IOException ie)
      {
        throw new HiveException(ie);
      }
    }

    private ByteBasedList getPartition(int i) throws HiveException
    {
      PTFPersistence.lock(lock.readLock());
      try
      {
        int numSplits = partitions.size();
        if ( numSplits == 0) {
          return partitions.get(0);
        }
        int start = 0;
        int end = numSplits - 1;

        while(start < end)
        {
          int mid = (start + end + 1) >>> 1;
          int val = partitionOffsets.get(mid);
          if ( val == i )
          {
            return partitions.get(mid);
          }
          else if ( val < i )
          {
            if ( end == mid)
            {
              return partitions.get(end);
            }
            start = mid;
          }
          else
          {
            end = mid - 1;
          }
        }
        return partitions.get(start);
      }
      finally
      {
        lock.readLock().unlock();
      }
    }

    @Override
    public void get(int i, Writable wObj) throws HiveException
    {
      ByteBasedList bl = getPartition(i);
      bl.get(i, wObj);
    }

    @Override
    public void append(Writable obj) throws HiveException
    {
      PTFPersistence.lock(lock.writeLock());
      try
      {
        partitions.get(partitions.size() -1).append(obj);
        currentSize += 1;
        lastModified = System.nanoTime();
      }
      catch(ListFullException le)
      {
        addPartition();
        append(obj);
      }
      finally
      {
        lock.writeLock().unlock();
      }

    }

    @Override
    public Object get(int i, Deserializer deserializer, Writable wObj) throws HiveException
    {
      ByteBasedList bl = getPartition(i);
      return bl.get(i, deserializer, wObj);
    }

    @Override
    public void append(Object obj, ObjectInspector OI, Serializer serializer) throws HiveException
    {
      PTFPersistence.lock(lock.writeLock());
      try
      {
        partitions.get(partitions.size() -1).append(obj, OI, serializer);
        currentSize += 1;
        lastModified = System.nanoTime();
      }
      catch(ListFullException le)
      {
        addPartition();
        append(obj, OI, serializer);
      }
      finally
      {
        lock.writeLock().unlock();
      }
    }

    @Override
    public Iterator<Writable> iterator(Writable wObj) throws HiveException
    {
      return new WIterator(wObj);
    }

    class WIterator implements Iterator<Writable>
    {
      Writable wObj;
      long checkTime;
      int i;
      Iterator<Writable> pIter;

      WIterator(Writable wObj) throws HiveException
      {
        this.wObj = wObj;
        checkTime = lastModified;
        i = 0;
        pIter = partitions.get(i).iterator(wObj);
      }

      @Override
      public boolean hasNext()
      {
        if ( pIter.hasNext() ) {
          return true;
        }
        if (checkTime != lastModified) {
          throw new ConcurrentModificationException();
        }
        try
        {
          if ( i < partitions.size() )
          {
            pIter = partitions.get(i++).iterator(wObj);
            return hasNext();
          }
          return false;
        }
        catch(HiveException e)
        {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Writable next()
      {
         if (checkTime != lastModified) {
          throw new ConcurrentModificationException();
        }
         return pIter.next();
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    }

    static class ShutdownHook extends Thread
    {
      private final Set<File> dirs = new LinkedHashSet<File>();

      public void register(File dir) {
        dirs.add(dir);
      }

      public void unregister(File dir) {
        dirs.remove(dir);
      }

      @Override
      public void run()
      {
        try
        {
          for (File dir : dirs) {
            PartitionedByteBasedList.deleteRecursively(dir);
          }
        }
        catch(Exception ie)
        {
        }
      }

    }

     // copied completely from guavar09 source
    /**
     * Deletes a file or directory and all contents recursively.
     *
     * <p>
     * If the file argument is a symbolic link the link will be deleted but not
     * the target of the link. If the argument is a directory, symbolic links
     * within the directory will not be followed.
     *
     * @param file
     *            the file to delete
     * @throws IOException
     *             if an I/O error occurs
     * @see #deleteDirectoryContents
     */
    public static void deleteRecursively(File file) throws IOException
    {
      if (file.isDirectory())
      {
        deleteDirectoryContents(file);
      }
      if (!file.delete())
      {
        throw new IOException("Failed to delete " + file);
      }
    }

    // copied completely from guavar09 source
    /**
     * Deletes all the files within a directory. Does not delete the directory
     * itself.
     *
     * <p>
     * If the file argument is a symbolic link or there is a symbolic link in
     * the path leading to the directory, this method will do nothing. Symbolic
     * links within the directory are not followed.
     *
     * @param directory
     *            the directory to delete the contents of
     * @throws IllegalArgumentException
     *             if the argument is not a directory
     * @throws IOException
     *             if an I/O error occurs
     * @see #deleteRecursively
     */
    public static void deleteDirectoryContents(File directory)
        throws IOException
    {
      /*Preconditions.checkArgument(directory.isDirectory(),
          "Not a directory: %s", directory);
      */
      if ( !directory.isDirectory())
      {
        throw new IOException(String.format("Not a directory: %s", directory));
      }

      // Symbolic links will have different canonical and absolute paths
      if (!directory.getCanonicalPath().equals(directory.getAbsolutePath()))
      {
        return;
      }
      File[] files = directory.listFiles();
      if (files == null)
      {
        throw new IOException("Error listing files for " + directory);
      }
      for (File file : files)
      {
        deleteRecursively(file);
      }
    }

    // copied completely from guava to remove dependency on guava
    /** Maximum loop count when creating temp directories. */
    private static final int TEMP_DIR_ATTEMPTS = 10000;
    public static File createTempDir()
    {
      File baseDir = new File(System.getProperty("java.io.tmpdir"));
      String baseName = System.currentTimeMillis() + "-";

      for (int counter = 0; counter < TEMP_DIR_ATTEMPTS; counter++)
      {
        File tempDir = new File(baseDir, baseName + counter);
        if (tempDir.mkdir())
        {
          return tempDir;
        }
      }
      throw new IllegalStateException("Failed to create directory within "
          + TEMP_DIR_ATTEMPTS + " attempts (tried " + baseName + "0 to "
          + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')');
    }

  }

  static class PersistentByteBasedList extends ByteBasedList
  {
    private static int headerSize() { return (Integer.SIZE + Integer.SIZE + Integer.SIZE + Long.SIZE) / Byte.SIZE;}
    protected static void store(ByteBasedList l, File f) throws IOException
    {
      /*
       * write startOffset:bytesUsed:currentSize:lastModified
       */
      int hdrSize = headerSize();
      ByteBuffer buf = ByteBuffer.allocate(hdrSize);

      buf.putInt(l.startOffset);
      buf.putInt(l.bytesUsed);
      buf.putInt(l.currentSize);
      buf.putLong(l.lastModified);
      buf.flip();

      /*
       * note: could save this space by using Memory-Mapped I/O and directly writing to the MM buffer.
       */
      ByteBuffer offsetB = ByteBuffer.allocate((Integer.SIZE/Byte.SIZE) * 2 * l.currentSize);
      IntBuffer iB = offsetB.asIntBuffer();
      iB.put(l.offsetsArray, 0, l.currentSize * 2);

      ByteBuffer bytesB = ByteBuffer.wrap(l.bytes, 0, l.bytesUsed);

      ByteBuffer[] bufs = new ByteBuffer[] { buf, offsetB, bytesB};
      FileOutputStream fos = new FileOutputStream(f);
      try
      {
        FileChannel fc = fos.getChannel();
        while (fc.write(bufs, 0, bufs.length) > 0) {
          ;
        }
      }
      finally
      {
        fos.close();
      }
    }

    protected static void load(ByteBasedList l, File f) throws IOException
    {
      int hdr = headerSize();
      FileInputStream fis = new FileInputStream(f);
      try
      {
        FileChannel fc = fis.getChannel();
        ByteBuffer buf0 = ByteBuffer.allocate(hdr);
        while (buf0.hasRemaining()) {
          fc.read(buf0);
        }
        buf0.flip();
        l.startOffset = buf0.getInt();
        l.bytesUsed = buf0.getInt();
        l.currentSize = buf0.getInt();
        l.lastModified = buf0.getLong();

        /*
         * note: could save this space by using Memory-Mapped I/O and directly writing to the MM buffer.
         */
        ByteBuffer offsetB = ByteBuffer.allocate((Integer.SIZE/Byte.SIZE) * 2 * l.currentSize);
        ByteBuffer bytesB = ByteBuffer.allocate(l.bytesUsed);
        ByteBuffer[] bufs = new ByteBuffer[] { offsetB, bytesB };
        while (fc.read(bufs) > 0) {
          ;
        }

        l.offsetsArray = new int[l.currentSize * 2];
        offsetB.flip();
        IntBuffer iB = offsetB.asIntBuffer();
        iB.get(l.offsetsArray);
        l.bytes = bytesB.array();
      }
      finally
      {
        fis.close();
      }
    }

    File file;
    SoftReference<ByteBasedList> memList;

    protected PersistentByteBasedList(File file, ByteBasedList l)
    {
      super(file);
      this.file = file;
      memList = new SoftReference<ByteBasedList>(l);
    }

    protected PersistentByteBasedList(File file)
    {
      this(file, null);
    }

    @Override
    protected void reset(int startOffset) throws HiveException {
      throw new HiveException("Reset on PersistentByteBasedList not supported");
    }

    @Override
    public void close() {
      super.close();
      ByteBasedList list = memList.get();
      if (list != null) {
        list.close();
      }
      memList.clear();
      try {
        PartitionedByteBasedList.deleteRecursively(file);
      } catch (Exception e) {
        // ignore
      }
    }

    private ByteBasedList getList() throws HiveException
    {
      PTFPersistence.lock(lock.readLock());
      try
      {
        ByteBasedList list = memList.get();
        if (list == null)
        {
          try
          {
            list = new ByteBasedList(file);
            load(list, file);
            memList = new SoftReference<ByteBasedList>(list);
          }
          catch(Exception ie)
          {
            throw new RuntimeException(ie);
          }
        }
        return list;
      }
      finally
      {
        lock.readLock().unlock();
      }
    }

    File getFile() {
      return file;
    }

    @Override
    public int size() throws HiveException
    {
      return getList().size();
    }

    @Override
    public void get(int i, Writable wObj) throws HiveException
    {
      getList().get(i, wObj);
    }

    @Override
    public void append(Writable obj) throws HiveException
    {
      throw new UnsupportedOperationException("Cannot append to a Persisted List");
    }

    @Override
    public Object get(int i, Deserializer deserializer, Writable wObj) throws HiveException
    {
      return getList().get(i, deserializer, wObj);
    }

    @Override
    public void append(Object obj, ObjectInspector OI, Serializer serializer) throws HiveException
    {
      throw new UnsupportedOperationException("Cannot append to a Persisted List");
    }

    @Override
    public Iterator<Writable> iterator(Writable wObj) throws HiveException
    {
      return getList().iterator(wObj);
    }

    @Override
    public Iterator<Object> iterator(Deserializer deserializer, Writable wObj) throws HiveException
    {
      return getList().iterator(deserializer, wObj);
    }

    @Override
    public void dump(StringBuilder bldr, Writable wObj) throws IOException, HiveException
    {
      getList().dump(bldr, wObj);
    }

    @Override
    public void dump(StringBuilder bldr, Deserializer deserializer, Writable wObj) throws IOException, HiveException
    {
      getList().dump(bldr, deserializer, wObj);
    }
  }

  public static class ByteBufferInputStream extends InputStream
  {
    ByteBuffer buffer;
    int mark = -1;

    public void intialize(ByteBuffer buffer)
    {
      this.buffer = buffer;
    }

    public void intialize(ByteBuffer buffer, int off, int len)
    {
      buffer = buffer.duplicate();
      buffer.position(off);
      buffer.limit(off + len);
      this.buffer = buffer.slice();
    }

    @Override
    public int read() throws IOException
    {
      return buffer.hasRemaining() ? (buffer.get() & 0xff) : -1;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException
    {
      int remaining = buffer.remaining();
      len= len <= remaining ? len : remaining;
      buffer.get(b, off, len);
      return len;
    }

    @Override
    public boolean markSupported() { return true; }

    @Override
    public void mark(int readAheadLimit)
    {
      mark = buffer.position();
    }

    @Override
    public void reset()
    {
      if ( mark == -1 ) {
        throw new IllegalStateException();
      }
      buffer.position(mark);
      mark = -1;
    }
  }

  public static class ByteBufferOutputStream extends OutputStream
  {
    ByteBuffer buffer;

    public void intialize(ByteBuffer buffer)
    {
      this.buffer = buffer;
    }

    public void intialize(ByteBuffer buffer, int off, int len)
    {
      buffer = buffer.duplicate();
      buffer.position(off);
      buffer.limit(off + len);
      this.buffer = buffer.slice();
    }

    @Override
    public void write(int b) throws IOException
    {
      buffer.put((byte) b);
    }

    @Override
    public void write(byte b[], int off, int len)
    {
      int remaining = buffer.remaining();
      if ( len > remaining )
      {
        throw new IndexOutOfBoundsException();
      }
      buffer.put(b, off, len);
    }
  }

  public static ThreadLocal<ByteArrayIS> bis = new ThreadLocal<ByteArrayIS>()
  {
    @Override
    protected ByteArrayIS initialValue()
    {
      return new ByteArrayIS();
    }
  };

  public static ThreadLocal<DataIStream> dis = new ThreadLocal<DataIStream>()
  {
    @Override
    protected DataIStream initialValue()
    {
      return new DataIStream(bis.get());
    }
  };

  public static ThreadLocal<ByteArrayOS> bos = new ThreadLocal<ByteArrayOS>()
  {
    @Override
    protected ByteArrayOS initialValue()
    {
      return new ByteArrayOS();
    }
  };

  public static ThreadLocal<DataOStream> dos = new ThreadLocal<DataOStream>()
  {
    @Override
    protected DataOStream initialValue()
    {
      return new DataOStream(bos.get());
    }
  };


  public static class DataIStream extends DataInputStream
  {
    public DataIStream(ByteArrayIS in)
    {
      super(in);
    }

    public ByteArrayIS getUnderlyingStream() { return (ByteArrayIS) in; }
  }

  public static class DataOStream extends DataOutputStream
  {
    public DataOStream(ByteArrayOS out)
    {
      super(out);
    }

    public ByteArrayOS getUnderlyingStream() { return (ByteArrayOS) out; }
  }

  public static class ByteArrayOS extends ByteArrayOutputStream
  {
    public ByteArrayOS() { super(); }
    public ByteArrayOS(int size) { super(size); }
    public final byte[] bytearray() { return buf; }
    public final int len() { return count; }

  }

  public static class ByteArrayIS extends ByteArrayInputStream
  {
    public ByteArrayIS() { super(new byte[0]); }
    public final byte[] bytearray() { return buf; }
    public final void setBuffer(byte[] buf, int offset, int len)
    {
      this.buf = buf;
          this.pos = offset;
          this.count = Math.min(offset + len, buf.length);
          this.mark = offset;
    }

  }

  public static void lock(Lock lock) throws HiveException
  {
    try
    {
      lock.lockInterruptibly();

    }
    catch(InterruptedException ie)
    {
      Thread.currentThread().interrupt();
      throw new HiveException("Operation interrupted", ie);
    }
  }


}
