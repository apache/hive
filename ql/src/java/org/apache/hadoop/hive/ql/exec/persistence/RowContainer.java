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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;

/**
 * Simple persistent container for rows.
 *
 * This container interface only accepts adding or appending new rows and
 * iterating through the rows in the order of their insertions.
 *
 * The iterator interface is a lightweight first()/next() API rather than
 * the Java Iterator interface. This way we do not need to create 
 * an Iterator object every time we want to start a new iteration. Below is 
 * simple example of how to convert a typical Java's Iterator code to the LW
 * iterator iterface.
 * 
 * Itereator itr = rowContainer.iterator();
 * while (itr.hasNext()) {
 *   v = itr.next();
 *   // do anything with v
 * }
 * 
 * can be rewritten to:
 * 
 * for ( v =  rowContainer.first(); 
 *       v != null; 
 *       v =  rowContainer.next()) {
 *   // do anything with v
 * }
 *
 * The adding and iterating operations can be interleaving. 
 *
 */
public class RowContainer<Row extends List> {
  
  protected Log LOG = LogFactory.getLog(this.getClass().getName());
  
  // max # of rows can be put into one block
  private static final int BLOCKSIZE   = 25000;
  private static final int BLKMETA_LEN = 100; // default # of block metadata: (offset,length) pair
  
  private Row[] lastBlock;   // the last block that add() should append to 
  private Row[] currBlock;   // the current block where the cursor is in
  private int blockSize;     // number of objects in the block before it is spilled to disk
  private int numBlocks;     // total # of blocks
  private int size;          // total # of elements in the RowContainer
  private File tmpFile;            // temporary file holding the spilled blocks
  private RandomAccessFile rFile;  // random access file holding the data
  private long[] off_len;          // offset length pair: i-th position is offset, (i+1)-th position is length
  private int itrCursor;     // iterator cursor in the currBlock
  private int addCursor;     // append cursor in the lastBlock
  private int pBlock;        // pointer to the iterator block
  private SerDe serde;       // serialization/deserialization for the row
  private ObjectInspector standardOI;  // object inspector for the row
  private ArrayList dummyRow; // representing empty row (no columns since value art is null)
  
  public RowContainer() {
    this(BLOCKSIZE);
  }
  
  public RowContainer(int blockSize) {
    // no 0-sized block
    this.blockSize = blockSize == 0 ? BLOCKSIZE : blockSize;
    this.size      = 0;
    this.itrCursor = 0;
    this.addCursor = 0;
    this.numBlocks = 0;
    this.pBlock    = 0;
    this.tmpFile   = null;
    this.lastBlock = (Row[]) new ArrayList[blockSize];
    this.currBlock = this.lastBlock;
    this.off_len   = new long[BLKMETA_LEN * 2];
    this.serde     = null;
    this.standardOI= null;
    this.dummyRow  = new ArrayList(0);
  }
  
  public RowContainer(int blockSize, SerDe sd, ObjectInspector oi) {
    this(blockSize);
    setSerDe(sd, oi);
  }
  
  public void setSerDe(SerDe sd, ObjectInspector oi) {
    assert serde != null : "serde is null";
    assert oi != null : "oi is null";
    this.serde = sd;
    this.standardOI = oi;
  }
  
  public void add(Row t) throws HiveException {
    if ( addCursor >= blockSize ) { // spill the current block to tmp file
      spillBlock(lastBlock);
      addCursor = 0;
      if ( numBlocks == 1 )
        lastBlock = (Row[]) new ArrayList[blockSize];
    } 
    lastBlock[addCursor++] = t;
    ++size;
  }
  
  public Row first() {
    if ( size == 0 )
      return null;
    
    if ( pBlock > 0 ) {
      pBlock = 0;
      currBlock = getBlock(0);
      assert currBlock != null: "currBlock == null";
    }  
    if ( currBlock == null && lastBlock != null ) {
      currBlock = lastBlock;
    }
    assert pBlock == 0: "pBlock != 0 ";
    itrCursor = 1;
    return currBlock[0];
  }
  
  public Row next() {
    assert pBlock<= numBlocks: "pBlock " + pBlock + " > numBlocks" + numBlocks; // pBlock should not be greater than numBlocks;
    if ( pBlock < numBlocks ) {
      if ( itrCursor < blockSize ) {
     	  return currBlock[itrCursor++];
	    } else if (  ++pBlock < numBlocks ) {
 	      currBlock = getBlock(pBlock);
 	      assert currBlock != null: "currBlock == null";
 	      itrCursor = 1;
	    	return  currBlock[0];
    	} else {
    	  itrCursor = 0;
    	  currBlock = lastBlock;
    	}
    } 
    // last block (pBlock == numBlocks)
    if ( itrCursor < addCursor )
      return currBlock[itrCursor++];
    else
      return null;
  }
  
  private void spillBlock(Row[] block) throws HiveException {
    try {
      if ( tmpFile == null ) {
        tmpFile = File.createTempFile("RowContainer", ".tmp", new File("/tmp"));
	      LOG.info("RowContainer created temp file " + tmpFile.getAbsolutePath());
 	      // Delete the temp file if the JVM terminate normally through Hadoop job kill command.
        // Caveat: it won't be deleted if JVM is killed by 'kill -9'.
        tmpFile.deleteOnExit(); 
        rFile = new RandomAccessFile(tmpFile, "rw");
      }
      byte[] buf = serialize(block);
      long offset = rFile.length();
      long len = buf.length;
      // append the block at the end
      rFile.seek(offset);
      rFile.write(buf);
      
      // maintain block metadata
      addBlockMetadata(offset, len);
    } catch (Exception e) {
      LOG.debug(e.toString());
      throw new HiveException(e);
    }
  }
  
  /**
   * Maintain the blocks meta data: number of blocks, and the block (offset, length)
   * pair. 
   * @param offset offset of the tmp file where the block was serialized.
   * @param len the length of the serialized block in the temp file.
   */
  private void addBlockMetadata(long offset, long len) {
    if ( (numBlocks+1) * 2 >= off_len.length ) { // expand (offset, len) array
      off_len = Arrays.copyOf(off_len, off_len.length*2);
    }
    off_len[numBlocks*2] = offset;
    off_len[numBlocks*2+1] = len;
    ++numBlocks;
  }
  
  /**
   * Serialize the object into a byte array.
   * @param obj object needed to be serialized
   * @return the byte array that contains the serialized array.
   * @throws IOException
   */
  private byte[] serialize(Row[] obj) throws HiveException {
    assert(serde != null && standardOI != null);
    
    ByteArrayOutputStream  baos;
    DataOutputStream     oos;
    
    try {
      baos = new ByteArrayOutputStream();
      oos = new DataOutputStream(baos);
      
      // # of rows
      oos.writeInt(obj.length); 
      
      // if serde or OI is null, meaning the join value is null, we don't need
      // to serialize anything to disk, just need to keep the length.
      if ( serde != null && standardOI != null ) {
        for ( int i = 0; i < obj.length; ++i ) {
          Writable outVal = serde.serialize(obj[i], standardOI);
          outVal.write(oos);
        }
      }
      oos.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
    return baos.toByteArray();
  }

  /**
   * Deserialize an object from a byte array
   * @param buf the byte array containing the serialized object.
   * @return the serialized object.
   */
  private Row[] deserialize(byte[] buf) throws HiveException {
    ByteArrayInputStream  bais;
    DataInputStream     ois;
    
    try {
      bais = new ByteArrayInputStream(buf);
      ois = new DataInputStream(bais);
      int sz = ois.readInt();
      assert sz == blockSize: 
             "deserialized size " + sz + " is not the same as block size " + blockSize;
      Row[] ret = (Row[]) new ArrayList[sz];
      
      // if serde or OI is null, meaning the join value is null, we don't need
      // to serialize anything to disk, just need to keep the length.
      for ( int i = 0; i < sz; ++i ) {
        if ( serde != null && standardOI != null ) {
          Writable val = serde.getSerializedClass().newInstance();
          val.readFields(ois);
        
          ret[i] = (Row) ObjectInspectorUtils.copyToStandardObject(
                           serde.deserialize(val),
                           serde.getObjectInspector(),
                           ObjectInspectorCopyOption.WRITABLE);
        } else {
          ret[i] = (Row) dummyRow;
        }
      }
      return ret;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  /**
   * Get the number of elements in the RowContainer.
   * @return number of elements in the RowContainer
   */
  public int size() {
    return size;
  }
  
  /**
   * Remove all elements in the RowContainer.
   */
  public void clear() throws HiveException {
    itrCursor = 0;
    addCursor = 0;
    numBlocks = 0;
    pBlock    = 0;
    size      = 0;
    try {
      if ( rFile != null )
        rFile.close();
      if ( tmpFile != null )
        tmpFile.delete();
    } catch (Exception e) {
      LOG.error(e.toString());
      throw new HiveException(e);
    }
    tmpFile = null;
  }
  
  private Row[] getBlock(int block) {
    long offset = off_len[block*2];
    long len = off_len[block*2+1];
    byte[] buf = new byte[(int)len];
    try {
      rFile.seek(offset);
      rFile.readFully(buf);
      currBlock = deserialize(buf);
    } catch (Exception e) {
      LOG.error(e.toString());
      return null;
    }
    return currBlock;
  }
}
