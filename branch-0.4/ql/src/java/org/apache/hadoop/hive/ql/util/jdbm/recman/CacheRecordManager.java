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

/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Copyright 2000-2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: CacheRecordManager.java,v 1.9 2005/06/25 23:12:32 doomdark Exp $
 */

package org.apache.hadoop.hive.ql.util.jdbm.recman;

import org.apache.hadoop.hive.ql.util.jdbm.RecordManager;
import org.apache.hadoop.hive.ql.util.jdbm.helper.CacheEvictionException;
import org.apache.hadoop.hive.ql.util.jdbm.helper.CachePolicy;
import org.apache.hadoop.hive.ql.util.jdbm.helper.CachePolicyListener;
import org.apache.hadoop.hive.ql.util.jdbm.helper.DefaultSerializer;
import org.apache.hadoop.hive.ql.util.jdbm.helper.Serializer;
import org.apache.hadoop.hive.ql.util.jdbm.helper.WrappedRuntimeException;

import java.io.IOException;
import java.util.Enumeration;

/**
 *  A RecordManager wrapping and caching another RecordManager.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 * @version $Id: CacheRecordManager.java,v 1.9 2005/06/25 23:12:32 doomdark Exp $
 */
public class CacheRecordManager
    implements RecordManager
{

    /**
     * Wrapped RecordManager
     */
    protected RecordManager _recman;


    /**
     * Cache for underlying RecordManager
     */
    protected CachePolicy _cache;


    /**
     * Construct a CacheRecordManager wrapping another RecordManager and
     * using a given cache policy.
     *
     * @param recman Wrapped RecordManager
     * @param cache Cache policy
     */
    public CacheRecordManager( RecordManager recman, CachePolicy cache )
    {
        if ( recman == null ) {
            throw new IllegalArgumentException( "Argument 'recman' is null" );
        }
        if ( cache == null ) {
            throw new IllegalArgumentException( "Argument 'cache' is null" );
        }
        _recman = recman;
        _cache = cache;
        
        _cache.addListener( new CacheListener() );
    }

    
    /**
     * Get the underlying Record Manager.
     *
     * @return underlying RecordManager or null if CacheRecordManager has
     *         been closed. 
     */
    public RecordManager getRecordManager()
    {
        return _recman;
    }

    
    /**
     * Get the underlying cache policy
     *
     * @return underlying CachePolicy or null if CacheRecordManager has
     *         been closed. 
     */
    public CachePolicy getCachePolicy()
    {
        return _cache;
    }

    
    /**
     *  Inserts a new record using a custom serializer.
     *
     *  @param obj the object for the new record.
     *  @return the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public long insert( Object obj )
        throws IOException
    {
        return insert( obj, DefaultSerializer.INSTANCE );
    }
        
        
    /**
     *  Inserts a new record using a custom serializer.
     *
     *  @param obj the object for the new record.
     *  @param serializer a custom serializer
     *  @return the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized long insert( Object obj, Serializer serializer )
        throws IOException
    {
        checkIfClosed();

        long recid = _recman.insert( obj, serializer );
        try {
            _cache.put( new Long( recid ), new CacheEntry( recid, obj, serializer, false ) );
        } catch ( CacheEvictionException except ) {
            throw new WrappedRuntimeException( except );
        }
        return recid;
    }


    /**
     *  Deletes a record.
     *
     *  @param recid the rowid for the record that should be deleted.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void delete( long recid )
        throws IOException
    {
        checkIfClosed();

        _recman.delete( recid );
        _cache.remove( new Long( recid ) );
    }


    /**
     *  Updates a record using standard Java serialization.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public void update( long recid, Object obj )
        throws IOException
    {
        update( recid, obj, DefaultSerializer.INSTANCE );
    }
    

    /**
     *  Updates a record using a custom serializer.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @param serializer a custom serializer
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void update( long recid, Object obj, 
                                     Serializer serializer )
        throws IOException
    {
        CacheEntry  entry;
        Long        id;
        
        checkIfClosed();

        id = new Long( recid );
        try {
            entry = (CacheEntry) _cache.get( id );
            if ( entry != null ) {
                // reuse existing cache entry
                entry._obj = obj;
                entry._serializer = serializer;
                entry._isDirty = true;
            } else {
                _cache.put( id, new CacheEntry( recid, obj, serializer, true ) );
            }
        } catch ( CacheEvictionException except ) {
            throw new IOException( except.getMessage() );
        }
    }


    /**
     *  Fetches a record using standard Java serialization.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @return the object contained in the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public Object fetch( long recid )
        throws IOException
    {
        return fetch( recid, DefaultSerializer.INSTANCE );
    }

        
    /**
     *  Fetches a record using a custom serializer.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @param serializer a custom serializer
     *  @return the object contained in the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized Object fetch( long recid, Serializer serializer )
        throws IOException
    {
        checkIfClosed();

        Long id = new Long( recid );
        CacheEntry entry = (CacheEntry) _cache.get( id );
        if ( entry == null ) {
            entry = new CacheEntry( recid, null, serializer, false );
            entry._obj = _recman.fetch( recid, serializer );
            try {
                _cache.put( id, entry );
            } catch ( CacheEvictionException except ) {
                throw new WrappedRuntimeException( except );
            }
        }
        return entry._obj;
    }


    /**
     *  Closes the record manager.
     *
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void close()
        throws IOException
    {
        checkIfClosed();

        updateCacheEntries();
        _recman.close();
        _recman = null;
        _cache = null;
    }


    /**
     *  Returns the number of slots available for "root" rowids. These slots
     *  can be used to store special rowids, like rowids that point to
     *  other rowids. Root rowids are useful for bootstrapping access to
     *  a set of data.
     */
    public synchronized int getRootCount()
    {
        checkIfClosed();

        return _recman.getRootCount();
    }


    /**
     *  Returns the indicated root rowid.
     *
     *  @see #getRootCount
     */
    public synchronized long getRoot( int id )
        throws IOException
    {
        checkIfClosed();

        return _recman.getRoot( id );
    }


    /**
     *  Sets the indicated root rowid.
     *
     *  @see #getRootCount
     */
    public synchronized void setRoot( int id, long rowid )
        throws IOException
    {
        checkIfClosed();

        _recman.setRoot( id, rowid );
    }


    /**
     * Commit (make persistent) all changes since beginning of transaction.
     */
    public synchronized void commit()
        throws IOException
    {
        checkIfClosed();
        updateCacheEntries();
        _recman.commit();
    }


    /**
     * Rollback (cancel) all changes since beginning of transaction.
     */
    public synchronized void rollback()
        throws IOException
    {
        checkIfClosed();

        _recman.rollback();

        // discard all cache entries since we don't know which entries
        // where part of the transaction
        _cache.removeAll();
    }


    /**
     * Obtain the record id of a named object. Returns 0 if named object
     * doesn't exist.
     */
    public synchronized long getNamedObject( String name )
        throws IOException
    {
        checkIfClosed();

        return _recman.getNamedObject( name );
    }


    /**
     * Set the record id of a named object.
     */
    public synchronized void setNamedObject( String name, long recid )
        throws IOException
    {
        checkIfClosed();

        _recman.setNamedObject( name, recid );
    }


    /**
     * Check if RecordManager has been closed.  If so, throw an
     * IllegalStateException
     */
    private void checkIfClosed()
        throws IllegalStateException
    {
        if ( _recman == null ) {
            throw new IllegalStateException( "RecordManager has been closed" );
        }
    }

    
    /**
     * Update all dirty cache objects to the underlying RecordManager.
     */
    protected void updateCacheEntries()
        throws IOException
    {
        Enumeration enume = _cache.elements();
        while ( enume.hasMoreElements() ) {
            CacheEntry entry = (CacheEntry) enume.nextElement();
            if ( entry._isDirty ) {
                _recman.update( entry._recid, entry._obj, entry._serializer );
                entry._isDirty = false;
            }
        }
    }

    
    private class CacheEntry
    {

        long _recid;
        Object _obj;
        Serializer _serializer;
        boolean _isDirty;
        
        CacheEntry( long recid, Object obj, Serializer serializer, boolean isDirty )
        {
            _recid = recid;
            _obj = obj;
            _serializer = serializer;
            _isDirty = isDirty;
        }
        
    } // class CacheEntry

    private class CacheListener
        implements CachePolicyListener
    {
        
        /** Notification that cache is evicting an object
         *
         * @arg obj object evited from cache
         *
         */
        public void cacheObjectEvicted( Object obj ) 
            throws CacheEvictionException
        {
            CacheEntry entry = (CacheEntry) obj;
            if ( entry._isDirty ) {
                try {
                    _recman.update( entry._recid, entry._obj, entry._serializer );
                } catch ( IOException except ) {
                    throw new CacheEvictionException( except );
                }
            }
        }
        
    }
}
