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
 * Copyright 2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2001 by their associated contributors.
 *
 */

package org.apache.hadoop.hive.ql.util.jdbm.helper;

import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * A run-time exception that wraps another exception. The printed stack
 * trace will be that of the wrapped exception.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: WrappedRuntimeException.java,v 1.1 2002/05/31 06:33:20 boisvert Exp $
 */
public class WrappedRuntimeException
    extends RuntimeException
{


    /**
     * The underlying exception.
     */
    private final Exception _except;


    /**
     * Constructs a new runtime exception based on a checked exception.
     *
     * @param message The error message
     * @param except The checked exception
     */
    public WrappedRuntimeException( String message, Exception except )
    {
        super( message == null ? "No message available" : message );

        if ( except instanceof WrappedRuntimeException &&
             ( (WrappedRuntimeException) except )._except != null )
        {
            _except = ( (WrappedRuntimeException) except )._except;
        } else {
            _except = except;
        }
    }


    /**
     * Constructs a new runtime exception based on a checked exception.
     *
     * @param except The checked exception
     */
    public WrappedRuntimeException( Exception except )
    {
        super( except == null || except.getMessage() == null ? "No message available" : except.getMessage() );

        if ( except instanceof WrappedRuntimeException &&
             ( (WrappedRuntimeException) except )._except != null )
        {
            _except = ( (WrappedRuntimeException) except )._except;
        } else {
            _except = except;
        }
    }


    /**
     * Returns the exception wrapped by this runtime exception.
     *
     * @return The exception wrapped by this runtime exception
     */
    public Exception getException()
    {
        return _except;
    }


    public void printStackTrace()
    {
        if ( _except == null ) {
            super.printStackTrace();
        } else {
            _except.printStackTrace();
        }
    }


    public void printStackTrace( PrintStream stream )
    {
        if ( _except == null ) {
            super.printStackTrace( stream );
        } else {
            _except.printStackTrace( stream );
        }
    }


    public void printStackTrace( PrintWriter writer )
    {
        if ( _except == null ) {
            super.printStackTrace( writer );
        } else {
            _except.printStackTrace( writer );
        }
    }

}


