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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.validation.entities.udfverify;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.functions.Arguments;
import org.apache.cassandra.cql3.functions.JavaUDF;
import org.apache.cassandra.cql3.functions.UDFContext;
import org.apache.cassandra.cql3.functions.UDFDataType;

/**
 * Used by {@link org.apache.cassandra.cql3.validation.entities.UFVerifierTest}.
 */
public final class ClassWithStaticInnerClass extends JavaUDF
{
    public ClassWithStaticInnerClass(UDFDataType returnType, UDFContext udfContext)
    {
        super(returnType, udfContext);
    }

    protected Object executeAggregateImpl(Object state, Arguments arguments)
    {
        throw new UnsupportedOperationException();
    }

    protected ByteBuffer executeImpl(Arguments arguments)
    {
        return null;
    }

    // this is NOT fine
    static final class ClassWithStaticInner_Inner {

    }
}
