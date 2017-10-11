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

package org.apache.cassandra.concurrent;

/**
 * An interface defining the ability to select a Netty TPC scheduler
 * for operations involving the concrete implementations of this interface,
 * see {@link org.apache.cassandra.db.ReadCommand} and {@link org.apache.cassandra.db.Mutation}
 * as examples.
 */
public interface Schedulable
{
    /**
     * Returns the executor to use for submitting the runnables of the operation. This will augment them with
     * information about the type of operation they implement.
     */
    TracingAwareExecutor getOperationExecutor();

    /**
     * Returns an executor to use when scheduling later stages of the processing.
     */
    StagedScheduler getScheduler();
}
