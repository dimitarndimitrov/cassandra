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

package org.apache.cassandra.net;

import static org.apache.cassandra.net.Verbs.GOSSIP;
import static org.apache.cassandra.net.Verbs.HINTS;
import static org.apache.cassandra.net.Verbs.LWT;
import static org.apache.cassandra.net.Verbs.OPERATIONS;
import static org.apache.cassandra.net.Verbs.READS;
import static org.apache.cassandra.net.Verbs.SCHEMA;
import static org.apache.cassandra.net.Verbs.WRITES;

/**
 * TODO
 */
public enum OSSVerb
{
    MUTATION(WRITES.WRITE),
    HINT(HINTS.HINT),
    READ_REPAIR(WRITES.READ_REPAIR),
    READ(READS.SINGLE_READ),
    REQUEST_RESPONSE(null),
    BATCH_STORE(WRITES.BATCH_STORE),
    BATCH_REMOVE(WRITES.BATCH_REMOVE),
    @Deprecated STREAM_REPLY(null),
    @Deprecated STREAM_REQUEST(null),
    RANGE_SLICE(READS.RANGE_READ),
    @Deprecated BOOTSTRAP_TOKEN(null),
    @Deprecated TREE_REQUEST(null),
    @Deprecated TREE_RESPONSE(null),
    @Deprecated JOIN(null),
    GOSSIP_DIGEST_SYN(GOSSIP.SYN),
    GOSSIP_DIGEST_ACK(GOSSIP.ACK),
    GOSSIP_DIGEST_ACK2(GOSSIP.ACK2),
    @Deprecated DEFINITIONS_ANNOUNCE(null),
    DEFINITIONS_UPDATE(SCHEMA.PUSH),
    TRUNCATE(OPERATIONS.TRUNCATE),
    SCHEMA_CHECK(SCHEMA.VERSION),
    @Deprecated INDEX_SCAN(null),
    REPLICATION_FINISHED(OPERATIONS.REPLICATION_FINISHED),
    INTERNAL_RESPONSE(null), // responses to internal calls
    COUNTER_MUTATION(WRITES.COUNTER_FORWARDING),
    @Deprecated STREAMING_REPAIR_REQUEST(null),
    @Deprecated STREAMING_REPAIR_RESPONSE(null),
    SNAPSHOT(OPERATIONS.SNAPSHOT),
    MIGRATION_REQUEST(SCHEMA.PULL),
    GOSSIP_SHUTDOWN(GOSSIP.SHUTDOWN),
    _TRACE(null),
    ECHO(GOSSIP.ECHO),
    REPAIR_MESSAGE(null),
    PAXOS_PREPARE(LWT.PREPARE),
    PAXOS_PROPOSE(LWT.PROPOSE),
    PAXOS_COMMIT(LWT.COMMIT),
    @Deprecated PAGED_RANGE(null),
    UNUSED_1(null),
    UNUSED_2(null),
    UNUSED_3(null),
    UNUSED_4(null),
    UNUSED_5(null);

    /**
     * The corresponding definition for the verb if there is one. Verbs that have no such definition are:
     *   - Deprecated verbs (and _TRACE): we won't ever receive them and only have them in the enum so ordinal() returns the right answer.
     *   - REQUEST_RESPONSE and INTERNAL_RESPONSE: previous used for all responses so correspond to any number of definitions.
     *   - REPAIR_MESSAGE: we've split all repair messages into sub-definitions so we need some specific handling.
     */
    private final Verb<?, ?> verb;

    OSSVerb(Verb<?, ?> verb)
    {
        this.verb = verb;
    }

    public Verb<?, ?> getDefinition()
    {
        return verb;
    }

    /**
     * TODO
     * @param id
     * @return
     */
    public static OSSVerb getVerbById(int id)
    {
        if (id < 0 || id >= OSSVerb.values().length)
        {
            throw new IllegalArgumentException("No verb found for ID " + id);
        }
        return OSSVerb.values()[id];
    }
}
