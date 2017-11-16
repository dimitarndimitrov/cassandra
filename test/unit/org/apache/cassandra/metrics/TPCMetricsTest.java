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

package org.apache.cassandra.metrics;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.cql3.CQLTester;

public class TPCMetricsTest extends CQLTester
{

    @Test
    public void testSimpleWriteMetrics() throws Throwable
    {
        createTable("CREATE TABLE %s (user text PRIMARY KEY, commentid int)");
        assertMetricsCount(0, TPCTaskType.WRITE, TPCTaskType.WRITE_RESPONSE);
        
        int numOps = 1000;
        for (int i = 0; i < numOps; ++i)
        {
            execute("INSERT INTO %s (user, commentid) VALUES(?,?);", "user" + i, i);
            Thread.sleep(1);
        }
        assertRows(execute("SELECT COUNT(*) FROM %s"), row(Long.valueOf(numOps)));

        Thread.sleep(10000);
        assertMetricsCount(numOps, TPCTaskType.WRITE, TPCTaskType.WRITE_RESPONSE);
    }
    
    @Test
    public void testSimpleReadMetrics() throws Throwable
    {
        createTable("CREATE TABLE %s (user text PRIMARY KEY, commentid int)");

        int numOps = 1000;
        for (int i = 0; i < numOps; ++i)
        {
            execute("INSERT INTO %s (user, commentid) VALUES(?,?);", "user" + i, i);
        }
        assertRows(execute("SELECT COUNT(*) FROM %s"), row(Long.valueOf(numOps)));

        int initialReads = getTotalCountForMetric(TPCTaskType.READ);
        int initialReadResponses = getTotalCountForMetric(TPCTaskType.READ_RESPONSE);

        Random prng = new Random();
        for (int i = 0; i < numOps; ++i)
        {
            execute("SELECT commentid FROM %s WHERE user=?", "user" + prng.nextInt(numOps));
        }
        
        Thread.sleep(1000);
        assertMetricsCount(numOps + initialReads, TPCTaskType.READ);
        assertMetricsCount(numOps + initialReadResponses, TPCTaskType.READ_RESPONSE);
    }
    
    private static void assertMetricsCount(long targetCount, TPCTaskType... tpcTaskTypes)
    {
        for (TPCTaskType tpcTaskType : tpcTaskTypes)
        {
            int count = getTotalCountForMetric(tpcTaskType);
            Assert.assertEquals("Unexpected count for type " + tpcTaskType, targetCount, count);
        }
    }
    
    private static int getTotalCountForMetric(TPCTaskType tpcTaskType)
    {
        int count = 0;
        for (int i = 0; i < TPC.getNumCores(); ++i)
            count += TPC.metrics(i).scheduledTaskCount(tpcTaskType);
        return count;
    }
}
