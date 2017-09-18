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
package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.tries.TrieNode;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.io.util.WrappingRebufferer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.PageAware;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PartitionIndexTest
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
        TPC.ensureInitialized();
    }

    IPartitioner partitioner = Util.testPartitioner();
    static final int COUNT = 245256;


    /**
     * Tests last-nodes-sizing failure uncovered during code review.
     */
    @Test
    public void testSizingBug() throws IOException, InterruptedException
    {
        for (int i = 1; i < COUNT; i *= 10)
        {
            testGetEq(generateRandomIndex(i));
            testGetEq(generateSequentialIndex(i));
        }
    }

    @Test
    public void testGetEq() throws IOException, InterruptedException
    {
        testGetEq(generateRandomIndex(COUNT));
        testGetEq(generateSequentialIndex(COUNT));
    }

    private void testGetEq(Pair<List<DecoratedKey>, PartitionIndex> data)
    {
        List<DecoratedKey> keys = data.left;
        try (PartitionIndex summary = data.right;
             PartitionIndex.Reader reader = summary.openReader(Rebufferer.ReaderConstraint.NONE))
        {
            for (int i = 0; i < data.left.size(); i++)
            {
                assertEquals(i, reader.exactCandidate(keys.get(i)));
                DecoratedKey key = generateRandomKey();
                assertEquals(eq(keys, key), eq(keys, key, reader.exactCandidate(key)));
            }
        }
    }

    @Test
    public void testGetGt() throws IOException
    {
        testGetGt(generateRandomIndex(COUNT));
        testGetGt(generateSequentialIndex(COUNT));
    }

    private void testGetGt(Pair<List<DecoratedKey>, PartitionIndex> data) throws IOException
    {
        List<DecoratedKey> keys = data.left;
        try (PartitionIndex summary = data.right;
             PartitionIndex.Reader reader = summary.openReader(Rebufferer.ReaderConstraint.NONE))
        {
            for (int i = 0; i < data.left.size(); i++)
            {
                assertEquals(i < data.left.size() - 1 ? i + 1 : -1, gt(keys, keys.get(i), reader));
                DecoratedKey key = generateRandomKey();
                assertEquals(gt(keys, key), gt(keys, key, reader));
            }
        }
    }

    @Test
    public void testGetGe() throws IOException
    {
        testGetGe(generateRandomIndex(COUNT));
        testGetGe(generateSequentialIndex(COUNT));
    }

    public void testGetGe(Pair<List<DecoratedKey>, PartitionIndex> data) throws IOException
    {
        List<DecoratedKey> keys = data.left;
        try (PartitionIndex summary = data.right;
             PartitionIndex.Reader reader = summary.openReader(Rebufferer.ReaderConstraint.NONE))
        {
            for (int i = 0; i < data.left.size(); i++)
            {
                assertEquals(i, ge(keys, keys.get(i), reader));
                DecoratedKey key = generateRandomKey();
                assertEquals(ge(keys, key), ge(keys, key, reader));
            }
        }
    }

    private long gt(List<DecoratedKey> keys, DecoratedKey key, PartitionIndex.Reader summary) throws IOException
    {
        return Optional.fromNullable(summary.ceiling(key, (pos, assumeGreater, sk) -> (assumeGreater || keys.get((int) pos).compareTo(sk) > 0) ? pos : null)).or(-1L);
    }

    private long ge(List<DecoratedKey> keys, DecoratedKey key, PartitionIndex.Reader summary) throws IOException
    {
        return Optional.fromNullable(summary.ceiling(key, (pos, assumeGreater, sk) -> (assumeGreater || keys.get((int) pos).compareTo(sk) >= 0) ? pos : null)).or(-1L);
    }

    private long eq(List<DecoratedKey> keys, DecoratedKey key, long exactCandidate)
    {
        int idx = (int) exactCandidate;
        if (exactCandidate == PartitionIndex.NOT_FOUND)
            return -1;
        return (keys.get(idx).equals(key)) ? idx : -1;
    }

    private long gt(List<DecoratedKey> keys, DecoratedKey key)
    {
        int index = Collections.binarySearch(keys, key);
        if (index < 0)
            index = -1 - index;
        else
            ++index;
        return index < keys.size() ? index : -1;
    }

    private long ge(List<DecoratedKey> keys, DecoratedKey key)
    {
        int index = Collections.binarySearch(keys, key);
        if (index < 0)
            index = -1 - index;
        return index < keys.size() ? index : -1;
    }

    private long eq(List<DecoratedKey> keys, DecoratedKey key)
    {
        int index = Collections.binarySearch(keys, key);
        return index >= 0 ? index : -1;
    }

    @Test
    public void testAddEmptyKey() throws Exception
    {
        IPartitioner p = new RandomPartitioner();
        File file = File.createTempFile("ColumnTrieReaderTest", "");
        SequentialWriter writer = new SequentialWriter(file, SequentialWriterOption.newBuilder().finishOnClose(true).build());
        try (FileHandle.Builder fhBuilder = new FileHandle.Builder(file.getPath())
                                                .bufferSize(PageAware.PAGE_SIZE)
                                                .withChunkCache(ChunkCache.instance)
                                                .mmapped(DatabaseDescriptor.getIndexAccessMode() == Config.DiskAccessMode.mmap);
             PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, fhBuilder);
            )
        {
            DecoratedKey key = p.decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            builder.addEntry(key, 42);
            builder.complete();
            try (PartitionIndex summary = PartitionIndex.load(fhBuilder, partitioner, false, Rebufferer.ReaderConstraint.NONE);
                 PartitionIndex.Reader reader = summary.openReader(Rebufferer.ReaderConstraint.NONE))
            {
                assertEquals(1, summary.size());
                assertEquals(42, reader.getLastIndexPosition());
                assertEquals(42, reader.exactCandidate(key));
            }
        }
    }

    @Test
    public void testIteration() throws IOException
    {
        Pair<List<DecoratedKey>, PartitionIndex> random = generateRandomIndex(COUNT);
        checkIteration(random.left, random.left.size(), random.right);
        random.right.close();
    }

    public void checkIteration(List<DecoratedKey> keys, int keysSize, PartitionIndex index)
    {
        try (PartitionIndex.IndexPosIterator iter = index.allKeysIterator(Rebufferer.ReaderConstraint.NONE))
        {
            int i = 0;
            while (true)
            {
                long pos = iter.nextIndexPos();
                if (pos == PartitionIndex.NOT_FOUND)
                    break;
                assertEquals(i, pos);
                ++i;
            }
            assertEquals(keysSize, i);
        } catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testConstrainedIteration() throws IOException
    {
        Pair<List<DecoratedKey>, PartitionIndex> random = generateRandomIndex(COUNT);
        PartitionIndex summary = random.right;
        List<DecoratedKey> keys = random.left;
        Random rand = new Random();

        for (int i = 0; i < 1000; ++i)
        {
            boolean exactLeft = rand.nextBoolean();
            boolean exactRight = rand.nextBoolean();
            DecoratedKey left = exactLeft ? keys.get(rand.nextInt(keys.size())) : generateRandomKey();
            DecoratedKey right = exactRight ? keys.get(rand.nextInt(keys.size())) : generateRandomKey();
            if (right.compareTo(left) < 0)
            {
                DecoratedKey t = left; left = right; right = t;
                boolean b = exactLeft; exactLeft = exactRight; exactRight = b;
            }

            try (PartitionIndex.IndexPosIterator iter = new PartitionIndex.IndexPosIterator(summary, left, right, Rebufferer.ReaderConstraint.NONE))
            {
                long p = iter.nextIndexPos();
                if (p == PartitionIndex.NOT_FOUND)
                {
                    int idx = (int) ge(keys, left); // first greater key
                    if (idx == -1)
                        continue;
                    assertTrue(left + " <= " + keys.get(idx) + " <= " + right + " but " + idx + " wasn't iterated.", right.compareTo(keys.get(idx)) < 0);
                    continue;
                }

                int idx = (int) p;
                if (p > 0)
                    assertTrue(left.compareTo(keys.get(idx - 1)) > 0);
                if (p < keys.size() - 1)
                    assertTrue(left.compareTo(keys.get(idx + 1)) < 0);
                if (exactLeft)      // must be precise on exact, otherwise could be in any relation
                    assertTrue(left == keys.get(idx));
                while (true)
                {
                    ++idx;
                    long pos = iter.nextIndexPos();
                    if (pos == PartitionIndex.NOT_FOUND)
                        break;
                    assertEquals(idx, pos);
                }
                --idx; // seek at last returned
                if (idx < keys.size() - 1)
                    assertTrue(right.compareTo(keys.get(idx + 1)) < 0);
                if (idx > 0)
                    assertTrue(right.compareTo(keys.get(idx - 1)) > 0);
                if (exactRight)      // must be precise on exact, otherwise could be in any relation
                    assertTrue(right == keys.get(idx));
            } catch (AssertionError e) {
                e.printStackTrace();
                System.out.format("Left %s%s Right %s%s\n", left.asByteComparableSource(), exactLeft ? "#" : "", right.asByteComparableSource(), exactRight ? "#" : "");
                try (PartitionIndex.IndexPosIterator iter2 = new PartitionIndex.IndexPosIterator(summary, left, right, Rebufferer.ReaderConstraint.NONE))
                {
                    long pos;
                    while ((pos = iter2.nextIndexPos()) != PartitionIndex.NOT_FOUND) {
                        System.out.println(keys.get((int) pos).asByteComparableSource());
                    }
                    System.out.format("Left %s%s Right %s%s\n", left.asByteComparableSource(), exactLeft ? "#" : "", right.asByteComparableSource(), exactRight ? "#" : "");
                }
                throw e;
            }
        }
    }

    @Test
    public void testPartialIndex() throws IOException
    {
        for (int reps = 0; reps < 10; ++reps)
        {
            File file = File.createTempFile("ColumnTrieReaderTest", "");
            SequentialWriter writer = new SequentialWriter(file, SequentialWriterOption.newBuilder().finishOnClose(true).build());
            List<DecoratedKey> list = Lists.newArrayList();
            int parts = 15;
            try (FileHandle.Builder fhBuilder = new FileHandle.Builder(file.getPath())
                                                .bufferSize(PageAware.PAGE_SIZE)
                                                .withChunkCache(ChunkCache.instance)
                                                .mmapped(DatabaseDescriptor.getIndexAccessMode() == Config.DiskAccessMode.mmap);
                 PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, fhBuilder);
            )
            {
                writer.setPostFlushListener(() -> builder.markPartitionIndexSynced(writer.getLastFlushOffset()));
                for (int i = 0; i < COUNT; i++)
                {
                    DecoratedKey key = generateRandomLengthKey();
                    list.add(key);
                }
                Collections.sort(list);
                AtomicInteger callCount = new AtomicInteger();

                int i = 0;
                for (int part = 1; part <= parts; ++part)
                {
                    for (; i < COUNT * part / parts; i++)
                        builder.addEntry(list.get(i), i);

                    final int addedSize = i;
                    builder.buildPartial(index ->
                                         {
                                             int indexSize = Collections.binarySearch(list, index.lastKey()) + 1;
                                             assert indexSize >= addedSize - 1;
                                             checkIteration(list, indexSize, index);
                                             index.close();
                                             callCount.incrementAndGet();
                                         }, 0, i * 1024);
                    builder.markDataSynced(i * 1024);
                    // verifier will be called when the sequentialWriter finishes a chunk
                }

                for (; i < COUNT; ++i)
                    builder.addEntry(list.get(i), i);
                builder.complete();
                try (PartitionIndex index = PartitionIndex.load(fhBuilder, partitioner, false, Rebufferer.ReaderConstraint.NONE))
                {
                    checkIteration(list, list.size(), index);
                }
                if (COUNT / parts > 16000)
                    assertEquals(parts - 1, callCount.get());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    class JumpingFile extends SequentialWriter
    {
        long[] cutoffs;
        long[] offsets;

        JumpingFile(File file, SequentialWriterOption option, long... cutoffsAndOffsets)
        {
            super(file, option);
            assert (cutoffsAndOffsets.length & 1) == 0;
            cutoffs = new long[cutoffsAndOffsets.length / 2];
            offsets = new long[cutoffs.length];
            for (int i = 0; i < cutoffs.length; ++i)
            {
                cutoffs[i] = cutoffsAndOffsets[i * 2];
                offsets[i] = cutoffsAndOffsets[i * 2 + 1];
            }
        }

        @Override
        public long position()
        {
            return jumped(super.position(), cutoffs, offsets);
        }
    }

    class JumpingRebufferer extends WrappingRebufferer
    {
        long[] cutoffs;
        long[] offsets;

        public JumpingRebufferer(Rebufferer source, long... cutoffsAndOffsets)
        {
            super(source);
            assert (cutoffsAndOffsets.length & 1) == 0;
            cutoffs = new long[cutoffsAndOffsets.length / 2];
            offsets = new long[cutoffs.length];
            for (int i = 0; i < cutoffs.length; ++i)
            {
                cutoffs[i] = cutoffsAndOffsets[i * 2];
                offsets[i] = cutoffsAndOffsets[i * 2 + 1];
            }
        }

        @Override
        public BufferHolder rebuffer(long position)
        {
            return rebuffer(position, ReaderConstraint.NONE);
        }

        @Override
        public BufferHolder rebuffer(long position, ReaderConstraint rc)
        {
            long pos;

            int idx = Arrays.binarySearch(offsets, position);
            if (idx < 0)
                idx = -2 - idx;
            pos = position;
            if (idx >= 0)
                pos = pos - offsets[idx] + cutoffs[idx];

            super.rebuffer(pos, rc);
            if (idx < cutoffs.length - 1 && buffer.limit() + offset > cutoffs[idx + 1])
                buffer.limit((int) (cutoffs[idx + 1] - offset));
            if (idx >= 0)
                offset = offset - cutoffs[idx] + offsets[idx];
            return this;
        }

        @Override
        public long fileLength()
        {
            return jumped(source.fileLength(), cutoffs, offsets);
        }

        @Override
        public String toString()
        {
            return Arrays.toString(cutoffs) + Arrays.toString(offsets);
        }
    }

    public class PartitionIndexJumping extends PartitionIndex
    {
        final long[] cutoffsAndOffsets;

        public PartitionIndexJumping(FileHandle fh, long trieRoot, long keyCount, DecoratedKey first, DecoratedKey last,
                                   long... cutoffsAndOffsets)
        {
            super(fh, trieRoot, keyCount, first, last);
            this.cutoffsAndOffsets = cutoffsAndOffsets;
        }

        @Override
        protected Rebufferer instantiateRebufferer()
        {
            return new JumpingRebufferer(super.instantiateRebufferer(), cutoffsAndOffsets);
        }
    }

    long jumped(long pos, long[] cutoffs, long[] offsets)
    {
        int idx = Arrays.binarySearch(cutoffs, pos);
        if (idx < 0)
            idx = -2 - idx;
        if (idx < 0)
            return pos;
        return pos - cutoffs[idx] + offsets[idx];
    }

    @Test
    public void testPointerGrowth() throws IOException
    {
        for (int reps = 0; reps < 10; ++reps)
        {
        File file = File.createTempFile("ColumnTrieReaderTest", "");
        long[] cutoffsAndOffsets = new long[] {
                2 * 4096, 1L << 16,
                4 * 4096, 1L << 24,
                6 * 4096, 1L << 31,
                8 * 4096, 1L << 32,
                10 * 4096, 1L << 33,
                12 * 4096, 1L << 34,
                14 * 4096, 1L << 40,
                16 * 4096, 1L << 42
        };
        SequentialWriter writer = new JumpingFile(file, SequentialWriterOption.newBuilder().finishOnClose(true).build(), cutoffsAndOffsets);
        List<DecoratedKey> list = Lists.newArrayList();
        try (FileHandle.Builder fhBuilder = new FileHandle.Builder(file.getPath())
                .bufferSize(PageAware.PAGE_SIZE)
                .withChunkCache(ChunkCache.instance)
                .mmapped(DatabaseDescriptor.getIndexAccessMode() == Config.DiskAccessMode.mmap);
             PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, fhBuilder);
            )
        {
            writer.setPostFlushListener(() -> builder.markPartitionIndexSynced(writer.getLastFlushOffset()));
            for (int i = 0; i < COUNT; i++)
            {
                DecoratedKey key = generateRandomKey();
                list.add(key);
            }
            Collections.sort(list);

            for (int i = 0; i < COUNT; ++i)
                builder.addEntry(list.get(i), i);
            long root = builder.complete();

            try (FileHandle fh = fhBuilder.complete();
                 PartitionIndex index = new PartitionIndexJumping(fh, root, COUNT, null, null, cutoffsAndOffsets);
                 Analyzer analyzer = new Analyzer(index))
            {
                checkIteration(list, list.size(), index);

                analyzer.run();
                if (analyzer.countPerType.elementSet().size() < 7)
                {
                    Assert.fail("Expecting at least 7 different node types, got " + analyzer.countPerType.elementSet().size() + "\n" + analyzer.countPerType);
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        }
    }

    public static class Analyzer extends PartitionIndex.Reader
    {
        Multiset<TrieNode> countPerType = HashMultiset.create();

        public Analyzer(PartitionIndex index)
        {
            super(index, Rebufferer.ReaderConstraint.NONE);
        }

        public void run()
        {
            run(root);
        }

        void run(long node)
        {
            go(node);

            countPerType.add(nodeType);

            int tr = transitionRange();
            for (int i = 0; i < tr; ++i)
            {
                long child = transition(i);
                if (child == -1)
                    continue;
                run(child);
                go(node);
            }
        }
    }


    private Pair<List<DecoratedKey>, PartitionIndex> generateRandomIndex(int size) throws IOException
    {
        return generateIndex(size, this::generateRandomKey);
    }

    private Pair<List<DecoratedKey>, PartitionIndex> generateSequentialIndex(int size) throws IOException
    {
        return generateIndex(size, new Supplier<DecoratedKey>()
        {
            long i = 0;
            public DecoratedKey get()
            {
                return sequentialKey(i++);
            }
        });
    }

    private Pair<List<DecoratedKey>, PartitionIndex> generateIndex(int size, Supplier<DecoratedKey> generateKey) throws IOException
    {
        File file = File.createTempFile("ColumnTrieReaderTest", "");
        SequentialWriter writer = new SequentialWriter(file, SequentialWriterOption.newBuilder().finishOnClose(true).build());
        List<DecoratedKey> list = Lists.newArrayList();
        try (FileHandle.Builder fhBuilder = new FileHandle.Builder(file.getPath())
                                            .bufferSize(PageAware.PAGE_SIZE)
                                            .withChunkCache(ChunkCache.instance)
                                            .mmapped(DatabaseDescriptor.getIndexAccessMode() == Config.DiskAccessMode.mmap);
             PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, fhBuilder);
        )
        {
            for (int i = 0; i < size; i++)
            {
                DecoratedKey key = generateKey.get();
                list.add(key);
            }
            Collections.sort(list);
            for (int i = 0; i < size; i++)
                builder.addEntry(list.get(i), i);
            builder.complete();
            PartitionIndex summary = PartitionIndex.load(fhBuilder, partitioner, false, Rebufferer.ReaderConstraint.NONE);
            return Pair.create(list, summary);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    DecoratedKey generateRandomKey()
    {
        UUID uuid = UUID.randomUUID();
        return partitioner.decorateKey(ByteBufferUtil.bytes(uuid));
    }

    DecoratedKey generateRandomLengthKey()
    {
        Random rand = ThreadLocalRandom.current();
        int length = nextPowerRandom(rand, 100, 10, 2);     // favor long strings
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < length; ++i)
            s.append(alphabet.charAt(nextPowerRandom(rand, 0, alphabet.length(), 2))); // favor clashes at a

        return partitioner.decorateKey(ByteBufferUtil.bytes(s.toString()));
    }

    int nextPowerRandom(Random rand, int x0, int x1, double power)
    {
        double r = Math.pow(rand.nextDouble(), power);
        return x0 + (int) ((x1 - x0) * r);
    }

    private static final String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    DecoratedKey sequentialKey(long i)
    {
        String s = "";
        for (int j = 50; j >= 0 ; j--)
        {
            int p = (int) Math.pow(10, j);
            int idx = (int) ((j + i) / p);
            s += alphabet.charAt(idx % alphabet.length());
        }
        return partitioner.decorateKey(ByteBufferUtil.bytes(s));
    }
}
