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

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.AbstractSSTableIterator;
import org.apache.cassandra.io.sstable.format.trieindex.RowIndexReader.IndexInfo;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.Rebufferer;

/**
 *  A Cell Iterator over SSTable
 */
class SSTableIterator extends AbstractSSTableIterator
{
    /**
     * The index of the slice being processed.
     */
    private int slice;

    public SSTableIterator(TrieIndexSSTableReader sstable,
                           FileDataInput file,
                           DecoratedKey key,
                           RowIndexEntry indexEntry,
                           Slices slices,
                           ColumnFilter columns,
                           Rebufferer.ReaderConstraint readerConstraint)
    {
        super(sstable, file, key, indexEntry, slices, columns, readerConstraint);
    }

    protected Reader createReaderInternal(RowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile, Rebufferer.ReaderConstraint rc)
    {
        return indexEntry.isIndexed()
             ? new ForwardIndexedReader(indexEntry, file, shouldCloseFile, rc)
             : new ForwardReader(file, shouldCloseFile);
    }

    protected int nextSliceIndex()
    {
        int next = slice;
        slice++;
        return next;
    }

    protected int currentSliceIndex()
    {
        assert slice > 0 : slice;
        return slice - 1;
    }

    protected boolean hasMoreSlices()
    {
        return slice < slices.size();
    }

    public boolean isReverseOrder()
    {
        return false;
    }

    private class ForwardReader extends Reader
    {
        // The start of the current slice. This will be null as soon as we know we've passed that bound.
        protected ClusteringBound start;
        // The end of the current slice. Will never be null.
        protected ClusteringBound end = ClusteringBound.TOP;

        protected Unfiltered next; // the next element to return: this is computed by hasNextInternal().

        protected boolean sliceDone; // set to true once we know we have no more result for the slice. This is in particular
                                     // used by the indexed reader when we know we can't have results based on the index.

        private ForwardReader(FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
        }

        public void setForSlice(Slice slice) throws IOException
        {
            start = slice.start() == ClusteringBound.BOTTOM ? null : slice.start();
            end = slice.end();

            sliceDone = false;
            next = null;
        }

        // Skip all data that comes before the currently set slice.
        // Return what should be returned at the end of this, or null if nothing should.
        private Unfiltered handlePreSliceData() throws IOException
        {
            assert deserializer != null;

            // Note that the following comparison is not strict. The reason is that the only cases
            // where it can be == is if the "next" is a RT start marker (either a '[' of a ')[' boundary),
            // and if we had a strict inequality and an open RT marker before this, we would issue
            // the open marker first, and then return then next later, which would send in the
            // stream both '[' (or '(') and then ')[' for the same clustering value, which is wrong.
            // By using a non-strict inequality, we avoid that problem (if we do get ')[' for the same
            // clustering value than the slice, we'll simply record it in 'openMarker').
            while (deserializer.hasNext() && deserializer.compareNextTo(start) <= 0)
            {
                if (deserializer.nextIsRow())
                    deserializer.skipNext();
                else
                    updateOpenMarker((RangeTombstoneMarker)deserializer.readNext());
            }

            ClusteringBound sliceStart = start;
            start = null;

            // We've reached the beginning of our queried slice. If we have an open marker
            // we should return that first.
            if (openMarker != null)
                return new RangeTombstoneBoundMarker(sliceStart, openMarker);

            return null;
        }

        // Compute the next element to return, assuming we're in the middle to the slice
        // and the next element is either in the slice, or just after it. Returns null
        // if we're done with the slice.
        protected Unfiltered computeNext() throws IOException
        {
            assert deserializer != null;

            while (true)
            {
                // We use a same reasoning as in handlePreSliceData regarding the strictness of the inequality below.
                // We want to exclude deserialized unfiltered equal to end, because 1) we won't miss any rows since those
                // woudn't be equal to a slice bound and 2) a end bound can be equal to a start bound
                // (EXCL_END(x) == INCL_START(x) for instance) and in that case we don't want to return start bound because
                // it's fundamentally excluded. And if the bound is a  end (for a range tombstone), it means it's exactly
                // our slice end, but in that  case we will properly close the range tombstone anyway as part of our "close
                // an open marker" code in hasNextInterna
                if (!deserializer.hasNext() || deserializer.compareNextTo(end) >= 0)
                    return null;

                Unfiltered next = deserializer.readNext();
                // We may get empty row for the same reason expressed on UnfilteredSerializer.deserializeOne.
                if (next.isEmpty())
                    continue;

                if (next.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
                    updateOpenMarker((RangeTombstoneMarker) next);
                return next;
            }
        }

        protected boolean hasNextInternal() throws IOException
        {
            if (next != null)
                return true;

            if (sliceDone)
                return false;

            if (start != null)
            {
                Unfiltered unfiltered = handlePreSliceData();
                if (unfiltered != null)
                {
                    next = unfiltered;
                    return true;
                }
            }

            next = computeNext();
            if (next != null)
                return true;

            // If we have an open marker, we should close it before finishing
            if (openMarker != null)
            {
                next = new RangeTombstoneBoundMarker(end, getAndClearOpenMarker());
                return true;
            }

            sliceDone = true; // not absolutely necessary but accurate and cheap
            return false;
        }

        protected Unfiltered nextInternal() throws IOException
        {
            if (!hasNextInternal())
                throw new NoSuchElementException();

            Unfiltered toReturn = next;
            next = null;
            return toReturn;
        }
    }

    private class ForwardIndexedReader extends ForwardReader
    {
        private final RowIndexReader indexReader;
        long basePosition;

        private ForwardIndexedReader(RowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile, Rebufferer.ReaderConstraint rc)
        {
            super(file, shouldCloseFile);
            basePosition = indexEntry.position;
            indexReader = new RowIndexReader(((TrieIndexSSTableReader) sstable).rowIndexFile, indexEntry, rc);
        }

        @Override
        public void close() throws IOException
        {
            indexReader.close();
            super.close();
        }

        @Override
        public void setForSlice(Slice slice) throws IOException
        {
            super.setForSlice(slice);
            IndexInfo indexInfo = indexReader.separatorFloor(metadata.comparator.asByteComparableSource(slice.start()));
            assert indexInfo != null;
            long position = basePosition + indexInfo.offset;
            if (file == null || position > file.getFilePointer())
            {
                openMarker = indexInfo.openDeletion;
                seekToPosition(position);
            }
            // Otherwise we are already in the relevant index block, there is no point to go back to its beginning.
        }
    }
}
