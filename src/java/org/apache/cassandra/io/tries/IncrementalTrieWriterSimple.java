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
package org.apache.cassandra.io.tries;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.cassandra.io.util.DataOutputBuffer;

/**
 * Incremental builder of on-disk tries. Takes sorted input.
 *
 * Incremental building is done by maintaining a stack of nodes in progress which follows the path to reach the last
 * added entry. When a new entry is needed, comparison with the previous can tell us how much of the parents stack
 * remains the same. The rest of the stack is complete as no new entry can affect them due to the input sorting.
 * The completed nodes can be written to disk and discarded, keeping only a pointer to their location in the file
 * (this pointer will be discarded too when the parent node is completed). This ensures that a very limited amount of
 * data is kept in memory at all times.
 *
 * Note: This class is currently unused and stands only as form of documentation for {@link IncrementalTrieWriterPageAware}.
 */
public class IncrementalTrieWriterSimple<Value>
extends IncrementalTrieWriterBase<Value, DataOutput, IncrementalTrieWriterSimple.Node<Value>>
implements IncrementalTrieWriter<Value>
{
    private long position = 0;

    public IncrementalTrieWriterSimple(TrieSerializer<Value, DataOutput> serializer, DataOutput dest)
    {
        super(serializer, dest, new Node<>((byte) 0));
    }

    @Override
    protected void complete(Node<Value> node) throws IOException
    {
        long nodePos = position;
        position += write(node, dest, position);
        node.finalizeWithPosition(nodePos);
    }

    @Override
    public PartialTail makePartialRoot() throws IOException
    {
        try (DataOutputBuffer buf = new DataOutputBuffer())
        {
            PTail tail = new PTail();
            tail.cutoff = position;
            tail.count = count;
            long nodePos = position;
            for (Node<Value> node : (Iterable<Node<Value>>) stack::descendingIterator)
            {
                node.filePos = nodePos;
                nodePos += write(node, buf, nodePos);
                // Hacky but works: temporarily write node's position. Will be overwritten when we finalize node.
            }
            tail.tail = buf.trimmedBuffer();
            tail.root = stack.getFirst().filePos;
            return tail;
        }
    }

    private long write(Node<Value> node, DataOutput dest, long nodePosition) throws IOException
    {
        long size = serializer.sizeofNode(node, nodePosition);
        serializer.write(dest, node, nodePosition);
        return size;
    }

    static class Node<Value> extends IncrementalTrieWriterBase.BaseNode<Value, Node<Value>>
    {
        Node(int transition)
        {
            super(transition);
        }

        @Override
        Node<Value> newNode(byte transition)
        {
            return new Node<Value>(transition & 0xFF);
        }

        public long serializedPositionDelta(int i, long nodePosition)
        {
            assert children.get(i).filePos != -1;
            return children.get(i).filePos - nodePosition;
        }

        public long maxPositionDelta(long nodePosition)
        {
            long min = 0;
            for (Node<Value> child : children)
            {
                if (child.filePos != -1)
                    min = Math.min(min, child.filePos - nodePosition);
            }
            return min;
        }
    }
}