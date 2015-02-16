/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.newHashMap;
import static java.lang.System.arraycopy;
import static java.util.Arrays.binarySearch;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.plugins.segment.ListRecord.LEVEL_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ID_BYTES;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.SMALL_LIMIT;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentWriter.BLOCK_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Template.MANY_CHILD_NODES;
import static org.apache.jackrabbit.oak.plugins.segment.Template.ZERO_CHILD_NODES;

import java.util.Formatter;
import java.util.Map;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This utility breaks down space usage per record type.
 * It accounts for value sharing. That is, an instance
 * of this class will remember which records it has seen
 * already and not count those again. Only the effective
 * space taken by the records is taken into account. Slack
 * space from aligning records is not accounted for.
 */
public class RecordUsageAnalyser {
    private long mapSize;       // leaf and branch
    private long listSize;      // list and bucket
    private long valueSize;     // inlined values
    private long templateSize;  // template
    private long nodeSize;      // node

    public long getMapSize() {
        return mapSize;
    }

    public long getListSize() {
        return listSize;
    }

    public long getValueSize() {
        return valueSize;
    }

    public long getTemplateSize() {
        return templateSize;
    }

    public long getNodeSize() {
        return nodeSize;
    }

    public void analyseNode(RecordId nodeId) {
        if (notSeen(nodeId)) {
            Segment segment = nodeId.getSegment();
            int offset = nodeId.getOffset();
            RecordId templateId = segment.readRecordId(offset);
            analyseTemplate(templateId);

            Template template = segment.readTemplate(templateId);

            // Recurses into child nodes in this segment
            if (template.getChildName() == MANY_CHILD_NODES) {
                RecordId childMapId = segment.readRecordId(offset + RECORD_ID_BYTES);
                MapRecord childMap = segment.readMap(childMapId);
                analyseMap(childMapId, childMap);
                for (ChildNodeEntry childNodeEntry : childMap.getEntries()) {
                    NodeState child = childNodeEntry.getNodeState();
                    if (child instanceof SegmentNodeState) {
                        RecordId childId = ((SegmentNodeState) child).getRecordId();
                        analyseNode(childId);
                    }
                }
            } else if (template.getChildName() != ZERO_CHILD_NODES) {
                RecordId childId = segment.readRecordId(offset + RECORD_ID_BYTES);
                analyseNode(childId);
            }

            // Recurse into properties
            int ids = template.getChildName() == ZERO_CHILD_NODES ? 1 : 2;
            nodeSize += ids * RECORD_ID_BYTES;
            PropertyTemplate[] propertyTemplates = template.getPropertyTemplates();
            for (PropertyTemplate propertyTemplate : propertyTemplates) {
                nodeSize += RECORD_ID_BYTES;
                RecordId propertyId = segment.readRecordId(offset + ids++ * RECORD_ID_BYTES);
                analyseProperty(propertyId, propertyTemplate);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Formatter formatter = new Formatter(sb);
        formatter.format(
                "%s in maps (leaf and branch records)%n",
                byteCountToDisplaySize(mapSize));
        formatter.format(
                "%s in lists (list and bucket records)%n",
                byteCountToDisplaySize(listSize));
        formatter.format(
                "%s in values (value and block records)%n",
                byteCountToDisplaySize(valueSize));
        formatter.format(
                "%s in templates (template records)%n",
                byteCountToDisplaySize(templateSize));
        formatter.format(
                "%s in nodes (node records)%n",
                byteCountToDisplaySize(nodeSize));
        return sb.toString();
    }

    private void analyseTemplate(RecordId templateId) {
        if (notSeen(templateId)) {
            Segment segment = templateId.getSegment();
            int size = 0;
            int offset = templateId.getOffset();
            int head = segment.readInt(offset + size);
            boolean hasPrimaryType = (head & (1 << 31)) != 0;
            boolean hasMixinTypes = (head & (1 << 30)) != 0;
            boolean zeroChildNodes = (head & (1 << 29)) != 0;
            boolean manyChildNodes = (head & (1 << 28)) != 0;
            int mixinCount = (head >> 18) & ((1 << 10) - 1);
            int propertyCount = head & ((1 << 18) - 1);
            size += 4;

            if (hasPrimaryType) {
                RecordId primaryId = segment.readRecordId(offset + size);
                analyseString(primaryId);
                size += Segment.RECORD_ID_BYTES;
            }

            if (hasMixinTypes) {
                for (int i = 0; i < mixinCount; i++) {
                    RecordId mixinId = segment.readRecordId(offset + size);
                    analyseString(mixinId);
                    size += Segment.RECORD_ID_BYTES;
                }
            }

            if (!zeroChildNodes && !manyChildNodes) {
                RecordId childNameId = segment.readRecordId(offset + size);
                analyseString(childNameId);
                size += Segment.RECORD_ID_BYTES;
            }

            for (int i = 0; i < propertyCount; i++) {
                RecordId propertyNameId = segment.readRecordId(offset + size);
                size += Segment.RECORD_ID_BYTES;
                size++;  // type
                analyseString(propertyNameId);
            }
            templateSize += size;
        }
    }

    private void analyseMap(RecordId mapId, MapRecord map) {
        if (notSeen(mapId)) {
            if (map.isDiff()) {
                analyseDiff(mapId, map);
            } else if (map.isLeaf()) {
                analyseLeaf(map);
            } else {
                analyseBranch(map);
            }
        }
    }

    private void analyseDiff(RecordId mapId, MapRecord map) {
        mapSize += 4;                                // -1
        mapSize += 4;                                // hash of changed key
        mapSize += RECORD_ID_BYTES;                  // key
        mapSize += RECORD_ID_BYTES;                  // value
        mapSize += RECORD_ID_BYTES;                  // base

        RecordId baseId = mapId.getSegment().readRecordId(
                mapId.getOffset() + 8 + 2 * RECORD_ID_BYTES);
        analyseMap(baseId, new MapRecord(baseId));
    }

    private void analyseLeaf(MapRecord map) {
        mapSize += 4;                                 // size
        mapSize += map.size() * 4;                    // key hashes

        for (MapEntry entry : map.getEntries()) {
            mapSize += 2 * RECORD_ID_BYTES;           // key value pairs
            analyseString(entry.getKey());
        }
    }

    private void analyseBranch(MapRecord map) {
        mapSize += 4;                                 // level/size
        mapSize += 4;                                 // bitmap
        for (MapRecord bucket : map.getBuckets()) {
            if (bucket != null) {
                mapSize += RECORD_ID_BYTES;
                analyseMap(bucket.getRecordId(), bucket);
            }
        }
    }

    private void analyseProperty(RecordId propertyId, PropertyTemplate template) {
        if (!contains(propertyId)) {
            Segment segment = propertyId.getSegment();
            int offset = propertyId.getOffset();
            Type<?> type = template.getType();

            if (type.isArray()) {
                notSeen(propertyId);
                int size = segment.readInt(offset);
                valueSize += 4;

                if (size > 0) {
                    RecordId listId = segment.readRecordId(offset + 4);
                    valueSize += RECORD_ID_BYTES;
                    for (RecordId valueId : new ListRecord(listId, size).getEntries()) {
                        analyseValue(valueId, type.getBaseType());
                    }
                    analyseList(listId, size);
                }
            } else {
                analyseValue(propertyId, type);
            }
        }
    }

    private void analyseValue(RecordId valueId, Type<?> type) {
        checkArgument(!type.isArray());
        if (type == BINARY) {
            analyseBlob(valueId);
        } else {
            analyseString(valueId);
        }
    }

    private void analyseBlob(RecordId blobId) {
        if (notSeen(blobId)) {
            Segment segment = blobId.getSegment();
            int offset = blobId.getOffset();
            byte head = segment.readByte(offset);
            if ((head & 0x80) == 0x00) {
                // 0xxx xxxx: small value
                valueSize += (1 + head);
            } else if ((head & 0xc0) == 0x80) {
                // 10xx xxxx: medium value
                int length = (segment.readShort(offset) & 0x3fff) + SMALL_LIMIT;
                valueSize += (2 + length);
            } else if ((head & 0xe0) == 0xc0) {
                // 110x xxxx: long value
                long length = (segment.readLong(offset) & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
                int size = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
                RecordId listId = segment.readRecordId(offset + 8);
                analyseList(listId, size);
                valueSize += (8 + RECORD_ID_BYTES + length);
            } else if ((head & 0xf0) == 0xe0) {
                // 1110 xxxx: external value
                int length = (head & 0x0f) << 8 | (segment.readByte(offset + 1) & 0xff);
                valueSize += (2 + length);
            } else {
                throw new IllegalStateException(String.format(
                        "Unexpected value record type: %02x", head & 0xff));
            }
        }
    }

    private void analyseString(RecordId stringId) {
        if (notSeen(stringId)) {
            Segment segment = stringId.getSegment();
            int offset = stringId.getOffset();

            long length = segment.readLength(offset);
            if (length < Segment.SMALL_LIMIT) {
                valueSize += (1 + length);
            } else if (length < Segment.MEDIUM_LIMIT) {
                valueSize += (2 + length);
            } else if (length < Integer.MAX_VALUE) {
                int size = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
                RecordId listId = segment.readRecordId(offset + 8);
                analyseList(listId, size);
                valueSize += (8 + RECORD_ID_BYTES + length);
            } else {
                throw new IllegalStateException("String is too long: " + length);
            }
        }
    }

    private void analyseList(RecordId listId, int size) {
        if (notSeen(listId)) {
            listSize += noOfListSlots(size) * RECORD_ID_BYTES;
        }
    }

    private static int noOfListSlots(int size) {
        if (size <= LEVEL_SIZE) {
            return size;
        } else {
            int fullBuckets = size / LEVEL_SIZE;
            if (size % LEVEL_SIZE > 1) {
                return size + noOfListSlots(fullBuckets + 1);
            } else {
                return size + noOfListSlots(fullBuckets);
            }
        }
    }

    private final Map<String, ShortSet> seenIds = newHashMap();

    private boolean notSeen(RecordId id) {
        String segmentId = id.getSegmentId().toString();
        ShortSet offsets = seenIds.get(segmentId);
        if (offsets == null) {
            offsets = new ShortSet();
            seenIds.put(segmentId, offsets);
        }
        return offsets.add(crop(id.getOffset()));
    }

    private boolean contains(RecordId id) {
        String segmentId = id.getSegmentId().toString();
        ShortSet offsets = seenIds.get(segmentId);
        return offsets != null && offsets.contains(crop(id.getOffset()));
    }

    private static short crop(int value) {
        return (short) (value >> Segment.RECORD_ALIGN_BITS);
    }

    static class ShortSet {
        short[] elements;

        boolean add(short n) {
            if (elements == null) {
                elements = new short[1];
                elements[0] = n;
                return true;
            } else {
                int k = binarySearch(elements, n);
                if (k < 0) {
                    int l = -k - 1;
                    short[] e = new short[elements.length + 1];
                    arraycopy(elements, 0, e, 0, l);
                    e[l] = n;
                    int c = elements.length - l;
                    if (c > 0) {
                        arraycopy(elements, l, e, l + 1, c);
                    }
                    elements = e;
                    return true;
                } else {
                    return false;
                }
            }
        }

        boolean contains(short n) {
            return elements != null && binarySearch(elements, n) >= 0;
        }
    }

}
