/*
 * Copyright (C) 2015, BMW Car IT GmbH
 *
 * Author: Sebastian Mattheis <sebastian.mattheis@bmw-carit.de>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package com.bmwcarit.barefoot.spatial;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import com.bmwcarit.barefoot.util.Triple;
import com.bmwcarit.barefoot.util.Tuple;
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry.Type;
import com.esri.core.geometry.OperatorExportToWkb;
import com.esri.core.geometry.OperatorImportFromWkb;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.QuadTree.QuadTreeIterator;
import com.esri.core.geometry.WkbExportFlags;
import com.esri.core.geometry.WkbImportFlags;

/**
 * Quad-tree index implementation of {@link SpatialIndex} to store polylines
 * ({@link Polyline}).
 */
public class QuadTreeIndex implements SpatialIndex<Tuple<Long, Double>>, Serializable {
    private static final long serialVersionUID = 1L;
    private final SpatialOperator spatial;
    private final static int height = 16;
    private QuadTree index = null;
    private final Map<Long, byte[]> geometries;
    private final Map<Integer, Long> indexToGimId;
    private final Envelope2D envelope;

    /**
     * Creates a {@link QuadTreeIndex} with default bounding box of spatially
     * indexed region and uses {@link SpatialOperator} implementation
     * {@link Geography}.
     */
    public QuadTreeIndex() {
        spatial = new Geography();
        envelope = new Envelope2D();
        envelope.setCoords(-180, -90, 180, 90);
        index = new QuadTree(envelope, height);
        geometries = new HashMap<>();
        indexToGimId = new HashMap<>();
    }

    /**
     * Creates a {@link QuadTreeIndex}.
     *
     * @param envelope
     *            Bounding box of spatially indexed region.
     * @param spatial
     *            {@link SpatialOperator} for spatial operations.
     */
    public QuadTreeIndex(Envelope2D envelope, SpatialOperator spatial) {
        this.spatial = spatial;
        this.envelope = envelope;
        index = new QuadTree(envelope, height);
        geometries = new HashMap<>();
        indexToGimId = new HashMap<>();
    }

    /**
     * Adds a copy of a {@link Polyline} in WKB format to spatial index with some
     * reference identifier.
     * <p>
     * <b>Note:</b> To store only references to geometry objects provide geometries
     * in WKB format use {@link QuadTreeIndex#add(int, byte[])}.
     *
     * @param id
     *            Identifier reference for polyline.
     * @param polyline
     *            {@link Polyline} object of geometry.
     * @param indexId
     *            Pointer for integer for tiles.
     * 
     */
    public void add(long id, Polyline polyline, int indexId) {
        Envelope2D env = new Envelope2D();
        polyline.queryEnvelope2D(env);
        indexToGimId.put(indexId, id);
        index.insert(indexId, env);

        ByteBuffer wkb = OperatorExportToWkb.local().execute(WkbExportFlags.wkbExportLineString, polyline, null);
        geometries.put(id, wkb.array());
    }

    /**
     * Adds a polyline ({@link Polyline}) in WKB format to spatial index with some
     * reference identifier.
     *
     * @param id
     *            Identifier reference for polyline.
     * @param wkb
     *            {@link ByteBuffer} object of geometry in WKB format.
     * @param indexId
     *            Pointer for integer for tiles.
     */
    public void add(long id, byte[] wkb, int indexId) {
        Polyline geometry = (Polyline) OperatorImportFromWkb.local().execute(WkbImportFlags.wkbImportDefaults,
                Type.Polyline, ByteBuffer.wrap(wkb), null);

        Envelope2D env = new Envelope2D();
        geometry.queryEnvelope2D(env);
        indexToGimId.put(indexId, id);
        index.insert(indexId, env);
        geometries.put(id, wkb);
    }

    /**
     * Clears {@link QuadTreeIndex} and removes all data.
     */
    public void clear() {
        index = new QuadTree(envelope, height);
        geometries.clear();
        indexToGimId.clear();
    }

    /**
     * Checks if reference identifier is contained.
     *
     * @param geoId
     *            Reference identifier.
     * @return True if it is contained, false otherwise.
     */
    public boolean contains(long geoId) {
        return geometries.containsKey(geoId);
    }

    @Override
    public Set<Tuple<Long, Double>> nearest(Point c) {
        if (index.getElementCount() == 0) {
            return null;
        }

        Set<Tuple<Long, Double>> nearests = new HashSet<>();
        double radius = 100, min = Double.MAX_VALUE;

        do {
            Envelope2D env = spatial.envelope(c, radius);

            QuadTreeIterator it = index.getIterator(env, 0);
            int handle = -1;

            while ((handle = it.next()) != -1) {
                int id = index.getElement(handle);
                long geoId = indexToGimId.get(id);
                Polyline geometry = (Polyline) OperatorImportFromWkb.local().execute(WkbImportFlags.wkbImportDefaults,
                        Type.Polyline, ByteBuffer.wrap(geometries.get(geoId)), null);

                double f = spatial.intercept(geometry, c);
                Point p = spatial.interpolate(geometry, spatial.length(geometry), f);
                double d = spatial.distance(p, c);

                if (d > min) {
                    continue;
                }

                if (d < min) {
                    min = d;
                    nearests.clear();
                }

                nearests.add(new Tuple<>(geoId, f));
            }

            radius *= 2;

        } while (nearests.isEmpty());

        return nearests;
    }

    @Override
    public Set<Tuple<Long, Double>> radius(Point c, double radius) {
        Set<Tuple<Long, Double>> neighbors = new HashSet<>();

        Envelope2D env = spatial.envelope(c, radius);

        QuadTreeIterator it = index.getIterator(env, 0);
        int handle = -1;

        while ((handle = it.next()) != -1) {
            int id = index.getElement(handle);
            long geoId = indexToGimId.get(id);
            Polyline geometry = (Polyline) OperatorImportFromWkb.local().execute(WkbImportFlags.wkbImportDefaults,
                    Type.Polyline, ByteBuffer.wrap(geometries.get(geoId)), null);

            double f = spatial.intercept(geometry, c);
            Point p = spatial.interpolate(geometry, spatial.length(geometry), f);
            double d = spatial.distance(p, c);

            if (d < radius) {
                neighbors.add(new Tuple<>(geoId, f));
            }
        }

        return neighbors;
    }

    @Override
    public Set<Tuple<Long, Double>> knearest(Point c, int k) {
        if (index.getElementCount() == 0) {
            return null;
        }

        Set<Long> visited = new HashSet<>();

        PriorityQueue<Triple<Long, Double, Double>> queue = new PriorityQueue<>(k,
                new Comparator<Triple<Long, Double, Double>>() {
                    @Override
                    public int compare(Triple<Long, Double, Double> left, Triple<Long, Double, Double> right) {
                        return left.three() < right.three() ? -1 : left.three() > right.three() ? +1 : 0;
                    }
                });

        double radius = 100;

        do {
            Envelope2D env = spatial.envelope(c, radius);

            QuadTreeIterator it = index.getIterator(env, 0);
            int handle = -1;

            while ((handle = it.next()) != -1) {
                int id = index.getElement(handle);
                long geoId = indexToGimId.get(id);

                if (visited.contains(geoId)) {
                    continue;
                }

                Polyline geometry = (Polyline) OperatorImportFromWkb.local().execute(WkbImportFlags.wkbImportDefaults,
                        Type.Polyline, ByteBuffer.wrap(geometries.get(geoId)), null);

                double f = spatial.intercept(geometry, c);
                Point p = spatial.interpolate(geometry, spatial.length(geometry), f);
                double d = spatial.distance(p, c);

                if (d < radius) { // Only within radius, we can be sure that we have semantically
                                  // correct k-nearest neighbors.
                    queue.add(new Triple<>(geoId, f, d));
                    visited.add(geoId);
                }
            }

            radius *= 2;

        } while (queue.size() < k);

        Set<Tuple<Long, Double>> result = new HashSet<>();

        while (result.size() < k) {
            Triple<Long, Double, Double> e = queue.poll();
            result.add(new Tuple<>(e.one(), e.two()));
        }

        return result;
    }
}
