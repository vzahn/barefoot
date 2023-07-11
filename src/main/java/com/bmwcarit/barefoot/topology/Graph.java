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

package com.bmwcarit.barefoot.topology;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Directed graph providing a basic routing topology to be used by
 * {@link Router} implementations.
 *
 * @param <E>
 *            {@link AbstractEdge} type of the graph.
 */
public class Graph<E extends AbstractEdge<E>> implements Serializable {
    private static final long serialVersionUID = 1L;
    protected final Map<Long, E> edges = new HashMap<>();

    /**
     * Adds an {@link AbstractEdge} to the graph. (Requires construction.)
     *
     * @param edge
     *            Edge to be added.
     * @return Returns a self reference to this graph.
     */
    public Graph<E> add(E edge) {
        edges.put(edge.id(), edge);
        return this;
    }

    /**
     * Gets {@link AbstractEdge} by its identifier.
     *
     * @param id
     *            {@link AbstractEdge}'s identifier.
     * @return {@link AbstractEdge} object if it is contained in the graph,
     *         otherwise returns null.
     */
    public E get(long id) {
        return edges.get(id);
    }

    /**
     * Constructs the graph which means edges are connected for iteration between
     * connections.
     *
     * @return Returns a self reference to this graph.
     */
    public Graph<E> construct() {
        Map<Long, ArrayList<E>> map = new HashMap<>();

        for (E edge : edges.values()) {
            if (!map.containsKey(edge.source())) {
                map.put(edge.source(), new ArrayList<>(Collections.singletonList(edge)));
            } else {
                map.get(edge.source()).add(edge);
            }
        }

        for (ArrayList<E> edges : map.values()) {
            for (int i = 1; i < edges.size(); ++i) {
                edges.get(i - 1).neighbor(edges.get(i));
                ArrayList<E> successors = map.get(edges.get(i - 1).target());
                edges.get(i - 1).successor(successors != null ? successors.get(0) : null);
            }

            edges.get(edges.size() - 1).neighbor(edges.get(0));
            ArrayList<E> successors = map.get(edges.get(edges.size() - 1).target());
            edges.get(edges.size() - 1).successor(successors != null ? successors.get(0) : null);
        }

        return this;
    }

}
