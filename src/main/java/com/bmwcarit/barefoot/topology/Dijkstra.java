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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bmwcarit.barefoot.roadmap.TimeSpeed;
import com.bmwcarit.barefoot.util.Quadruple;
import com.bmwcarit.barefoot.util.Tuple;

/**
 * Dijkstra's algorithm implementation of a {@link Router}. The routing
 * functions use the Dijkstra algorithm for finding shortest paths according to
 * a customizable {@link Cost} function.
 *
 * @param <E>
 *            Implementation of {@link AbstractEdge} in a directed
 *            {@link Graph}.
 * @param <P>
 *            {@link Point} type of positions in the network.
 */
public class Dijkstra<E extends AbstractEdge<E>, P extends Point<E>> implements Router<E, P> {
    private static Logger logger = LoggerFactory.getLogger(Dijkstra.class);

    /**
     * Route mark representation for msmt2.
     */
    class Mark2 extends Quadruple<E, E, Double, Double> implements Comparable<Mark2> {
        private static final long serialVersionUID = 1L;
        private double drivenTime;

        /**
         * Constructor of an entry.
         *
         * @param one
         *            {@link AbstractEdge} defining the route mark.
         * @param two
         *            Predecessor {@link AbstractEdge}.
         * @param three
         *            Cost value to this route mark.
         * @param four
         *            Bounding cost value to this route mark.
         */
        public Mark2(E one, E two, Double three, Double four, Double driven) {
            super(one, two, three, four);
            this.drivenTime = driven;
        }

        @Override
        public int compareTo(Mark2 other) {
            return (this.three() < other.three()) ? -1 : (this.three() > other.three()) ? 1 : 0;
        }
    }

    /**
     * Route mark representation for msmt.
     */
    class Mark1 extends Quadruple<E, E, Double, Double> implements Comparable<Mark1> {
        private static final long serialVersionUID = 1L;

        /**
         * Constructor of an entry.
         *
         * @param one
         *            {@link AbstractEdge} defining the route mark.
         * @param two
         *            Predecessor {@link AbstractEdge}.
         * @param three
         *            Cost value to this route mark.
         * @param four
         *            Bounding cost value to this route mark.
         */
        public Mark1(E one, E two, Double three, Double four) {
            super(one, two, three, four);
        }

        @Override
        public int compareTo(Mark1 other) {
            return (this.three() < other.three()) ? -1 : (this.three() > other.three()) ? 1 : 0;
        }
    }

    @Override
    public Map<P, List<E>> route(P source, Set<P> targets, Cost<E> cost, Cost<E> bound, Double max, Double deltaTime,
            Double maxVelocity) {
        return ssmt2(source, targets, cost, bound, max, deltaTime, maxVelocity);
    }

    private Map<P, List<E>> ssmt2(P source, Set<P> targets, Cost<E> cost, Cost<E> bound, Double max, Double deltaTime,
            Double maxVelocity) {
        Map<P, Tuple<P, List<E>>> map = msmt2(new HashSet<>(Arrays.asList(source)), targets, cost, bound, max,
                deltaTime, maxVelocity);
        Map<P, List<E>> result = new HashMap<>();
        for (Entry<P, Tuple<P, List<E>>> entry : map.entrySet()) {
            result.put(entry.getKey(), entry.getValue() == null ? null : entry.getValue().two());
        }
        return result;
    }

    private Map<P, Tuple<P, List<E>>> msmt2(final Set<P> sources, final Set<P> targets, Cost<E> cost, Cost<E> bound,
            Double max, Double deltaTime, Double maxVelocity) {

        /*
         * Initialize map of edges to target points.
         */
        Map<E, Set<P>> targetEdges = new HashMap<>();
        for (P target : targets) {
            logger.trace("initialize target {} with edge {} and fraction {}", target, target.edge().id(),
                    target.fraction());
            Set<P> targetEdge = targetEdges.get(target.edge());
            if (targetEdge == null) {
                targetEdges.put(target.edge(), new HashSet<>(Arrays.asList(target)));
            } else {
                targetEdge.add(target);
            }
        }

        /*
         * Setup data structures
         */
        PriorityQueue<Mark2> priorities = new PriorityQueue<>();
        Map<E, Mark2> entries = new HashMap<>();
        Map<P, Mark2> finishs = new HashMap<>();
        Map<Mark2, P> reaches = new HashMap<>();
        Map<Mark2, P> starts = new HashMap<>();
        Cost<E> time = (Cost<E>) new TimeSpeed(maxVelocity);

        /*
         * Initialize map of edges with start points
         */
        for (P source : sources) { // initialize sources as start edges
            double startcost = cost.cost(source.edge(), 1 - source.fraction());
            double startbound = bound != null ? bound.cost(source.edge(), 1 - source.fraction()) : 0.0;
            double startDrivenTime = time != null ? time.cost(source.edge(), 1 - source.fraction()) : 0.0;

            logger.trace("init source {} with start edge {} and fraction {} with {} cost", source, source.edge().id(),
                    source.fraction(), startcost);
            Set<P> targetEdgesFromSource = targetEdges.get(source.edge());
            if (targetEdgesFromSource != null) { // start edge reaches target edge
                for (P target : targetEdgesFromSource) {
                    if (target.fraction() < source.fraction()) {
                        continue;
                    }
                    double reachcost = startcost - cost.cost(source.edge(), 1 - target.fraction());
                    double reachbound = bound != null ? startbound - bound.cost(source.edge(), 1 - target.fraction())
                            : 0.0;
                    double reachDrivenTime = time != null
                            ? startDrivenTime - time.cost(source.edge(), 1 - source.fraction())
                            : 0.0;

                    logger.trace("reached target {} with start edge {} from {} to {} with {} cost", target,
                            source.edge().id(), source.fraction(), target.fraction(), reachcost);

                    Mark2 reach = new Mark2(source.edge(), null, reachcost, reachbound, reachDrivenTime);
                    reaches.put(reach, target);
                    starts.put(reach, source);
                    priorities.add(reach);
                }
            }

            Mark2 start = entries.get(source.edge());
            if (start == null) {
                logger.trace("add source {} with start edge {} and fraction {} with {} cost", source,
                        source.edge().id(), source.fraction(), startcost);

                start = new Mark2(source.edge(), null, startcost, startbound, startDrivenTime);
                entries.put(source.edge(), start);
                starts.put(start, source);
                priorities.add(start);
            } else if (startcost < start.three()) {
                logger.trace("update source {} with start edge {} and fraction {} with {} cost", source,
                        source.edge().id(), source.fraction(), startcost);

                start = new Mark2(source.edge(), null, startcost, startbound, startDrivenTime);
                entries.put(source.edge(), start);
                starts.put(start, source);
                priorities.remove(start);
                priorities.add(start);
            }
        }

        /*
         * Dijkstra algorithm.
         */
        while (priorities.size() > 0) {
            Mark2 current = priorities.poll();

            if (targetEdges.isEmpty()) {
                logger.trace("finshed all targets");
                break;
            }

            if (max != null && current.four() > max) {
                logger.trace("reached maximum bound");
                continue;
            }

            if (deltaTime != null && current.drivenTime > deltaTime) {
                logger.trace("reached maximum time");
                continue;
            }

            /*
             * Finish target if reached.
             */
            P targetReaches = reaches.get(current);
            if (targetReaches != null) {
                if (finishs.containsKey(targetReaches)) {
                    continue;
                } else {
                    logger.trace("finished target {} with edge {} and fraction {} with {} cost", targetReaches,
                            current.one(), targetReaches.fraction(), current.three());
                    finishs.put(targetReaches, current);
                    Set<P> edges = targetEdges.get(current.one());
                    edges.remove(targetReaches);
                    if (edges.isEmpty()) {
                        targetEdges.remove(current.one());
                    }
                    continue;
                }
            }

            logger.trace("succeed edge {} with {} cost", current.one().id(), current.three());

            Iterator<E> successors = current.one().successors();

            while (successors.hasNext()) {
                E successor = successors.next();

                double succcost = current.three() + cost.cost(successor);
                double succbound = bound != null ? current.four() + bound.cost(successor) : 0.0;
                double succDrivenTime = time != null ? current.drivenTime + time.cost(successor) : 0.0;

                Set<P> successorTargetEdges = targetEdges.get(successor);
                if (successorTargetEdges != null) { // reach target edge
                    for (P target : successorTargetEdges) {
                        double reachcost = succcost - cost.cost(successor, 1 - target.fraction());
                        double reachbound = bound != null ? succbound - bound.cost(successor, 1 - target.fraction())
                                : 0.0;

                        double reachDrivenTime = time != null
                                ? succDrivenTime - time.cost(successor, 1 - target.fraction())
                                : 0.0;

                        logger.trace("reached target {} with successor edge {} and fraction {} with {} cost", target,
                                successor.id(), target.fraction(), reachcost);

                        Mark2 reach = new Mark2(successor, current.one(), reachcost, reachbound, reachDrivenTime);
                        reaches.put(reach, target);
                        priorities.add(reach);
                    }
                }

                if (!entries.containsKey(successor)) {
                    logger.trace("added successor edge {} with {} cost", successor.id(), succcost);
                    Mark2 mark = new Mark2(successor, current.one(), succcost, succbound, succDrivenTime);

                    entries.put(successor, mark);
                    priorities.add(mark);
                }
            }
        }

        Map<P, Tuple<P, List<E>>> paths = new HashMap<>();

        for (P target : targets) {
            Mark2 iterator = finishs.get(target);
            if (iterator == null) {
                paths.put(target, null);
            } else {
                LinkedList<E> path = new LinkedList<>();
                Mark2 start = null;
                while (iterator != null) {
                    path.addFirst(iterator.one());
                    start = iterator;
                    iterator = iterator.two() != null ? entries.get(iterator.two()) : null;
                }
                paths.put(target, new Tuple<P, List<E>>(starts.get(start), path));
            }
        }

        entries.clear();
        finishs.clear();
        reaches.clear();
        priorities.clear();

        return paths;
    }

}
