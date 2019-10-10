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

package com.bmwcarit.barefoot.matcher;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bmwcarit.barefoot.markov.Filter;
import com.bmwcarit.barefoot.markov.KState;
import com.bmwcarit.barefoot.road.Heading;
import com.bmwcarit.barefoot.roadmap.Distance;
import com.bmwcarit.barefoot.roadmap.Road;
import com.bmwcarit.barefoot.roadmap.RoadMap;
import com.bmwcarit.barefoot.roadmap.RoadPoint;
import com.bmwcarit.barefoot.roadmap.Route;
import com.bmwcarit.barefoot.spatial.SpatialOperator;
import com.bmwcarit.barefoot.topology.Cost;
import com.bmwcarit.barefoot.topology.Router;
import com.bmwcarit.barefoot.util.Tuple;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.WktExportFlags;

/**
 * Matcher filter for Hidden Markov Model (HMM) map matching. It is a HMM filter
 * (@{link Filter}) and determines emission and transition probabilities for map
 * matching with HMM.
 */
public class Matcher extends Filter<MatcherCandidate, MatcherTransition, MatcherSample> {
    private static final Logger logger = LoggerFactory.getLogger(Matcher.class);

    private final RoadMap map;
    private final Router<Road, RoadPoint> router;
    private final Cost<Road> cost;
    private final SpatialOperator spatial;

    private double sig2 = Math.pow(5d, 2);
    private double sigA = Math.pow(10d, 2);
    private double sqrt2piSigA = Math.sqrt(2d * Math.PI * sigA);
    private double lambda = 0d;
    private double radius = 200;
    private double distance = 15000;
    private double maxVelocity = 1.85;
    private double transitionFactor = 2;
    private double transitionDistance = 800d;
    private double gpsOutageFactor = 0.0d;
    private double uTurnPenalty = 20d;
    private double tunnelPass = 1d;
    private double maxBaseFactor = 1;
    private double laneWidth = 1;
    private double minHeadingVelocityThreshold = 1;
    private boolean useAccuracyForSigma = true;

    /**
     * Creates a HMM map matching filter for some map, router, cost function, and
     * spatial operator.
     *
     * @param map
     *            {@link RoadMap} object of the map to be matched to.
     * @param router
     *            {@link Router} object to be used for route estimation.
     * @param cost
     *            Cost function to be used for routing.
     * @param spatial
     *            Spatial operator for spatial calculations.
     */
    public Matcher(RoadMap map, Router<Road, RoadPoint> router, Cost<Road> cost, SpatialOperator spatial) {
        this.map = map;
        this.router = router;
        this.cost = cost;
        this.spatial = spatial;
    }

    /**
     * Set wether horizontal accuracy shall be taken into account, when calculation
     * emission. Default is true.
     * 
     * @param useAccuracyForSigma
     *            true if accuracy shall be taken into account, else false.
     */
    public void setUseAccuracyForSigma(boolean useAccuracyForSigma) {
        this.useAccuracyForSigma = useAccuracyForSigma;
    }

    /**
     * Gets standard deviation in meters of gaussian distribution that defines
     * emission probabilities.
     *
     * @return Standard deviation in meters of gaussian distribution that defines
     *         emission probabilities.
     */
    public double getSigma() {
        return Math.sqrt(this.sig2);
    }

    /**
     * Sets standard deviation in meters of gaussian distribution for defining
     * emission probabilities (default is 5 meters).
     *
     * @param sigma
     *            Standard deviation in meters of gaussian distribution for defining
     *            emission probabilities (default is 5 meters).
     */
    public void setSigma(double sigma) {
        this.sig2 = Math.pow(sigma, 2);
    }

    /**
     * Gets lambda parameter of negative exponential distribution defining
     * transition probabilities.
     *
     * @return Lambda parameter of negative exponential distribution defining
     *         transition probabilities.
     */
    public double getLambda() {
        return this.lambda;
    }

    /**
     * Sets lambda parameter of negative exponential distribution defining
     * transition probabilities (default is 0.0). It uses adaptive parameterization,
     * if lambda is set to 0.0.
     *
     * @param lambda
     *            Lambda parameter of negative exponential distribution defining
     *            transition probabilities.
     */
    public void setLambda(double lambda) {
        this.lambda = lambda;
    }

    /**
     * Gets maximum radius for candidate selection in meters.
     *
     * @return Maximum radius for candidate selection in meters.
     */
    public double getMaxRadius() {
        return this.radius;
    }

    /**
     * Sets maximum radius for candidate selection in meters (default is 100
     * meters).
     *
     * @param radius
     *            Maximum radius for candidate selection in meters.
     */
    public void setMaxRadius(double radius) {
        this.radius = radius;
    }

    /**
     * Gets maximum transition distance in meters.
     *
     * @return Maximum transition distance in meters.
     */
    public double getMaxDistance() {
        return this.distance;
    }

    /**
     * Sets maximum transition distance in meters (default is 15000 meters).
     *
     * @param distance
     *            Maximum transition distance in meters.
     */
    public void setMaxDistance(double distance) {
        this.distance = distance;
    }

    public double getSigmaA() {
        return Math.sqrt(this.sigA);
    }

    /**
     * Sets sigA and sqrt2piSigA.
     * 
     * @param sigA
     *            sigA
     */
    public void setSigmaA(double sigA) {
        this.sigA = Math.pow(sigA, 2);
        this.sqrt2piSigA = Math.sqrt(2d * Math.PI * sigA);
    }

    public double getMaxVelocity() {
        return maxVelocity;
    }

    public void setMaxVelocity(double maxVelocity) {
        this.maxVelocity = maxVelocity;
    }

    /**
     * Get factor of transition to calculate HHM-break.
     * 
     * @return the transitionFactor double
     */
    public double getTransitionFactor() {
        return transitionFactor;
    }

    /**
     * Great cycle distance x factor shall me smaller than transition.
     * 
     * @param transitionFactor
     *            the transitionFactor to set
     */
    public void setTransitionFactor(double transitionFactor) {
        this.transitionFactor = transitionFactor;
    }

    public double getTransitionDistance() {
        return transitionDistance;
    }

    /**
     * Set max value for transitionfactor, after this value max transition is x
     * sqrt(2).
     * 
     * @param transitionDistance
     *            the transitionDistance to set
     */
    public void setTransitionDistance(double transitionDistance) {
        this.transitionDistance = transitionDistance;
    }

    /**
     * Set GPS-outage factor to multiply transition with, depending on gpsOutage and
     * tunnel transition.
     * 
     * @param gpsOutageFactor
     *            the gpsOutageFactor to set
     */
    public void setGpsOutageFactor(double gpsOutageFactor) {
        this.gpsOutageFactor = gpsOutageFactor;
    }

    /**
     * Get the UTurnPenalty in m for increasing the route for the transition.
     * 
     * @return the uTurnPenalty
     */
    public double getuTurnPenalty() {
        return uTurnPenalty;
    }

    /**
     * Set the UTurnPenalty in m for increasing the route for the transition.
     * 
     * @param uTurnPenalty
     *            the uTurnPenalty to set
     */
    public void setuTurnPenalty(double uTurnPenalty) {
        this.uTurnPenalty = uTurnPenalty;
    }

    /**
     * Get parameter for a "maybeTunnel" , until when a tunnel is a special tunnel.
     * 
     * @return the tunnelPass
     */
    public double getTunnelPass() {
        return tunnelPass;
    }

    /**
     * 
     * Set parameter for a "maybeTunnel" , until when a tunnel is a special tunnel.
     * 
     * @param tunnelPass
     *            the tunnelPass to set
     */
    public void setTunnelPass(double tunnelPass) {
        this.tunnelPass = tunnelPass;
    }

    /**
     * 
     * Get parameter for a the maximum allowed distance based on the spatial
     * distance between two points.
     * 
     * @return the maxBaseFactor
     */
    public double getMaxBaseFactor() {
        return maxBaseFactor;
    }

    /**
     * 
     * Set parameter for a the maximum allowed distance based on the spatial
     * distance between two points.
     * 
     * @param maxBaseFactor
     *            the maxBaseFactor to set
     */
    public void setMaxBaseFactor(double maxBaseFactor) {
        this.maxBaseFactor = maxBaseFactor;
    }

    /**
     * 
     * Get width of a standardized lane in meter.
     * 
     * @return the laneWidth
     */
    public double getLaneWidth() {
        return laneWidth;
    }

    /**
     * 
     * Set parameter with of a standardized lane in meter.
     * 
     * @param laneWidth
     *            the laneWidth to set
     */
    public void setLaneWidth(double laneWidth) {
        this.laneWidth = laneWidth;
    }

    /**
     * 
     * Get threshold for use of heading, as an attribute from velocity of the
     * position in m/s .
     * 
     * @return the minHeadingVelocityThreshold
     */
    public double getMinHeadingVelocityThreshold() {
        return minHeadingVelocityThreshold;
    }

    /**
     * 
     * Set parameter of threshold for heading use, the parameter is the velocity of
     * the position in m/s.
     * 
     * @param minHeadingVelocityThreshold
     *            the minHeadingVelocityThreshold to set
     */
    public void setMinHeadingVelocityThreshold(double minHeadingVelocityThreshold) {
        this.minHeadingVelocityThreshold = minHeadingVelocityThreshold;
    }

    @Override
    protected Set<Tuple<MatcherCandidate, Double>> candidates(Set<MatcherCandidate> predecessors,
            MatcherSample sample) {
        return candidates(predecessors, sample, null);
    }

    @Override
    protected Set<Tuple<MatcherCandidate, Double>> candidates(Set<MatcherCandidate> predecessors, MatcherSample sample,
            Double perimeter) {
        if (logger.isTraceEnabled()) {
            logger.trace("finding candidates for sample {} {}",
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ").format(sample.time()),
                    GeometryEngine.geometryToWkt(sample.point(), WktExportFlags.wktExportPoint));
        }
        if (perimeter != null) {
            radius = perimeter;
        }
        Set<RoadPoint> pointsRadius = map.spatial().radius(sample.point(), radius);
        Set<RoadPoint> points = pointsRadius;
        // Minset is not yet suitable for all situations:
        // Set<RoadPoint> points = new HashSet<>(Minset.removeZeroMeter(pointsRadius));

        Map<Long, RoadPoint> map = new HashMap<>();
        for (RoadPoint point : points) {
            map.put(point.edge().id(), point);
        }

        for (MatcherCandidate predecessor : predecessors) {
            RoadPoint point = map.get(predecessor.point().edge().id());
            if (point != null && point.edge() != null
                    && spatial.distance(point.geometry(), predecessor.point().geometry()) < getSigma()
                    && ((point.edge().heading() == Heading.forward && point.fraction() < predecessor.point().fraction())
                            || (point.edge().heading() == Heading.backward
                                    && point.fraction() > predecessor.point().fraction()))) {
                points.remove(point);
                points.add(predecessor.point());
            }
        }
        Set<Tuple<MatcherCandidate, Double>> candidates = new HashSet<>();
        logger.debug("{} candidates", points.size());
        for (RoadPoint point : points) {
            MatcherCandidate candidate = new MatcherCandidate(point, sample);
            // a lane is ~ 3m, the deviation is 1.5m
            double dz = Math.max(0d, spatial.distance(sample.point(), point.geometry()) - laneWidth / 2);
            double sigma2 = sig2;
            double sqrt2piSig2 = Math.sqrt(2d * Math.PI * sigma2);
            if (useAccuracyForSigma && !Double.isNaN(sample.getAccuracy())) {
                sigma2 = Math.pow((sample.getAccuracy() + getSigma()) / 2d, 2);
                sqrt2piSig2 = Math.sqrt(2d * Math.PI * sigma2);
            }

            double emission = 1 / sqrt2piSig2 * Math.exp((-1) * dz * dz / (2 * sigma2));

            if (!Double.isNaN(sample.azimuth()) && !Double.isNaN(sample.getVelocity())
                    && sample.getVelocity() >= minHeadingVelocityThreshold) {
                double da = sample.azimuth() > point.azimuth()
                        ? Math.min(sample.azimuth() - point.azimuth(), 360 - (sample.azimuth() - point.azimuth()))
                        : Math.min(point.azimuth() - sample.azimuth(), 360 - (point.azimuth() - sample.azimuth()));

                emission = (1 / sqrt2piSig2 * 1 / sqrt2piSigA)
                        * Math.exp((-1) * dz * dz / (2 * sigma2) + (-1) * da * da / (2 * sigA));
                candidate.setDeltaHeading(da);
                candidate.setDistance(dz);
                if (logger.isTraceEnabled()) {
                    logger.trace("{} diffHeading: {} emission: {}",
                            ((MatcherCandidate) candidate).point().edge().base().refid(), da,
                            1 / sqrt2piSigA * Math.exp((-1) * da * da / (2 * sigA)));
                }

            }

            candidates.add(new Tuple<>(candidate, emission));
            if (logger.isTraceEnabled()) {
                logger.trace("{} diffDistance: {} emission: {}",
                        ((MatcherCandidate) candidate).point().edge().base().refid(), dz,
                        1 / sqrt2piSig2 * Math.exp((-1) * dz * dz / (2 * sigma2)));
                logger.trace("{} -> total emission: {}", ((MatcherCandidate) candidate).point().edge().base().refid(),
                        emission);
            }
        }
        return candidates;
    }

    @Override
    protected Tuple<MatcherTransition, Double> transition(Tuple<MatcherSample, MatcherCandidate> predecessor,
            Tuple<MatcherSample, MatcherCandidate> candidate) {

        return null;
    }

    @Override
    protected Map<MatcherCandidate, Map<MatcherCandidate, Tuple<MatcherTransition, Double>>> transitions(
            final Tuple<MatcherSample, Set<MatcherCandidate>> predecessors,
            final Tuple<MatcherSample, Set<MatcherCandidate>> candidates) {

        if (logger.isTraceEnabled()) {
            logger.trace("finding transitions for sample {} {} with {} x {} candidates",
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ").format(candidates.one().time()),
                    GeometryEngine.geometryToWkt(candidates.one().point(), WktExportFlags.wktExportPoint),
                    predecessors.two().size(), candidates.two().size());
        }

        final Set<RoadPoint> targets = new HashSet<>();
        for (MatcherCandidate candidate : candidates.two()) {
            targets.add(candidate.point());
        }

        final Map<MatcherCandidate, Map<MatcherCandidate, Tuple<MatcherTransition, Double>>> transitions = new ConcurrentHashMap<>();
        final double base = 1.0 * spatial.distance(predecessors.one().point(), candidates.one().point());
        final double bound = distance;
        final double deltaTime = (candidates.one().time() - predecessors.one().time()) / 1000;
        final double maxOverSpeed = maxVelocity;

        for (final MatcherCandidate predecessor : predecessors.two()) {
            Map<RoadPoint, List<Road>> routes = router.route(predecessor.point(), targets, cost, new Distance(), bound,
                    deltaTime, maxOverSpeed);

            transitions.put(predecessor, addTransitions(candidates, predecessor, base, routes, predecessors.one()));
        }

        return transitions;
    }

    protected Map<MatcherCandidate, Tuple<MatcherTransition, Double>> addTransitions(
            Tuple<MatcherSample, Set<MatcherCandidate>> candidates, MatcherCandidate predecessor, double base,
            Map<RoadPoint, List<Road>> routes, MatcherSample matcherSample) {
        Map<MatcherCandidate, Tuple<MatcherTransition, Double>> map = new HashMap<>();
        double roundingFraction = 0.0001d;
        for (MatcherCandidate candidate : candidates.two()) {
            List<Road> edges = routes.get(candidate.point());
            if (edges == null) {
                continue;
            }
            Route route = new Route(predecessor.point(), candidate.point(), edges);
            Route routeForCostFunction = route;
            boolean isUTurn = false;
            int edgeSize = edges.size();
            if (edgeSize >= 2) {
                if (edges.get(0).base().id() == edges.get(1).base().id() && edges.get(0).id() != edges.get(1).id()) {
                    RoadPoint start = predecessor.point(), end = candidate.point();
                    // Here, additional cost of 5 meters are added to the route
                    // length in order to penalize and avoid turns, e.g., at the
                    // end of a trace.
                    isUTurn = true;
                    if (edgeSize > 2) {
                        // When start is forward add 1mm from the driven length,
                        // so on the backward it has to drive 1mm more
                        // When start is backward add 1mm to the driven length
                        // so, when driving forward is has to drive 1mm more.
                        // forward and backward are driving from 0 -> 1 fraction
                        // 1mm is added due to rounding issues with double

                        start = new RoadPoint(edges.get(1),
                                Math.max(0d, 1 - (start.fraction() + (roundingFraction / edges.get(1).length()))));

                        edges.remove(0);

                    } else {
                        if (start.fraction() < 1 - end.fraction()) {
                            end = new RoadPoint(edges.get(0),
                                    Math.min(1d, 1 - end.fraction() + (roundingFraction / edges.get(0).length())));
                            edges.remove(1);
                        } else {
                            start = new RoadPoint(edges.get(1),
                                    Math.max(0d, 1 - start.fraction() - (roundingFraction / edges.get(1).length())));
                            edges.remove(0);
                        }
                    }
                    routeForCostFunction = new Route(start, end, edges);

                } else if (edgeSize > 2 && edges.get(edgeSize - 1).base().id() == edges.get(edgeSize - 2).base().id()
                        && edges.get(edgeSize - 1).id() != edges.get(edgeSize - 2).id()) {
                    // If a UTurn occur at the end of the route
                    isUTurn = true;
                }
            }

            // According to Newson and Krumm 2009, transition probability is
            // lambda * Math.exp((-1.0) * lambda * Math.abs(dt - route.length())),
            // however, we experimentally choose
            // lambda * Math.exp((-1.0) * lambda * Math.max(0, route.length() - dt))
            // to avoid unnecessary routes in case of u-turns.
            double beta = lambda == 0 ? (Math.max(1d, candidates.one().time() - matcherSample.time()) / 1000)
                    : 1 / lambda;
            double routeCost = routeForCostFunction.cost(cost);
            double distanceRoute = base;

            double transition = 0;
            if (isUTurn) {
                transition = (1 / beta) * Math.exp((-1.0) * (Math.abs((routeCost - base)) + uTurnPenalty) / beta);
            } else {
                transition = (1 / beta) * Math.exp((-1.0) * (Math.abs((routeCost - base))) / beta);
            }

            // Weighting GPS re-gain
            boolean candidateGpsOutage = candidate.getSample().isGpsOutage();
            boolean predecessorGpsOutage = predecessor.getSample().isGpsOutage();
            double tunnelLength = routeForCostFunction.tunnelLength();
            boolean routeHasTunnel = 0d < tunnelLength;
            double transitionPenalty = 1;
            // Route is no tunnel or tunnel length is above "maybe-tunnel" limit
            boolean isMaybeTunnel = 2.5 * tunnelPass >= routeCost && tunnelLength <= tunnelPass;
            if ((!routeHasTunnel || !isMaybeTunnel)
                    // Re-gain and partial tunnel
                    && (candidateGpsOutage && tunnelLength < routeCost
                            // Or no re-gain and tunnel
                            || (!candidateGpsOutage && routeHasTunnel))
                    // If predecessor is no re-gain or predecessor and candidate are re-gain signals
                    && (!predecessorGpsOutage || predecessorGpsOutage && candidateGpsOutage)) {
                // punish mismatch between sample and map information
                double tunnelPenaltyRoute = 0d;

                if (routeHasTunnel) {
                    if (candidateGpsOutage) {
                        // Punish non-tunnel parts
                        tunnelPenaltyRoute = routeCost - tunnelLength;
                    } else {
                        // Punish tunnel parts
                        tunnelPenaltyRoute = tunnelLength;
                    }
                } else {
                    tunnelPenaltyRoute = base;
                }

                transitionPenalty = Math.max((1 / beta) //
                        * Math.exp((-1.0) * (tunnelPenaltyRoute * gpsOutageFactor) / beta),
                        Double.MIN_VALUE / transition);

            } // else leave transition as is, without punishing

            // If routeCost is longer than 2 x base then discard transition, except route
            // includes tunnel
            if (routeHasTunnel) {
                routeCost = routeCost - tunnelLength;
            }
            if ((routeCost > distanceRoute * transitionFactor
                    || (distanceRoute > transitionDistance && routeCost > distanceRoute * Math.sqrt(2)))
                    && (distanceRoute > 35d || routeCost > base * maxBaseFactor)) {
                transition = 0;
            }

            double orgTransition = transition;

            transition = transition * transitionPenalty;
            if (logger.isTraceEnabled()) {
                String addition = isUTurn ? "with UTurn Penalty " + uTurnPenalty : "";
                String penalty = transitionPenalty != 1
                        ? "original transition " + orgTransition + " transition penalty " + transitionPenalty
                        : "";

                logger.trace("{} -> {} base: {} routeCost: {} " + addition + penalty + " transition: {}",
                        ((MatcherCandidate) predecessor).point().edge().base().refid(),
                        ((MatcherCandidate) candidate).point().edge().base().refid(), base, routeCost, transition);
            }
            candidate.setDeltaRoute(Math.abs((routeForCostFunction.length() - base)));
            map.put(candidate, new Tuple<>(new MatcherTransition(route), transition));

        }
        return map;

    }

    /**
     * Matches a full sequence of samples, {@link MatcherSample} objects and returns
     * state representation of the full matching which is a {@link KState} object.
     *
     * @param samples
     *            Sequence of samples, {@link MatcherSample} objects.
     * @param minDistance
     *            Minimum distance in meters between subsequent samples as criterion
     *            to match a sample. (Avoids unnecessary matching where samples are
     *            more dense than necessary.)
     * @param minInterval
     *            Minimum time interval in milliseconds between subsequent samples
     *            as criterion to match a sample. (Avoids unnecessary matching where
     *            samples are more dense than necessary.)
     * @return State representation of the full matching which is a {@link KState}
     *         object.
     */
    public MatcherKState mmatch(List<MatcherSample> samples, double minDistance, int minInterval) {
        Collections.sort(samples, new Comparator<MatcherSample>() {
            @Override
            public int compare(MatcherSample left, MatcherSample right) {
                return (int) (left.time() - right.time());
            }
        });

        MatcherKState state = new MatcherKState();

        for (MatcherSample sample : samples) {
            if (state.sample() != null
                    && (spatial.distance(sample.point(), state.sample().point()) < Math.max(0, minDistance)
                            || (sample.time() - state.sample().time()) < Math.max(0, minInterval))) {
                continue;
            }
            Set<MatcherCandidate> vector = execute(state.vector(), state.sample(), sample);
            state.update(vector, sample);
        }

        return state;
    }

}
