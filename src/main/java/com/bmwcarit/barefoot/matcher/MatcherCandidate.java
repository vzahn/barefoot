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

import java.util.UUID;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bmwcarit.barefoot.roadmap.RoadMap;
import com.bmwcarit.barefoot.roadmap.RoadPoint;

/**
 * Matching candidate for Hidden Markov Model (HMM) map matching representing a
 * position on the map.
 */
public class MatcherCandidate implements Comparable<MatcherCandidate> {
    private final RoadPoint point;
    private final MatcherSample sample;
    private final static Logger logger = LoggerFactory.getLogger(MatcherCandidate.class);

    private final String id;
    private MatcherCandidate predecessor = null;
    private MatcherTransition transition = null;
    private double seqprob = 0d;
    private double filtprob = 0d;
    private long time = 0L;
    private Double distance = 0d;
    private Double deltaHeading = 0d;
    private Double deltaRoute = 0d;
    private boolean uTurn = false;

    /**
     * Creates a matching candidate.
     *
     * @param point
     *            {@link RoadPoint} object that is point on the map represented by
     *            matching candidate.
     */
    public MatcherCandidate(RoadPoint point) {
        this.point = point;
        this.sample = null;
        this.id = UUID.randomUUID().toString();
    }

    /**
     * Creates a matching candidate with heading and velocity entries.
     *
     * @param point
     *            {@link RoadPoint} object that is point on the map represented by
     *            matching candidate.
     */
    public MatcherCandidate(RoadPoint point, MatcherSample sample) {
        this.point = point;
        this.sample = sample;
        this.id = UUID.randomUUID().toString();
    }

    /**
     * Creates a matching candidate from its JSON representation.
     *
     * @param json
     *            JSON representation of matching candidate.
     * @param factory
     *            Matcher factory for creation of matching candidates, transitions
     *            and samples.
     * @param map
     *            {@link RoadMap} object used for creation of matching candidates,
     *            transitions and samples.
     * @throws JSONException
     *             thrown on JSON parse error.
     */
    public MatcherCandidate(JSONObject json, MatcherFactory factory, RoadMap map) throws JSONException {
        id = json.getString("id");
        JSONObject jsontrans = json.optJSONObject("transition");
        if (jsontrans != null) {
            transition = factory.transition(jsontrans);
        }

        // This does not handle infinite values.
        filtprob = json.getDouble("filtprob");
        seqprob = json.getDouble("seqprob");
        time = json.getLong("time");
        point = RoadPoint.fromJSON(json.getJSONObject("roadpoint"), map);
        if (json.has("sample")) {
            sample = factory.sample(json.getJSONObject("sample"));
            // sample = new MatcherSample(json.getJSONObject("sample"));//(Point)
            // GeometryEngine.geometryFromWkt(json.getString("sample"),
            // WktImportFlags.wktImportDefaults,
            // Type.Point);
        } else {
            sample = null;
        }

    }

    /**
     * Gets {@link RoadPoint} as point on the map represented by the matching
     * candidate.
     *
     * @return {@link RoadPoint} object of the matching candidate.
     */
    public RoadPoint point() {
        return point;
    }

    public MatcherSample getSample() {
        return sample;
    }

    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("filtprob", Double.isInfinite(filtprob) ? "Infinity" : filtprob);
        json.put("seqprob", Double.isInfinite(seqprob) ? "-Infinity" : seqprob);
        if (transition != null) {
            json.put("transition", transition.toJSON());
        }
        json.put("time", time);
        json.put("roadpoint", point.toJSON());

        if (sample != null) {
            json.put("sample", sample.toJSON());
        }

        return json;
    }

    @Override
    public String toString() {
        String s = "Edge-RefId: " + this.point().edge().base().refid() + ", fraction:" + this.point.fraction();
        return s;
    }

    /**
     * Tells if the current candidate is more likely.
     * 
     * @param estimate
     *            StateCandiate.
     * @return If the Candidate is more likely then the parameter Candidate..
     */
    public boolean likelier(MatcherCandidate estimate) {
        if (estimate == null || this.seqprob > estimate.seqprob) {
            return true;
        } else if (this.seqprob == estimate.seqprob) {
            logger.trace("Candidate has equal seqprob.");
            MatcherTransition currentBestTransition = (MatcherTransition) estimate.transition;
            MatcherTransition currentTransition = (MatcherTransition) this.transition;
            // Make deterministic decision based on shortest number of roads
            if (currentBestTransition != null && currentTransition != null && currentBestTransition.route() != null
                    && currentTransition.route() != null
                    && currentBestTransition.route().size() != currentTransition.route().size()) {
                if (currentBestTransition.route().size() > currentTransition.route().size()) {
                    logger.trace("Taking new with shorter transition.");
                    return true;
                } else if (currentBestTransition.route().size() < currentTransition.route().size()) {
                    logger.trace("Keeping old with shorter transition.");
                    return false;
                }
            } else {
                // Make deterministic decision based on arbitrary edge-id
                MatcherCandidate currentCandidate = this;
                MatcherCandidate bestCandidate = estimate;
                if (bestCandidate.point().edge().id() <= currentCandidate.point().edge().id()) {
                    logger.trace(
                            "Keeping old, not preferring transition decision: " + bestCandidate.point().edge().id());
                    return false;
                } else {
                    logger.trace(
                            "Taking new, not preferring transition decision: " + currentCandidate.point().edge().id());
                    return true;
                }
            }

        }
        return false;

    }

    /**
     * Gets identifier of state candidate.
     *
     * @return Identifier of state candidate.
     */
    public String id() {
        return id;
    }

    /**
     * Gets predecessor in the most likely sequence to this state candidate. If
     * there is no such sequence it's null.
     *
     * @return Predecessor in the most likely sequence to this state candidate, if
     *         it exists otherwise null.
     */
    public MatcherCandidate predecessor() {
        return predecessor;
    }

    /**
     * Sets predecessor in the most likely sequence to this state candidate.
     *
     * @param predecessor
     *            Most likely predecessor state candidate.
     */
    public void predecessor(MatcherCandidate predecessor) {
        this.predecessor = predecessor;
    }

    /**
     * Gets transition from predecessor, if it exists otherwise null.
     *
     * @return Transition from predecessor, if it exists otherwise null.
     */
    public MatcherTransition transition() {
        return transition;
    }

    /**
     * Sets transition from predecessor, if it exists otherwise null.
     *
     * @param transition
     *            Transition from most likely predecessor state candidate.
     */
    public void transition(MatcherTransition transition) {
        this.transition = transition;
    }

    /**
     * Gets sequence probability of the state candidate (logarithmic scaled with
     * <i>log<sub>10</sub></i>).
     *
     * @return State candidate's sequence probability.
     */
    public double seqprob() {
        return seqprob;
    }

    /**
     * Sets sequence probability of the state candidate (logarithmic scaled with
     * <i>log<sub>10</sub></i>).
     *
     * @param seqprob
     *            Sequence probability
     */
    public void seqprob(double seqprob) {
        this.seqprob = seqprob;
    }

    /**
     * Gets filter probability of the state candidate.
     *
     * @return Filter probability.
     */
    public double filtprob() {
        return filtprob;
    }

    /**
     * Sets filter probability of the state candidate.
     *
     * @param filtprob
     *            Filter probability.
     */
    public void filtprob(double filtprob) {
        this.filtprob = filtprob;
    }

    /**
     * Gets time of sample for state candidate.
     *
     * @return time of point.
     */
    public long time() {
        return time;
    }

    /**
     * Sets time of the state candidate.
     *
     * @param time
     *            of point from sample.
     */
    public void time(long time) {
        this.time = time;
    }

    public Double getDistance() {
        return distance;
    }

    public void setDistance(Double distance) {
        this.distance = distance;
    }

    public Double getDeltaHeading() {
        return deltaHeading;
    }

    public void setDeltaHeading(Double deltaHeading) {
        this.deltaHeading = deltaHeading;
    }

    public Double getDeltaRoute() {
        return deltaRoute;
    }

    public void setDeltaRoute(Double deltaRoute) {
        this.deltaRoute = deltaRoute;
    }

    public boolean isuTurn() {
        return uTurn;
    }

    public void setuTurn(boolean uTurn) {
        this.uTurn = uTurn;
    }

    @Override
    public int compareTo(MatcherCandidate o) {
        // return UUID.fromString(id).compareTo(UUID.fromString(o.id));
        return id.compareTo(o.id);
    }

}
