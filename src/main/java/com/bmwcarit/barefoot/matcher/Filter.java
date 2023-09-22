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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bmwcarit.barefoot.util.Tuple;

/**
 * Hidden Markov Model (HMM) filter for inference of states in a stochastic
 * process.
 */
public abstract class Filter {
    private final static Logger logger = LoggerFactory.getLogger(Filter.class);

    /**
     * Gets state vector, which is a set of {@link MatcherCandidate} objects and
     * with its emission probability.
     *
     * @param predecessors
     *            Predecessor state candidate <i>s<sub>t-1</sub></i>.
     * @param sample
     *            Measurement sample.
     * 
     * @param radius
     *            SearchRadius for candidates.
     * @return Set of tuples consisting of a {@link MatcherCandidate} and its
     *         emission probability.
     */
    protected abstract Set<Tuple<MatcherCandidate, Double>> candidates(Set<MatcherCandidate> predecessors,
            MatcherSample sample, Double radius);

    /**
     * Gets transitions and its transition probabilities for each pair of state
     * candidates <i>s<sub>t</sub></i> and <i>s<sub>t-1</sub></i>.
     *
     * @param predecessors
     *            Tuple of a set of predecessor state candidate
     *            <i>s<sub>t-1</sub></i> and its respective measurement sample.
     * @param candidates
     *            Tuple of a set of state candidate <i>s<sub>t</sub></i> and its
     *            respective measurement sample.
     * @return Maps each predecessor state candidate <i>s<sub>t-1</sub> &#8712;
     *         S<sub>t-1</sub></i> to a map of state candidates <i>s<sub>t</sub>
     *         &#8712; S<sub>t</sub></i> containing all transitions from
     *         <i>s<sub>t-1</sub></i> to <i>s<sub>t</sub></i> and its transition
     *         probability, or null if there no transition.
     */
    protected abstract Map<MatcherCandidate, Map<MatcherCandidate, Tuple<MatcherTransition, Double>>> transitions(
            Tuple<MatcherSample, Set<MatcherCandidate>> predecessors,
            Tuple<MatcherSample, Set<MatcherCandidate>> candidates);

    /**
     * Executes Hidden Markov Model (HMM) filter iteration that determines for a
     * given measurement sample <i>z<sub>t</sub></i>, which is a
     * {@link MatcherSample} object, and of a predecessor state vector
     * <i>S<sub>t-1</sub></i>, which is a set of {@link MatcherCandidate} objects, a
     * state vector <i>S<sub>t</sub></i> with filter and sequence probabilities set.
     * <p>
     * <b>Note:</b> The set of state candidates <i>S<sub>t-1</sub></i> is allowed to
     * be empty. This is either the initial case or an HMM break occured, which is
     * no state candidates representing the measurement sample could be found.
     *
     * @param predecessors
     *            State vector <i>S<sub>t-1</sub></i>, which may be empty.
     * @param sample
     *            Measurement sample <i>z<sub>t</sub></i>.
     * @param previous
     *            Previous measurement sample <i>z<sub>t-1</sub></i>.
     *
     * @return State vector <i>S<sub>t</sub></i>, which may be empty if an HMM break
     *         occured.
     */
    public Set<MatcherCandidate> execute(Set<MatcherCandidate> predecessors, MatcherSample previous,
            MatcherSample sample, Double radius) {
        if (logger.isTraceEnabled()) {
            try {
                logger.trace("execute sample {}", sample.toJSON());
            } catch (JSONException e) {
                logger.trace("execute sample (not JSON parsable sample: {})", e.getMessage());
            }
        }

        assert (predecessors != null);
        assert (sample != null);

        Set<MatcherCandidate> result = new LinkedHashSet<>();
        Set<Tuple<MatcherCandidate, Double>> candidates = candidates(predecessors, sample, radius);
        logger.trace("{} state candidates", candidates.size());

        double normsum = 0;

        if (!predecessors.isEmpty()) {
            Set<MatcherCandidate> states = new LinkedHashSet<>();
            for (Tuple<MatcherCandidate, Double> candidate : candidates) {
                states.add(candidate.one());
            }
            Map<MatcherCandidate, Map<MatcherCandidate, Tuple<MatcherTransition, Double>>> transitions = transitions(
                    new Tuple<>(previous, predecessors), new Tuple<>(sample, states));

            for (Tuple<MatcherCandidate, Double> candidate : candidates) {
                MatcherCandidate candidateOne = candidate.one();
                candidateOne.seqprob(Double.NEGATIVE_INFINITY);
                if (logger.isTraceEnabled()) {
                    try {
                        logger.trace("state candidate {} ({}) {}",
                                ((MatcherCandidate) candidateOne).point().edge().base().refid(), candidate.two(),
                                candidateOne.toJSON().toString());
                    } catch (JSONException e) {
                        logger.trace("state candidate (not JSON parsable candidate: {})", e.getMessage());
                    }
                }
                MatcherCandidate previousPredecessor = null;
                for (MatcherCandidate predecessor : predecessors) {
                    Tuple<MatcherTransition, Double> transition = transitions.get(predecessor).get(candidateOne);
                    if (transition == null || transition.two() == 0) {
                        continue;
                    }

                    candidateOne.filtprob(candidateOne.filtprob() + (transition.two() * predecessor.filtprob()));
                    double seqprob = predecessor.seqprob() + Math.log10(transition.two()) + Math.log10(candidate.two());
                    if (logger.isTraceEnabled()) {
                        try {
                            logger.trace(
                                    "state transition {} -> {} (seqprob: {}, transitionlog10: {}, emissionlog10: {}) {}",
                                    ((MatcherCandidate) predecessor).point().edge().base().refid(),
                                    ((MatcherCandidate) candidate.one()).point().edge().base().refid(),
                                    predecessor.seqprob(), Math.log10(transition.two()), Math.log10(candidate.two()),
                                    transition.one().toJSON().toString());
                        } catch (JSONException e) {
                            logger.trace("state transition (not JSON parsable transition: {})", e.getMessage());
                        } catch (NullPointerException npe) {
                            logger.trace("can't trace details, as some attributes were null {}", npe.getMessage());
                        }
                    }
                    if (seqprob > candidateOne.seqprob()) {
                        previousPredecessor = modifyCandidate(candidateOne, predecessor, transition.one(), seqprob);
                    } else if (seqprob == candidateOne.seqprob()) {
                        logger.trace("Candidate has equal seqprob.");
                        MatcherTransition currentBestTransition = (MatcherTransition) candidateOne.transition();
                        MatcherTransition currentTransition = (MatcherTransition) transition.one();
                        // Make deterministic decision based on shortest number of roads
                        if (currentBestTransition != null && currentTransition != null
                                && currentBestTransition.route() != null && currentTransition.route() != null
                                && currentBestTransition.route().size() != currentTransition.route().size()) {
                            if (currentBestTransition.route().size() > currentTransition.route().size()) {
                                logger.trace("Taking new with shorter transition.");
                                previousPredecessor = modifyCandidate(candidateOne, predecessor, transition.one(),
                                        seqprob);
                            } else if (currentBestTransition.route().size() < currentTransition.route().size()) {
                                logger.trace("Keeping old with shorter transition.");
                            }
                        } else {
                            // Make deterministic decision based on arbitrary edge-id
                            MatcherCandidate mcPre = (MatcherCandidate) predecessor;
                            MatcherCandidate mcPrePre = (MatcherCandidate) previousPredecessor;
                            if (mcPrePre != null && mcPrePre.point().edge().id() <= mcPre.point().edge().id()) {
                                logger.trace("Keeping old, not preferring transition decision: "
                                        + mcPrePre.point().edge().id());
                            } else {
                                logger.trace(
                                        "Taking new, not preferring transition decision: " + mcPre.point().edge().id());
                                previousPredecessor = modifyCandidate(candidateOne, predecessor, transition.one(),
                                        seqprob);
                            }
                        }

                    }
                }

                if (candidateOne.predecessor() != null) {
                    logger.debug("state candidate {} -> {} ({}, {}, route: {})",
                            ((MatcherCandidate) candidateOne.predecessor()).point().edge().base().refid(),
                            ((MatcherCandidate) candidateOne).point().edge().base().refid(), candidateOne.filtprob(),
                            candidateOne.seqprob(), ((MatcherCandidate) candidateOne).transition().toString());

                    logger.trace("state candidate {} -> {} ({}, {})",
                            ((MatcherCandidate) candidateOne.predecessor()).point().edge().base().refid(),
                            ((MatcherCandidate) candidateOne).point().edge().base().refid(), candidateOne.filtprob(),
                            candidateOne.seqprob());
                } else {
                    logger.trace("state candidate - -> {} ({}, {})",
                            ((MatcherCandidate) candidateOne).point().edge().base().refid(), candidateOne.filtprob(),
                            candidateOne.seqprob());
                }

                if (Double.isNaN(candidateOne.filtprob()) || candidateOne.filtprob() == 0) {
                    continue;
                }
                candidateOne.time(sample.time());
                candidateOne.filtprob(candidateOne.filtprob() * candidate.two());
                result.add(candidateOne);

                normsum += candidateOne.filtprob();
            }
        }

        if (!candidates.isEmpty() && result.isEmpty() && !predecessors.isEmpty()) {
            logger.info("HMM break - no state transitions for sample " + ((MatcherSample) sample).toString());
        }

        if (result.isEmpty() || predecessors.isEmpty()) {
            for (Tuple<MatcherCandidate, Double> candidate : candidates) {
                if (candidate.two() == 0) {
                    continue;
                }
                MatcherCandidate candidateOne = candidate.one();
                normsum += candidate.two();
                candidateOne.filtprob(candidate.two());
                candidateOne.seqprob(Math.log10(candidate.two()));
                candidateOne.time(sample.time());
                result.add(candidateOne);

                if (logger.isTraceEnabled()) {
                    try {
                        logger.trace("state candidate {} ({}) {}",
                                ((MatcherCandidate) candidateOne).point().edge().base().refid(), candidate.two(),
                                candidateOne.toJSON().toString());
                    } catch (JSONException e) {
                        logger.trace("state candidate (not JSON parsable candidate: {})", e.getMessage());
                    }
                }
            }
        }

        if (result.isEmpty()) {
            logger.info("HMM break - no state emissions" + ((MatcherSample) sample).toString());
        }

        for (MatcherCandidate candidate : result) {
            /*
             * Change candidate to prob to 0, if normsum of all candidates is 0, NaN cannot
             * be transfered to json
             */
            if (Double.isNaN(candidate.filtprob() / normsum) || Double.isNaN(normsum)) {
                candidate.filtprob(0.0);
            } else {
                candidate.filtprob(candidate.filtprob() / normsum);
            }

        }

        logger.trace("{} state candidates for state update", result.size());
        return result;
    }

    /**
     * Sets all given attributes for candidate and returns predecessor for
     * convenient calling.
     * 
     * @param candidate
     * @param predecessor
     * @param transition
     * @param seqprob
     */
    private MatcherCandidate modifyCandidate(MatcherCandidate candidate, MatcherCandidate predecessor,
            MatcherTransition transition, double seqprob) {
        candidate.predecessor(predecessor);
        candidate.transition(transition);
        candidate.seqprob(seqprob);
        return predecessor;
    }

}
