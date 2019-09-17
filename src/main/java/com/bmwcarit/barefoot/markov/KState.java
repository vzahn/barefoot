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

package com.bmwcarit.barefoot.markov;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.bmwcarit.barefoot.util.Tuple;

/**
 * <i>k</i>-State data structure for organizing state memory in HMM inference.
 *
 * @param <C>
 *            Candidate inherits from {@link StateCandidate}.
 * @param <T>
 *            Transition inherits from {@link StateTransition}.
 * @param <S>
 *            Sample inherits from {@link Sample}.
 */
public class KState<C extends StateCandidate<C, T, S>, T extends StateTransition, S extends Sample>
        extends StateMemory<C, T, S> {
    private final static Logger logger = LoggerFactory.getLogger(Filter.class);
    private final int k;
    private final long t;
    private final int maxCounters;
    private final LinkedList<Tuple<Set<C>, S>> sequence;
    private final Map<C, Integer> counters;
    private List<C> candidateStorage;

    /**
     * Creates empty {@link KState} object with default parameters, i.e. capacity is
     * unbounded.
     */
    public KState() {
        this.maxCounters = Integer.MAX_VALUE;
        this.k = -1;
        this.t = -1;
        this.sequence = new LinkedList<>();
        this.counters = new HashMap<>();
        this.candidateStorage = new ArrayList<>();
    }

    /**
     * Creates a {@link KState} object from a JSON representation.
     *
     * @param json
     *            JSON representation of a {@link KState} object.
     * @param factory
     *            Factory for creation of state candidates and transitions.
     *
     * @throws JSONException
     *             thrown on JSON extraction or parsing error.
     */
    public KState(JSONObject json, Factory<C, T, S> factory) throws JSONException {
        this.maxCounters = json.getInt("m");
        this.k = json.getInt("k");
        this.t = json.getLong("t");
        this.sequence = new LinkedList<>();
        this.counters = new HashMap<>();
        this.candidateStorage = new ArrayList<>();

        Map<String, C> candidates = new HashMap<>();
        JSONArray jsoncandidates = json.getJSONArray("candidates");
        for (int i = 0; i < jsoncandidates.length(); ++i) {
            JSONObject jsoncandidate = jsoncandidates.getJSONObject(i);
            C candidate = factory.candidate(jsoncandidate.getJSONObject("candidate"));
            int count = jsoncandidate.getInt("count");

            counters.put(candidate, count);
            candidates.put(candidate.id(), candidate);
        }
        JSONArray jsoncandidatestorage = json.getJSONArray("candidatestorage");
        for (int i = 0; i < jsoncandidatestorage.length(); ++i) {
            JSONObject jsoncandidate = jsoncandidatestorage.getJSONObject(i);
            C candidate = factory.candidate(jsoncandidate.getJSONObject("candidate"));
            candidateStorage.add(candidate);
        }

        JSONArray jsonsequence = json.getJSONArray("sequence");
        for (int i = 0; i < jsonsequence.length(); ++i) {
            JSONObject jsonseqelement = jsonsequence.getJSONObject(i);
            Set<C> vector = new HashSet<>();
            JSONArray jsonvector = jsonseqelement.getJSONArray("vector");
            for (int j = 0; j < jsonvector.length(); ++j) {
                JSONObject jsonelement = jsonvector.getJSONObject(j);

                String candid = jsonelement.getString("candid");
                String predid = jsonelement.getString("predid");

                C candidate = candidates.get(candid);
                C pred = candidates.get(predid);

                if (candidate == null || (!predid.isEmpty() && pred == null)) {
                    throw new JSONException("inconsistent JSON of KState object");
                }

                candidate.predecessor(pred);
                vector.add(candidate);
            }

            S sample = factory.sample(jsonseqelement.getJSONObject("sample"));

            sequence.add(new Tuple<>(vector, sample));
        }

        Collections.sort(sequence, new Comparator<Tuple<Set<C>, S>>() {
            @Override
            public int compare(Tuple<Set<C>, S> left, Tuple<Set<C>, S> right) {
                if (left.two().time() < right.two().time()) {
                    return -1;
                } else if (left.two().time() > right.two().time()) {
                    return +1;
                }
                return 0;
            }
        });
    }

    /**
     * Creates an empty {@link KState} object and sets <i>&kappa;</i> and
     * <i>&tau;</i> parameters.
     *
     * @param k
     *            <i>&kappa;</i> parameter bounds the length of the state sequence
     *            to at most <i>&kappa;+1</i> states, if <i>&kappa; &ge; 0</i>.
     * @param t
     *            <i>&tau;</i> parameter bounds length of the state sequence to
     *            contain only states for the past <i>&tau;</i> milliseconds.
     * 
     * 
     * @param m
     *            parameter bounds length of counter to contain only limited
     *            candidates, amount m.
     * 
     */
    public KState(int k, long t, int m) {
        this.k = k;
        this.t = t;
        this.maxCounters = m;
        this.sequence = new LinkedList<>();
        this.counters = new HashMap<>();
        this.candidateStorage = new ArrayList<>();
    }

    /**
     * Sets the candidate store and activates truncation of the matching sequence
     * 
     * @param List<C>
     *            ArrayList<C> for the candidate storage
     */
    public void setCandidateStorage(List<C> candidateStorage) {
        this.candidateStorage = candidateStorage;
    }

    @Override
    public boolean isEmpty() {
        return counters.isEmpty();
    }

    @Override
    public int size() {
        return counters.size();
    }

    @Override
    public Long time() {
        if (sequence.isEmpty()) {
            return null;
        } else {
            return sequence.peekLast().two().time();
        }
    }

    @Override
    public S sample() {
        if (sequence.isEmpty()) {
            return null;
        } else {
            return sequence.peekLast().two();
        }
    }

    /**
     * Gets the sequence of measurements <i>z<sub>0</sub>, z<sub>1</sub>, ...,
     * z<sub>t</sub></i>.
     *
     * @return List with the sequence of measurements.
     */
    public List<S> samples() {
        LinkedList<S> samples = new LinkedList<>();
        for (Tuple<Set<C>, S> element : sequence) {
            samples.add(element.two());
        }
        return samples;
    }

    @Override
    public void update(Set<C> vector, S sample) {
        if (vector.isEmpty()) {
            return;
        }

        if (!sequence.isEmpty() && sequence.peekLast().two().time() >= sample.time()) {
            throw new RuntimeException("out-of-order state update is prohibited. Last Time: "
                    + sequence.peekLast().two().time() + " , sample time: " + sample.time());
        }

        List<C> test = vector.stream().collect(Collectors.toList());
        for (C candidate : test) {
            counters.put(candidate, 0);
            if (candidate.predecessor() != null) {
                if (!counters.containsKey(candidate.predecessor())
                        || !sequence.peekLast().one().contains(candidate.predecessor())) {
                    throw new RuntimeException("Inconsistent update vector.");
                }
                counters.put(candidate.predecessor(), counters.get(candidate.predecessor()) + 1);
            }
        }

        if (!sequence.isEmpty()) {
            Set<C> deletes = new HashSet<>();
            C estimate = null;

            for (C candidate : sequence.peekLast().one()) {
                if (estimate == null || candidate.seqprob() > estimate.seqprob()) {
                    estimate = candidate;
                }
                if (counters.get(candidate) == 0) {
                    logger.debug("remove Candidate:" + candidate.toString());
                    deletes.add(candidate);
                }
            }

            int size = sequence.peekLast().one().size();

            for (C candidate : deletes) {
                if (deletes.size() != size || candidate != estimate) {
                    remove(candidate, sequence.size() - 1);
                }
            }
        }

        sequence.add(new Tuple<>(vector, sample));

        // move stable candidate to the candidate storage, if the storage is set
        if (candidateStorage != null) {

            // find the first n sequence elements with an vector length of 1,
            // means they are stable
            int stableCount = 0;
            for (Tuple<Set<C>, S> t : sequence) {
                if (t.one().size() > 1) {
                    break;
                }
                stableCount++;
            }

            // delete all but one stable candidates (the last is kept as an fix
            // point for further matching
            for (int i = 0; i < stableCount - 1; i++) {
                Set<C> deletes = sequence.removeFirst().one();
                for (C candidate : deletes) {
                    candidateStorage.add(candidate);
                    if (candidate.transition() != null) {
                        logger.debug("stable candidate  {} ({}, {}, route: {})",
                                ((MatcherCandidate) candidate).point().edge().base().refid(), candidate.filtprob(),
                                candidate.seqprob(), ((MatcherCandidate) candidate).transition().toString());
                    } else {
                        logger.debug("stable candidate - -> {} ({}, {})",
                                ((MatcherCandidate) candidate).point().edge().base().refid(), candidate.filtprob(),
                                candidate.seqprob());
                    }
                    counters.remove(candidate);

                }

                for (C candidate : sequence.peekFirst().one()) {
                    candidate.predecessor(null);

                }
            }
        }

        if (counters.size() > maxCounters) {
            /*
             * deletes old counters when size is reached.
             */
            int deletedCounters = 0;
            Set<C> allKeys = counters.keySet();
            Set<C> removableCounters = new HashSet<>();
            for (C candidate : allKeys) {
                if (t > 0 && t < sample.time() - candidate.time()) {
                    removableCounters.add(candidate);
                    deletedCounters++;
                }
            }
            for (C removeable : removableCounters) {
                counters.remove(removeable);
            }

            logger.warn("Size of candidates: " + maxCounters + " reached. Deleted " + deletedCounters
                    + " counters with time older than " + t + " in ms");
        }
        if (!sequence.isEmpty()) {
            // deletion of candidates,that are to old (t) or sequence is to long
            // (k)

            int deletedCounters = 0;
            while ((t > 0 && !sequence.isEmpty() && sample.time() - sequence.peekFirst().two().time() > t)
                    || (k >= 0 && sequence.size() > k + 1)) {
                Set<C> deletes = sequence.removeFirst().one();
                for (C candidate : deletes) {
                    if (counters.remove(candidate) != null) {
                        deletedCounters++;
                    }
                }

                for (C candidate : sequence.peekFirst().one()) {
                    candidate.predecessor(null);
                }
            }
            if (deletedCounters > 0) {
                logger.warn(
                        "Size of previous candidates: " + k + " reached. Deleted " + deletedCounters + " counters.");
            }

        }
        assert (k < 0 || sequence.size() <= k + 1);

    }

    protected void remove(C candidate, int index) {
        Set<C> vector = sequence.get(index).one();
        counters.remove(candidate);
        vector.remove(candidate);

        C predecessor = candidate.predecessor();
        if (predecessor != null) {
            counters.put(predecessor, counters.get(predecessor) - 1);
            if (counters.get(predecessor) == 0) {
                remove(predecessor, index - 1);
            }
        }
    }

    @Override
    public Set<C> vector() {
        if (sequence.isEmpty()) {
            return new HashSet<>();
        } else {
            return sequence.peekLast().one();
        }
    }

    @Override
    public C estimate() {
        if (sequence.isEmpty()) {
            return null;
        }

        C estimate = null;
        for (C candidate : sequence.peekLast().one()) {
            if (estimate == null || candidate.filtprob() > estimate.filtprob()) {
                estimate = candidate;
            }
        }
        return estimate;
    }

    /**
     * Gets the most likely sequence of state candidates <i>s<sub>0</sub>,
     * s<sub>1</sub>, ..., s<sub>t</sub></i>.
     *
     * @return List of the most likely sequence of state candidates.
     */
    public List<C> sequence() {
        if (sequence.isEmpty()) {
            return null;
        }

        C kestimate = null;

        for (C candidate : sequence.peekLast().one()) {
            if (kestimate == null || candidate.seqprob() > kestimate.seqprob()) {
                kestimate = candidate;
            }
        }

        LinkedList<C> ksequence = new LinkedList<>();

        for (int i = sequence.size() - 1; i >= 0; --i) {
            if (kestimate != null) {
                ksequence.push(kestimate);
                kestimate = kestimate.predecessor();
            } else {
                ksequence.push(sequence.get(i).one().iterator().next());
                assert (sequence.get(i).one().size() == 1);
            }
        }

        return ksequence;
    }

    /**
     * Gets the stable candidates <i>s<sub>0</sub>, s<sub>1</sub>, ...,
     * s<sub>t</sub></i>.
     *
     * @return List of stable candidates.
     */
    public Set<C> candidates() {
        if (counters.isEmpty()) {
            return null;
        }

        return counters.keySet();
    }

    /**
     * Gets the candidateStorage <i>s<sub>0</sub>, s<sub>1</sub>, ...,
     * s<sub>t</sub></i>.
     *
     * @return List of stored candidates.
     */
    public List<C> candidateStorage() {
        if (candidateStorage.isEmpty()) {
            return null;
        }
        return candidateStorage;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();
        JSONArray jsonsequence = new JSONArray();
        for (Tuple<Set<C>, S> element : sequence) {
            JSONObject jsonseqelement = new JSONObject();
            JSONArray jsonvector = new JSONArray();
            for (C candidate : element.one()) {
                JSONObject jsoncandidate = new JSONObject();
                jsoncandidate.put("candid", candidate.id());
                jsoncandidate.put("predid", candidate.predecessor() == null ? "" : candidate.predecessor().id());
                jsonvector.put(jsoncandidate);
            }
            jsonseqelement.put("vector", jsonvector);
            jsonseqelement.put("sample", element.two().toJSON());
            jsonsequence.put(jsonseqelement);
        }

        JSONArray jsoncandidates = new JSONArray();
        for (Entry<C, Integer> entry : counters.entrySet()) {
            JSONObject jsoncandidate = new JSONObject();
            jsoncandidate.put("candidate", entry.getKey().toJSON());
            jsoncandidate.put("count", entry.getValue());
            jsoncandidates.put(jsoncandidate);
        }
        JSONArray jsoncandidatestorage = new JSONArray();
        /*
         * 
         */
        int maxCandidateStorage = 10;
        if (candidateStorage.size() < maxCandidateStorage) {
            maxCandidateStorage = candidateStorage.size();
        }

        for (int i = 0; i < maxCandidateStorage; i++) {
            JSONObject jsoncandidate = new JSONObject();
            C candidate = candidateStorage.get(i);
            jsoncandidate.put("candidate", candidate.toJSON());
            jsoncandidatestorage.put(jsoncandidate);
        }
        json.put("k", k);
        json.put("t", t);
        json.put("m", maxCounters);
        json.put("sequence", jsonsequence);
        json.put("candidates", jsoncandidates);
        json.put("candidatestorage", jsoncandidatestorage);

        return json;
    }

    /**
     * Returns the candidate list and the samples.
     * 
     * @return the sequence.
     */
    public LinkedList<Tuple<Set<C>, S>> getSequence() {
        return sequence;
    }

    /**
     * Returns the candidate data.
     * 
     * @return the counters.
     */
    public Map<C, Integer> getCounters() {
        return counters;
    }

}
