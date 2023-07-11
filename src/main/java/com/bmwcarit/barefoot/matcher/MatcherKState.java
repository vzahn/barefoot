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

import org.json.JSONException;
import org.json.JSONObject;

import com.bmwcarit.barefoot.markov.KState;

/**
 * <i>k</i>-State data structure wrapper of {@link KState} for organizing state
 * memory in HMM map matching.
 */
public class MatcherKState extends KState<MatcherCandidate, MatcherTransition, MatcherSample> {

    /**
     * Creates a {@link MatcherKState} object from a JSON representation.
     *
     * @param json
     *            JSON representation of a {@link MatcherKState} object.
     * @param factory
     *            {@link MatcherFactory} for creation of matcher candidates and
     *            transitions.
     * @throws JSONException
     *             thrown on JSON extraction or parsing error.
     */
    public MatcherKState(JSONObject json, MatcherFactory factory) throws JSONException {
        super(json, factory);

    }

    /**
     * Creates an empty {@link MatcherKState} object and sets <i>&kappa;</i> and
     * <i>&tau;</i> parameters.
     *
     * @param k
     *            <i>&kappa;</i> parameter bounds the length of the state sequence
     *            to at most <i>&kappa;+1</i> states, if <i>&kappa; &ge; 0</i>.
     * @param t
     *            <i>&tau;</i> parameter bounds length of the state sequence to
     *            contain only states for the past <i>&tau;</i> milliseconds.
     * 
     * @param m
     *            parameter bounds length of counter to contain only limited
     *            candidates, amount m.
     */
    public MatcherKState(int k, long t, int m) {
        super(k, t, m);
    }

}
