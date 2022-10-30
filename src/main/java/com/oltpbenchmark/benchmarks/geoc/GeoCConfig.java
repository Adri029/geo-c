/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package com.oltpbenchmark.benchmarks.geoc;

import java.text.SimpleDateFormat;

public final class GeoCConfig {

    public final static String[] nameTokens = {"BAR", "OUGHT", "ABLE", "PRI",
            "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

    public final static String terminalPrefix = "Term-";

    public final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public final static int configWhseCount = 1;
    public final static int configItemCount = 100000; // tpc-c std = 100,000
    public final static int configDistPerWhse = 10; // tpc-c std = 10
    // It should be at minimum 1000, because of the customer name generator.
    public final static int configCustPerDist = 1000; // tpc-c std = 3,000; 
    public final static int configIndPerCust = 10; 
    public final static int configWhseSpecificItems = 1000; 

    /**
     * An invalid item id used to rollback a new order transaction.
     */
    public static final int INVALID_ITEM_ID = -12345;
}
