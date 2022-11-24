/*
 * Copyright 2022 by Adriano Maior and Rui Ribeiro
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

package com.oltpbenchmark.benchmarks.geoc.pojo;

import java.sql.Timestamp;

public class ShoppingCartLine {
    public int _scl_c_id;
    public int _scl_d_id;
    public int _scl_w_id;
    public int _scl_i_id;
    public int _scl_supply_w_id;
    public Timestamp _scl_delivery_d;
    public int _scl_quantity;
    public float _scl_amount;
    public String _scl_dist_info;
}
