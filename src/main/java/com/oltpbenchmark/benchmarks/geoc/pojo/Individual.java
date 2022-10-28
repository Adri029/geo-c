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

import java.util.Objects;

public class Individual {

    public int _ind_id;
    public int _ind_c_id;
    public int _ind_d_id;
    public int _ind_w_id;
    public String _ind_name;

    @Override
    public String toString() {
        return ("\n***************** Individual ********************"
                + "\n*           _ind_id = "
                + _ind_id
                + "\n*         _ind_c_id = "
                + _ind_c_id
                + "\n*         _ind_d_id = "
                + _ind_d_id
                + "\n*         _ind_w_id = "
                + _ind_w_id
                + "\n*         _ind_name = "
                + _ind_name
                + "\n**********************************************");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Individual individual = (Individual) o;
        return _ind_c_id == individual._ind_c_id && _ind_d_id == individual._ind_d_id
                && _ind_w_id == individual._ind_w_id && _ind_name == individual._ind_name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(_ind_id, _ind_c_id, _ind_d_id, _ind_w_id, _ind_name);
    }
}
