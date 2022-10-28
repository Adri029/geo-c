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

package com.oltpbenchmark.benchmarks.geoc.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.geoc.GeoCConstants;
import com.oltpbenchmark.benchmarks.geoc.GeoCUtil;
import com.oltpbenchmark.benchmarks.geoc.GeoCWorker;
import com.oltpbenchmark.benchmarks.geoc.pojo.ShoppingCartLine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

public class CheckCart extends GeoCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(CheckCart.class);

    public SQLStmt cartGetShoppingCartLinesSQL = new SQLStmt(
            "SELECT _scl_c_id, _scl_d_id, _scl_w_id, _scl_number, _scl_i_id, _scl_supply_w_id," +
                    "_scl_delivery_d, _scl_quantity, _scl_amount, _scl_dist_info" +
                    " FROM " + GeoCConstants.TABLENAME_SHOPPING_CART_LINE +
                    " WHERE _SCL_W_ID = ? AND _SCL_D_ID = ? AND _SCL_C_ID = ?");

    public void run(Connection conn, Random gen, int w_id, int numWarehouses, int terminalDistrictLowerID,
            int terminalDistrictUpperID, GeoCWorker w) throws SQLException {

        int d_id = GeoCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int c_id = GeoCUtil.getCustomerID(gen);

        var cartLines = getShoppingCartLines(conn, w_id, d_id, c_id);

        if (LOG.isTraceEnabled()) {
            String terminalMessage = "\n+-------------------------- CHECK-CART ---------------------------+" +
                    "\n Warehouse:  " +
                    w_id +
                    "\n District:   " +
                    d_id +
                    "\n Customer:   " +
                    c_id +
                    "\n Line count: " +
                    cartLines.size() +
                    "\n+-----------------------------------------------------------------+\n\n";
            LOG.trace(terminalMessage);
        }
    }

    private Collection<ShoppingCartLine> getShoppingCartLines(Connection conn, int w_id, int d_id, int c_id)
            throws SQLException {
        var cartLines = new ArrayList<ShoppingCartLine>();

        try (PreparedStatement cartGetShoppingCartLines = this.getPreparedStatement(conn,
                cartGetShoppingCartLinesSQL)) {
            cartGetShoppingCartLines.setInt(1, w_id);
            cartGetShoppingCartLines.setInt(2, d_id);
            cartGetShoppingCartLines.setInt(3, c_id);

            try (ResultSet rs = cartGetShoppingCartLines.executeQuery()) {
                while (rs.next()) {
                    var line = new ShoppingCartLine();

                    line._scl_c_id = rs.getInt("_scl_c_id");
                    line._scl_d_id = rs.getInt("_scl_d_id");
                    line._scl_w_id = rs.getInt("_scl_w_id");
                    line._scl_number = rs.getInt("_scl_number");
                    line._scl_i_id = rs.getInt("_scl_i_id");
                    line._scl_supply_w_id = rs.getInt("_scl_supply_w_id");
                    line._scl_delivery_d = rs.getTimestamp("_scl_delivery_d");
                    line._scl_quantity = rs.getInt("_scl_quantity");
                    line._scl_amount = rs.getFloat("_scl_amount");
                    line._scl_dist_info = rs.getString("_scl_dist_info");

                    cartLines.add(line);
                }
            }
        }

        return cartLines;
    }
}
