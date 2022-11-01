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
import com.oltpbenchmark.benchmarks.geoc.GeoCConfig;
import com.oltpbenchmark.benchmarks.geoc.GeoCConstants;
import com.oltpbenchmark.benchmarks.geoc.GeoCWorker;
import com.oltpbenchmark.benchmarks.geoc.pojo.Stock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

public class Restock extends GeoCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(Restock.class);

    public SQLStmt restockGetStockInfoSQL = new SQLStmt(
            "SELECT S_I_ID, S_QUANTITY"
                    + " FROM " + GeoCConstants.TABLENAME_STOCK
                    + " WHERE S_W_ID = ? AND S_QUANTITY <= ?");

    public SQLStmt restockUpdateStockSQL = new SQLStmt(
            "UPDATE " + GeoCConstants.TABLENAME_STOCK
                    + " SET S_QUANTITY = ? WHERE S_W_ID = ? AND S_I_ID = ?");

    public void run(Connection conn, Random gen, int w_id, int numWarehouses, int terminalDistrictLowerID,
            int terminalDistrictUpperID, GeoCWorker w) throws SQLException {

        var threshold = GeoCConfig.configStockThreshold;
        var stocks = getStockInfo(conn, w_id, threshold);
        if (stocks.size() == 0) {
            // This is an expected error: there are no items in need of a restock
            throw new UserAbortException("EXPECTED restock rollback: W_ID=" + w_id + " does not need restocking!");
        }

        var restockQuantity = GeoCConfig.configRestockQuantity;
        updateStock(conn, w_id, stocks, restockQuantity);

        if (LOG.isTraceEnabled()) {
            String terminalMessage = "\n+--------------------------- RESTOCK -----------------------------+" +
                    "\n Warehouse:      " +
                    w_id +
                    "\n Updated stocks: " +
                    stocks.size() +
                    "\n+-----------------------------------------------------------------+\n\n";
            LOG.trace(terminalMessage);
        }
    }

    private Collection<Stock> getStockInfo(Connection conn, int w_id, int threshold) throws SQLException {
        var stocks = new ArrayList<Stock>();

        try (PreparedStatement restockGetStockInfo = this.getPreparedStatement(conn,
                restockGetStockInfoSQL)) {
            restockGetStockInfo.setInt(1, w_id);
            restockGetStockInfo.setInt(2, threshold);

            try (ResultSet rs = restockGetStockInfo.executeQuery()) {
                while (rs.next()) {
                    var stock = new Stock();

                    stock.s_i_id = rs.getInt("s_i_id");
                    stock.s_quantity = rs.getInt("s_quantity");

                    stocks.add(stock);
                }
            }
        }

        return stocks;
    }

    private void updateStock(Connection conn, int w_id, Collection<Stock> stocks, int restockQuantity)
            throws SQLException {
        try (PreparedStatement restockUpdateStock = this.getPreparedStatement(conn,
                restockUpdateStockSQL)) {
            for (var stock : stocks) {
                restockUpdateStock.setInt(1, stock.s_quantity + restockQuantity);
                restockUpdateStock.setInt(2, w_id);
                restockUpdateStock.setInt(3, stock.s_i_id);
                restockUpdateStock.addBatch();
            }

            restockUpdateStock.executeBatch();
            restockUpdateStock.clearBatch();
        }
    }
}
