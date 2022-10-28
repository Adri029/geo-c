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

package com.oltpbenchmark.benchmarks.geoc.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.geoc.GeoCConfig;
import com.oltpbenchmark.benchmarks.geoc.GeoCConstants;
import com.oltpbenchmark.benchmarks.geoc.GeoCUtil;
import com.oltpbenchmark.benchmarks.geoc.GeoCWorker;
import com.oltpbenchmark.benchmarks.geoc.pojo.Stock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.Random;

public class IncreaseCartLine extends GeoCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(IncreaseCartLine.class);

    public final SQLStmt stmtGetCustSQL = new SQLStmt(
        "SELECT C_DISCOUNT, C_LAST, C_CREDIT" +
        "  FROM " + GeoCConstants.TABLENAME_CUSTOMER +
        " WHERE C_W_ID = ? " +
        "   AND C_D_ID = ? " +
        "   AND C_ID = ?");

    public final SQLStmt stmtGetWhseSQL = new SQLStmt(
        "SELECT W_TAX " +
        "  FROM " + GeoCConstants.TABLENAME_WAREHOUSE +
        " WHERE W_ID = ?");

    public final SQLStmt stmtGetDistSQL = new SQLStmt(
            "SELECT D_NEXT_O_ID, D_TAX " +
                    "  FROM " + GeoCConstants.TABLENAME_DISTRICT +
                    " WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE");

    public final SQLStmt stmtGetItemSQL = new SQLStmt(
            "SELECT I_PRICE, I_NAME , I_DATA " +
                    "  FROM " + GeoCConstants.TABLENAME_ITEM +
                    " WHERE I_ID = ?");

    public final SQLStmt stmtGetStockSQL = new SQLStmt(
            "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " +
                    "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
                    "  FROM " + GeoCConstants.TABLENAME_STOCK +
                    " WHERE S_I_ID = ? " +
                    "   AND S_W_ID = ? FOR UPDATE");

    public final SQLStmt stmtCreateCartLineSQL = new SQLStmt(
            "INSERT INTO " + GeoCConstants.TABLENAME_SHOPPING_CART_LINE +
                    " (_SCL_C_ID, _SCL_D_ID, _SCL_W_ID, _SCL_I_ID, _SCL_SUPPLY_W_ID, _SCL_DELIVERY_D, _SCL_QUANTITY, _SCL_AMOUNT, _SCL_DIST_INFO, _SCL_NUMBER) "
                    +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt stmtUpdateCartLineSQL = new SQLStmt(
            "UPDATE " + GeoCConstants.TABLENAME_SHOPPING_CART_LINE +
                    " SET _SCL_QUANTITY = ?, _SCL_AMOUNT = ? WHERE _SCL_W_ID = ? AND _SCL_D_ID = ? AND _SCL_C_ID = ? AND _SCL_I_ID = ?");

    public final SQLStmt stmtGetLastCartLineSQL = new SQLStmt(
            "SELECT max(_SCL_NUMBER) AS LAST_LINE FROM " + GeoCConstants.TABLENAME_SHOPPING_CART_LINE +
                    " WHERE _SCL_W_ID = ? AND _SCL_D_ID = ? AND _SCL_C_ID = ?");

    public final SQLStmt stmtCheckItemInCartSQL = new SQLStmt(
            "SELECT _SCL_QUANTITY FROM " + GeoCConstants.TABLENAME_SHOPPING_CART_LINE +
                    " WHERE _SCL_W_ID = ? AND _SCL_D_ID = ? AND _SCL_C_ID = ? AND _SCL_I_ID = ?");

    public void run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID, GeoCWorker w) throws SQLException {

        int districtID = GeoCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = GeoCUtil.getCustomerID(gen);

        int numItems = GeoCUtil.randomNumber(5, 15, gen);
        int itemID = GeoCUtil.getItemID(gen);
        int supplierWarehouseID;
        int orderQuantity = GeoCUtil.randomNumber(1, 10, gen);
        int allLocal = 1;

        if (GeoCUtil.randomNumber(1, 100, gen) > 1) {
            supplierWarehouseID = terminalWarehouseID;
        } else {
            do {
                supplierWarehouseID = GeoCUtil.randomNumber(1, numWarehouses, gen);
            } while (supplierWarehouseID == terminalWarehouseID && numWarehouses > 1);
            allLocal = 0;
        }

        // we need to cause 1% of the new orders to be rolled back.
        if (GeoCUtil.randomNumber(1, 100, gen) == 1) {
            itemID = GeoCConfig.INVALID_ITEM_ID;
        }

        increaseCartLineTransaction(terminalWarehouseID, districtID, customerID, numItems, allLocal, itemID,
                supplierWarehouseID, orderQuantity, conn);

    }

    private void increaseCartLineTransaction(int w_id, int d_id, int c_id,
            int o_ol_cnt, int o_all_local, int itemID,
            int supplierWarehouseID, int orderQuantity, Connection conn) throws SQLException {

        getCustomer(conn, w_id, d_id, c_id);

        getWarehouse(conn, w_id);

        getDistrict(conn, w_id, d_id);

        Timestamp _scl_delivery_d = Timestamp.valueOf(LocalDateTime.now());

        try (PreparedStatement stmtCreateCartLine = this.getPreparedStatement(conn, this.stmtCreateCartLineSQL)) {
            int _scl_supply_w_id = supplierWarehouseID;
            int _scl_i_id = itemID;
            int _scl_quantity = orderQuantity;

            int inCartQnty = getItemInCartQuantity(conn, w_id, d_id, c_id, _scl_i_id);
            _scl_quantity += inCartQnty;

            // this may occasionally error and that's ok!
            float i_price = getItemPrice(conn, _scl_i_id);
            float _scl_amount = _scl_quantity * i_price;

            if (inCartQnty != 0){
                executeUpdateCartLine(conn, _scl_quantity, _scl_amount, _scl_supply_w_id, d_id, c_id, _scl_i_id);
            }

            Stock s = getStock(conn, _scl_supply_w_id, _scl_i_id, _scl_quantity);

            if (s.s_quantity < _scl_quantity){
                /*TODO abort*/
            }

            String ol_dist_info = getDistInfo(d_id, s);

            int next_id = getNextItemID(conn, w_id, d_id, c_id);

            stmtCreateCartLine.setInt(1, c_id);
            stmtCreateCartLine.setInt(2, d_id);
            stmtCreateCartLine.setInt(3, w_id);
            stmtCreateCartLine.setInt(4, _scl_i_id);
            stmtCreateCartLine.setInt(5, _scl_supply_w_id);
            stmtCreateCartLine.setTimestamp(6, _scl_delivery_d);
            stmtCreateCartLine.setInt(7, _scl_quantity);
            stmtCreateCartLine.setDouble(8, _scl_amount);
            stmtCreateCartLine.setString(9, ol_dist_info);
            stmtCreateCartLine.setInt(10, next_id);

            stmtCreateCartLine.execute();
        }

    }

    private void executeUpdateCartLine(Connection conn, int _scl_quantity, float _scl_amount, int w_id, int d_id, int c_id, int _scl_i_id) throws SQLException{
        try (PreparedStatement stmtUpdateCartLine = this.getPreparedStatement(conn, this.stmtUpdateCartLineSQL)){
            stmtUpdateCartLine.setInt(1, _scl_quantity);
            stmtUpdateCartLine.setFloat(2, _scl_amount);
            stmtUpdateCartLine.setInt(3, w_id);
            stmtUpdateCartLine.setInt(4, d_id);
            stmtUpdateCartLine.setInt(5, c_id);
            stmtUpdateCartLine.setInt(6, _scl_i_id);
            stmtUpdateCartLine.executeUpdate();
        }
    }

    private int getItemInCartQuantity(Connection conn, int w_id, int d_id, int c_id, int _scl_i_id) throws SQLException{
        try (PreparedStatement stmtCheckItemInCart = this.getPreparedStatement(conn, this.stmtCheckItemInCartSQL)){
            stmtCheckItemInCart.setInt(1, w_id);
            stmtCheckItemInCart.setInt(2, d_id);
            stmtCheckItemInCart.setInt(3, c_id);
            stmtCheckItemInCart.setInt(4, _scl_i_id);
            try (ResultSet rs = stmtCheckItemInCart.executeQuery()) {
                if (!rs.next()){
                    return 0;
                }else{
                    return rs.getInt("_SCL_QUANTITY");
                }
            }
        }
    }

    private int getNextItemID(Connection conn, int w_id, int d_id, int c_id) throws SQLException{
        try(PreparedStatement stmtGetLastCartLine = this.getPreparedStatement(conn, this.stmtGetLastCartLineSQL)){
            stmtGetLastCartLine.setInt(1, w_id);
            stmtGetLastCartLine.setInt(2, d_id);
            stmtGetLastCartLine.setInt(3, c_id);
            try (ResultSet rs = stmtGetLastCartLine.executeQuery()) {
                if (!rs.next()){
                    throw new UserAbortException("W_ID=" + w_id + " D_ID=" + d_id + " C_ID=" + c_id + " not found!");
                }

                return rs.getInt("LAST_LINE") + 1;
            }
        }
    }

    private String getDistInfo(int d_id, Stock s) {
        return switch (d_id) {
            case 1 -> s.s_dist_01;
            case 2 -> s.s_dist_02;
            case 3 -> s.s_dist_03;
            case 4 -> s.s_dist_04;
            case 5 -> s.s_dist_05;
            case 6 -> s.s_dist_06;
            case 7 -> s.s_dist_07;
            case 8 -> s.s_dist_08;
            case 9 -> s.s_dist_09;
            case 10 -> s.s_dist_10;
            default -> null;
        };
    }

    private Stock getStock(Connection conn, int ol_supply_w_id, int ol_i_id, int ol_quantity) throws SQLException {
        try (PreparedStatement stmtGetStock = this.getPreparedStatement(conn, stmtGetStockSQL)) {
            stmtGetStock.setInt(1, ol_i_id);
            stmtGetStock.setInt(2, ol_supply_w_id);
            try (ResultSet rs = stmtGetStock.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("S_I_ID=" + ol_i_id + " not found!");
                }
                Stock s = new Stock();
                s.s_quantity = rs.getInt("S_QUANTITY");
                s.s_dist_01 = rs.getString("S_DIST_01");
                s.s_dist_02 = rs.getString("S_DIST_02");
                s.s_dist_03 = rs.getString("S_DIST_03");
                s.s_dist_04 = rs.getString("S_DIST_04");
                s.s_dist_05 = rs.getString("S_DIST_05");
                s.s_dist_06 = rs.getString("S_DIST_06");
                s.s_dist_07 = rs.getString("S_DIST_07");
                s.s_dist_08 = rs.getString("S_DIST_08");
                s.s_dist_09 = rs.getString("S_DIST_09");
                s.s_dist_10 = rs.getString("S_DIST_10");

                return s;
            }
        }
    }

    private float getItemPrice(Connection conn, int ol_i_id) throws SQLException {
        try (PreparedStatement stmtGetItem = this.getPreparedStatement(conn, stmtGetItemSQL)) {
            stmtGetItem.setInt(1, ol_i_id);
            try (ResultSet rs = stmtGetItem.executeQuery()) {
                if (!rs.next()) {
                    // This is (hopefully) an expected error: this is an expected new order rollback
                    throw new UserAbortException("EXPECTED Increase Cart Line rollback: I_ID=" + ol_i_id + " not found!");
                }

                return rs.getFloat("I_PRICE");
            }
        }
    }


    private int getDistrict(Connection conn, int w_id, int d_id) throws SQLException {
        try (PreparedStatement stmtGetDist = this.getPreparedStatement(conn, stmtGetDistSQL)) {
            stmtGetDist.setInt(1, w_id);
            stmtGetDist.setInt(2, d_id);
            try (ResultSet rs = stmtGetDist.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
                }
                return rs.getInt("D_NEXT_O_ID");
            }
        }
    }

    private void getWarehouse(Connection conn, int w_id) throws SQLException {
        try (PreparedStatement stmtGetWhse = this.getPreparedStatement(conn, stmtGetWhseSQL)) {
            stmtGetWhse.setInt(1, w_id);
            try (ResultSet rs = stmtGetWhse.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("W_ID=" + w_id + " not found!");
                }
            }
        }
    }

    private void getCustomer(Connection conn, int w_id, int d_id, int c_id) throws SQLException {
        try (PreparedStatement stmtGetCust = this.getPreparedStatement(conn, stmtGetCustSQL)) {
            stmtGetCust.setInt(1, w_id);
            stmtGetCust.setInt(2, d_id);
            stmtGetCust.setInt(3, c_id);
            try (ResultSet rs = stmtGetCust.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("C_D_ID=" + d_id + " C_ID=" + c_id + " not found!");
                }
            }
        }
    }

}
