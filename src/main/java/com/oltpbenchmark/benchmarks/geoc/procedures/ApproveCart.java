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
import com.oltpbenchmark.benchmarks.geoc.GeoCConstants;
import com.oltpbenchmark.benchmarks.geoc.GeoCUtil;
import com.oltpbenchmark.benchmarks.geoc.GeoCWorker;
import com.oltpbenchmark.benchmarks.geoc.pojo.ShoppingCartLine;
import com.oltpbenchmark.benchmarks.geoc.pojo.Stock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ApproveCart extends GeoCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(ApproveCart.class);

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

    public final SQLStmt stmtGetSupervisorFromCustomerSQL = new SQLStmt(
            "SELECT _C_IND_ID FROM " + GeoCConstants.TABLENAME_CUSTOMER +
                    " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");

    public final SQLStmt stmtGetCartLinesSQL = new SQLStmt(
            "SELECT * FROM " + GeoCConstants.TABLENAME_SHOPPING_CART_LINE +
                    " WHERE _SCL_W_ID = ? AND _SCL_D_ID = ? AND _SCL_C_ID = ?");

    public final SQLStmt stmtUpdateDistSQL = new SQLStmt(
            "UPDATE " + GeoCConstants.TABLENAME_DISTRICT +
                    "   SET D_NEXT_O_ID = D_NEXT_O_ID + 1 " +
                    " WHERE D_W_ID = ? " +
                    "   AND D_ID = ?");

    public final SQLStmt stmtInsertNewOrderSQL = new SQLStmt(
            "INSERT INTO " + GeoCConstants.TABLENAME_NEWORDER +
                    " (NO_O_ID, NO_D_ID, NO_W_ID) " +
                    " VALUES ( ?, ?, ?)");

    public final SQLStmt stmtInsertOOrderSQL = new SQLStmt(
            "INSERT INTO " + GeoCConstants.TABLENAME_OPENORDER +
                    " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt stmtGetStockSQL = new SQLStmt(
            "SELECT S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " +
            "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
                    "  FROM " + GeoCConstants.TABLENAME_STOCK +
                    " WHERE S_I_ID = ? " +
                    "   AND S_W_ID = ?");

    public final SQLStmt stmtUpdateStockSQL = new SQLStmt(
            "UPDATE " + GeoCConstants.TABLENAME_STOCK +
                    "   SET S_QUANTITY = ? , " +
                    "       S_YTD = S_YTD + ?, " +
                    "       S_ORDER_CNT = S_ORDER_CNT + 1, " +
                    "       S_REMOTE_CNT = S_REMOTE_CNT + ? " +
                    " WHERE S_I_ID = ? " +
                    "   AND S_W_ID = ?");

    public final SQLStmt stmtInsertOrderLineSQL = new SQLStmt(
            "INSERT INTO " + GeoCConstants.TABLENAME_ORDERLINE +
                    " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) "
                    +
                    " VALUES (?,?,?,?,?,?,?,?,?)");

    public final SQLStmt stmtClearShoppingCartSQL = new SQLStmt(
            "DELETE FROM " + GeoCConstants.TABLENAME_SHOPPING_CART_LINE +
                    " WHERE _SCL_W_ID = ? AND _SCL_D_ID = ? AND _SCL_C_ID = ?");

    public void run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID, GeoCWorker w) throws SQLException {

        int districtID = GeoCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = GeoCUtil.getCustomerID(gen);

        //FIXME: We need to cause 1% of the new orders to be rolled back.

        int _ind_id = 1; //FIXME: add random chance of not being the supervisor
        Timestamp o_entry_d = Timestamp.valueOf(LocalDateTime.now());

        approveCartTransaction(terminalWarehouseID, districtID, customerID, _ind_id, o_entry_d, conn);

    }

    private void approveCartTransaction(int w_id, int d_id, int c_id,
            int _ind_id, Timestamp o_entry_d, Connection conn) throws SQLException {

        getCustomer(conn, w_id, d_id, c_id);

        getWarehouse(conn, w_id);

        int d_next_o_id = getDistrict(conn, w_id, d_id);

        int _c_ind_id = getSupervisorFromCustomer(conn, w_id, d_id, c_id);

        if (_ind_id != _c_ind_id){
            throw new UserAbortException("_IND_ID " + _ind_id + " is not a supervisor!");
        }

        List<ShoppingCartLine> cartLines = getShoppingCartLines(conn, w_id, d_id, c_id);

        int o_ol_cnt = cartLines.size();

        boolean o_all_local = cartLines.stream().map(c -> c._scl_supply_w_id).allMatch(ol_supply_w_id -> ol_supply_w_id == w_id);

        updateDistrict(conn, w_id, d_id);

        insertOpenOrder(conn, w_id, d_id, c_id, o_ol_cnt, o_all_local, d_next_o_id);

        insertNewOrder(conn, w_id, d_id, d_next_o_id);

        try (PreparedStatement stmtUpdateStock = this.getPreparedStatement(conn, stmtUpdateStockSQL);
                PreparedStatement stmtInsertOrderLine = this.getPreparedStatement(conn, stmtInsertOrderLineSQL);
                PreparedStatement stmtClearShoppingCart = this.getPreparedStatement(conn, stmtClearShoppingCartSQL)) {

            for (ShoppingCartLine cartLine: cartLines) {
                int ol_supply_w_id = cartLine._scl_supply_w_id;
                int ol_i_id = cartLine._scl_i_id;
                int ol_quantity = cartLine._scl_quantity;
                int ol_number = cartLine._scl_number;
                float ol_amount = cartLine._scl_amount;

                Stock s = getStock(conn, ol_supply_w_id, ol_i_id);

                String ol_dist_info = getDistInfo(d_id, s);

                stmtInsertOrderLine.setInt(1, d_next_o_id);
                stmtInsertOrderLine.setInt(2, d_id);
                stmtInsertOrderLine.setInt(3, w_id);
                stmtInsertOrderLine.setInt(4, ol_number);
                stmtInsertOrderLine.setInt(5, ol_i_id);
                stmtInsertOrderLine.setInt(6, ol_supply_w_id);
                stmtInsertOrderLine.setInt(7, ol_quantity);
                stmtInsertOrderLine.setDouble(8, ol_amount);
                stmtInsertOrderLine.setString(9, ol_dist_info);
                stmtInsertOrderLine.addBatch();

                int s_remote_cnt_increment;

                if (ol_supply_w_id == w_id) {
                    s_remote_cnt_increment = 0;
                } else {
                    s_remote_cnt_increment = 1;
                }

                stmtUpdateStock.setInt(1, s.s_quantity);
                stmtUpdateStock.setInt(2, ol_quantity);
                stmtUpdateStock.setInt(3, s_remote_cnt_increment);
                stmtUpdateStock.setInt(4, ol_i_id);
                stmtUpdateStock.setInt(5, ol_supply_w_id);
                stmtUpdateStock.addBatch();

            }

            stmtInsertOrderLine.executeBatch();
            stmtInsertOrderLine.clearBatch();

            stmtUpdateStock.executeBatch();
            stmtUpdateStock.clearBatch();

            stmtClearShoppingCart.setInt(1, w_id);
            stmtClearShoppingCart.setInt(2, d_id);
            stmtClearShoppingCart.setInt(3, c_id);
            stmtClearShoppingCart.execute();
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

    private Stock getStock(Connection conn, int ol_supply_w_id, int ol_i_id) throws SQLException {
        try (PreparedStatement stmtGetStock = this.getPreparedStatement(conn, stmtGetStockSQL)) {
            stmtGetStock.setInt(1, ol_i_id);
            stmtGetStock.setInt(2, ol_supply_w_id);
            try (ResultSet rs = stmtGetStock.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("S_I_ID=" + ol_i_id + " not found!");
                }
                Stock s = new Stock();
                s.s_quantity = rs.getInt("S_QUANTITY");
                s.s_ytd = rs.getFloat("S_YTD");
                s.s_order_cnt = rs.getInt("S_ORDER_CNT");
                s.s_remote_cnt = rs.getInt("S_REMOTE_CNT");
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

    private void insertNewOrder(Connection conn, int w_id, int d_id, int o_id) throws SQLException {
        try (PreparedStatement stmtInsertNewOrder = this.getPreparedStatement(conn, stmtInsertNewOrderSQL);) {
            stmtInsertNewOrder.setInt(1, o_id);
            stmtInsertNewOrder.setInt(2, d_id);
            stmtInsertNewOrder.setInt(3, w_id);
            int result = stmtInsertNewOrder.executeUpdate();

            if (result == 0) {
                LOG.warn("new order not inserted");
            }
        }
    }

    private void insertOpenOrder(Connection conn, int w_id, int d_id, int c_id, int o_ol_cnt, Boolean o_all_local, int o_id)
            throws SQLException {
        try (PreparedStatement stmtInsertOOrder = this.getPreparedStatement(conn, stmtInsertOOrderSQL);) {
            stmtInsertOOrder.setInt(1, o_id);
            stmtInsertOOrder.setInt(2, d_id);
            stmtInsertOOrder.setInt(3, w_id);
            stmtInsertOOrder.setInt(4, c_id);
            stmtInsertOOrder.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            stmtInsertOOrder.setInt(6, o_ol_cnt);
            stmtInsertOOrder.setInt(7, o_all_local ? 1:0);

            int result = stmtInsertOOrder.executeUpdate();

            if (result == 0) {
                LOG.warn("open order not inserted");
            }
        }
    }

    private void updateDistrict(Connection conn, int w_id, int d_id) throws SQLException {
        try (PreparedStatement stmtUpdateDist = this.getPreparedStatement(conn, stmtUpdateDistSQL)) {
            stmtUpdateDist.setInt(1, w_id);
            stmtUpdateDist.setInt(2, d_id);
            int result = stmtUpdateDist.executeUpdate();
            if (result == 0) {
                throw new RuntimeException(
                        "Error!! Cannot update next_order_id on district for D_ID=" + d_id + " D_W_ID=" + w_id);
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

    private int getSupervisorFromCustomer(Connection conn, int w_id, int d_id, int c_id) throws SQLException {
        try (PreparedStatement stmtGetSupervisorFromCustomer = this.getPreparedStatement(conn,
                stmtGetSupervisorFromCustomerSQL)) {
            stmtGetSupervisorFromCustomer.setInt(1, w_id);
            stmtGetSupervisorFromCustomer.setInt(2, d_id);
            stmtGetSupervisorFromCustomer.setInt(3, c_id);
            try (ResultSet rs = stmtGetSupervisorFromCustomer.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("W_ID=" + w_id + " D_ID=" + d_id + " C_ID=" + c_id + " not found!");
                }

                return rs.getInt(1);
            }
        }
    }

    public List<ShoppingCartLine> getShoppingCartLines(Connection conn, int w_id, int d_id, int c_id) throws SQLException {
        try (PreparedStatement stmtGetCartLines = this.getPreparedStatement(conn,
                stmtGetCartLinesSQL)) {
            stmtGetCartLines.setInt(1, w_id);
            stmtGetCartLines.setInt(2, d_id);
            stmtGetCartLines.setInt(3, c_id);
            try (ResultSet rs = stmtGetCartLines.executeQuery()) {
                if (!rs.next()) {
                    throw new UserAbortException("Cart is empty for W_ID=" + w_id + " D_ID=" + d_id + " C_ID=" + c_id + " not found!");
                }

                List<ShoppingCartLine> cartLines = new ArrayList<>();
                while(rs.next()){
                    ShoppingCartLine cartLine = new ShoppingCartLine();
                    cartLine._scl_c_id = rs.getInt("_SCL_C_ID");
                    cartLine._scl_c_id = rs.getInt("_SCL_D_ID");
                    cartLine._scl_w_id = rs.getInt("_SCL_W_ID");
                    cartLine._scl_number = rs.getInt("_SCL_NUMBER");
                    cartLine._scl_i_id = rs.getInt("_SCL_I_ID");
                    cartLine._scl_supply_w_id = rs.getInt("_SCL_SUPPLY_W_ID");
                    cartLine._scl_quantity = rs.getInt("_SCL_QUANTITY");
                    cartLine._scl_delivery_d = rs.getTimestamp("_SCL_DELIVERY_D");
                    cartLine._scl_amount = rs.getFloat("_SCL_AMOUNT");
                    cartLine._scl_dist_info = rs.getString("_SCL_DIST_INFO");

                    cartLines.add(cartLine);
                }
                return cartLines;
            }
        }
    }

}
