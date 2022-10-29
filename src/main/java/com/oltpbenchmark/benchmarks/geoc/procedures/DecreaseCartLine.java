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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DecreaseCartLine extends GeoCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(DecreaseCartLine.class);

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

    public final SQLStmt stmtUpdateCartLineSQL = new SQLStmt(
            "UPDATE " + GeoCConstants.TABLENAME_SHOPPING_CART_LINE +
                    " SET _SCL_QUANTITY = ?, _SCL_AMOUNT = ? WHERE _SCL_W_ID = ? AND _SCL_D_ID = ? AND _SCL_C_ID = ? AND _SCL_I_ID = ?");

    public final SQLStmt stmtGetItemIdCartLineSQL = new SQLStmt(
            "SELECT _SCL_I_ID FROM " + GeoCConstants.TABLENAME_SHOPPING_CART_LINE +
                    " WHERE _SCL_W_ID = ? AND _SCL_D_ID = ? AND _SCL_C_ID = ?");

    public final SQLStmt stmtCheckItemQuantityInCartSQL = new SQLStmt(
            "SELECT _SCL_QUANTITY FROM " + GeoCConstants.TABLENAME_SHOPPING_CART_LINE +
                    " WHERE _SCL_W_ID = ? AND _SCL_D_ID = ? AND _SCL_C_ID = ? AND _SCL_I_ID = ?");

    public final SQLStmt stmtRemoveCartLineSQL = new SQLStmt(
            "DELETE FROM " + GeoCConstants.TABLENAME_SHOPPING_CART_LINE +
                    " WHERE _SCL_W_ID = ? AND _SCL_D_ID = ? AND _SCL_C_ID = ? AND _SCL_I_ID = ?");

    public void run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID, GeoCWorker w) throws SQLException {

        int districtID = GeoCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = GeoCUtil.getCustomerID(gen);

        int item = GeoCUtil.randomNumber(0,10000,gen);
        int removeQuantity = GeoCUtil.randomNumber(1, 10, gen);

        decreaseCartLineTransaction(terminalWarehouseID, districtID, customerID, item,
                removeQuantity, conn);

    }

    private void decreaseCartLineTransaction(int w_id, int d_id, int c_id,
            int item, int removeQuantity, Connection conn) throws SQLException {

        getCustomer(conn, w_id, d_id, c_id);

        getWarehouse(conn, w_id);

        getDistrict(conn, w_id, d_id);

        int _scl_i_id = getItemToDecrease(conn, w_id, d_id, c_id, item);

        int inCartQnty = getItemInCartQuantity(conn, w_id, d_id, c_id, _scl_i_id);
        int _scl_quantity = inCartQnty - removeQuantity;

        // this may occasionally error and that's ok!
        float i_price = getItemPrice(conn, _scl_i_id);
        float _scl_amount = _scl_quantity * i_price;

        if (_scl_quantity > 0) {
            executeUpdateCartLine(conn, _scl_quantity, _scl_amount, w_id, d_id, c_id, _scl_i_id);
        } else {
            executeRemoveCartLine(conn, w_id, d_id, c_id, _scl_i_id);
        }

    }

    private void executeRemoveCartLine(Connection conn, int w_id, int d_id, int c_id, int i_id) throws SQLException {
        try(PreparedStatement stmtRemoveCartLine = this.getPreparedStatement(conn, this.stmtRemoveCartLineSQL)){
            stmtRemoveCartLine.setInt(1, w_id);
            stmtRemoveCartLine.setInt(2, d_id);
            stmtRemoveCartLine.setInt(3, c_id);
            stmtRemoveCartLine.setInt(4, i_id);
            stmtRemoveCartLine.execute();
        }

    }

    private void executeUpdateCartLine(Connection conn, int _scl_quantity, float _scl_amount, int w_id, int d_id,
            int c_id, int _scl_i_id) throws SQLException {
        try (PreparedStatement stmtUpdateCartLine = this.getPreparedStatement(conn, this.stmtUpdateCartLineSQL)) {
            stmtUpdateCartLine.setInt(1, _scl_quantity);
            stmtUpdateCartLine.setFloat(2, _scl_amount);
            stmtUpdateCartLine.setInt(3, w_id);
            stmtUpdateCartLine.setInt(4, d_id);
            stmtUpdateCartLine.setInt(5, c_id);
            stmtUpdateCartLine.setInt(6, _scl_i_id);
            stmtUpdateCartLine.executeUpdate();
        }
    }

    private int getItemToDecrease(Connection conn, int w_id, int d_id, int c_id, int number) throws SQLException {
        try (PreparedStatement stmtCheckItemInCart = this.getPreparedStatement(conn, this.stmtGetItemIdCartLineSQL)) {
            stmtCheckItemInCart.setInt(1, w_id);
            stmtCheckItemInCart.setInt(2, d_id);
            stmtCheckItemInCart.setInt(3, c_id);
            try (ResultSet rs = stmtCheckItemInCart.executeQuery()) {
                if (!rs.next()) {
                    throw new UserAbortException("EXPECTED Decrease Cart Line rollback: Cart Line for W_ID=" + w_id
                            + "D_ID=" + d_id + "C_ID=" + c_id + " not found!");
                }

                List<Integer> scl_ids = new ArrayList<>();
                scl_ids.add(rs.getInt("_SCL_I_ID"));

                while (rs.next()) {
                    scl_ids.add(rs.getInt("_SCL_I_ID"));
                }

                int idx = number % scl_ids.size();
                return scl_ids.get(idx);
            }
        }
    }

    private int getItemInCartQuantity(Connection conn, int w_id, int d_id, int c_id, int _scl_i_id)
            throws SQLException {
        try (PreparedStatement stmtCheckItemInCart = this.getPreparedStatement(conn,
                this.stmtCheckItemQuantityInCartSQL)) {
            stmtCheckItemInCart.setInt(1, w_id);
            stmtCheckItemInCart.setInt(2, d_id);
            stmtCheckItemInCart.setInt(3, c_id);
            stmtCheckItemInCart.setInt(4, _scl_i_id);
            try (ResultSet rs = stmtCheckItemInCart.executeQuery()) {
                if (!rs.next()) {
                    return 0;
                } else {
                    return rs.getInt("_SCL_QUANTITY");
                }
            }
        }
    }


    private float getItemPrice(Connection conn, int ol_i_id) throws SQLException {
        try (PreparedStatement stmtGetItem = this.getPreparedStatement(conn, stmtGetItemSQL)) {
            stmtGetItem.setInt(1, ol_i_id);
            try (ResultSet rs = stmtGetItem.executeQuery()) {
                if (!rs.next()) {
                    // This is (hopefully) an expected error: this is an expected new order rollback
                    throw new UserAbortException(
                            "EXPECTED Decrease Cart Line rollback: I_ID=" + ol_i_id + " not found!");
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
