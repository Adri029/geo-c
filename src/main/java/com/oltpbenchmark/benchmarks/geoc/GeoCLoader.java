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

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.geoc.pojo.*;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * GeoC Benchmark Loader
 */
public class GeoCLoader extends Loader<GeoCBenchmark> {

    private static final int FIRST_UNPROCESSED_O_ID = 2101;

    private final long numWarehouses;

    public GeoCLoader(GeoCBenchmark benchmark) {
        super(benchmark);
        numWarehouses = Math.max(Math.round(GeoCConfig.configWhseCount * this.scaleFactor), 1);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();
        final CountDownLatch itemLatch = new CountDownLatch(1);

        // ITEM
        // This will be invoked first and executed in a single thread.
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
                loadItems(conn, GeoCConfig.configItemCount);
            }

            @Override
            public void afterLoad() {
                itemLatch.countDown();
            }
        });

        // WAREHOUSES
        // We use a separate thread per warehouse. Each thread will load
        // all of the tables that depend on that warehouse. They all have
        // to wait until the ITEM table is loaded first though.
        for (int w = 1; w <= numWarehouses; w++) {
            final int w_id = w;
            LoaderThread t = new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) {

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load WAREHOUSE {}", w_id);
                    }
                    // WAREHOUSE
                    loadWarehouse(conn, w_id);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load STOCK {}", w_id);
                    }
                    // STOCK
                    loadStock(conn, w_id, GeoCConfig.configItemCount, numWarehouses, GeoCConfig.configWhseSpecificItems);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load DISTRICT {}", w_id);
                    }
                    // DISTRICT
                    loadDistricts(conn, w_id, GeoCConfig.configDistPerWhse);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load CUSTOMER {}", w_id);
                    }
                    // CUSTOMER
                    loadCustomers(conn, w_id, GeoCConfig.configDistPerWhse, GeoCConfig.configCustPerDist);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load _INDIVIDUAL {}", w_id);
                    }
                    // _INDIVIDUAL
                    loadIndividuals(conn, w_id, GeoCConfig.configDistPerWhse, GeoCConfig.configCustPerDist, GeoCConfig.configIndPerCust);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to update CUSTOMER with supervisors {}", w_id);
                    }
                    // _INDIVIDUAL
                    updateCustomersWithSupervisors(conn, w_id, GeoCConfig.configDistPerWhse, GeoCConfig.configCustPerDist);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load CUSTOMER HISTORY {}", w_id);
                    }
                    // CUSTOMER HISTORY
                    loadCustomerHistory(conn, w_id, GeoCConfig.configDistPerWhse, GeoCConfig.configCustPerDist);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load ORDERS {}", w_id);
                    }
                    // ORDERS
                    loadOpenOrders(conn, w_id, GeoCConfig.configDistPerWhse, GeoCConfig.configCustPerDist);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load NEW ORDERS {}", w_id);
                    }
                    // NEW ORDERS
                    loadNewOrders(conn, w_id, GeoCConfig.configDistPerWhse, GeoCConfig.configCustPerDist);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load ORDER LINES {}", w_id);
                    }
                    // ORDER LINES
                    loadOrderLines(conn, w_id, GeoCConfig.configDistPerWhse, GeoCConfig.configCustPerDist, GeoCConfig.configWhseSpecificItems);

                }

                @Override
                public void beforeLoad() {

                    // Make sure that we load the ITEM table first

                    try {
                        itemLatch.await();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
            threads.add(t);
        }
        return (threads);
    }

    private PreparedStatement getInsertStatement(Connection conn, String tableName) throws SQLException {
        Table catalog_tbl = benchmark.getCatalog().getTable(tableName);
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        return conn.prepareStatement(sql);
    }


    protected void loadItems(Connection conn, int itemCount) {

        try (PreparedStatement itemPrepStmt = getInsertStatement(conn, GeoCConstants.TABLENAME_ITEM)) {

            int batchSize = 0;
            for (int i = 1; i <= itemCount; i++) {

                Item item = new Item();
                item.i_id = i;
                item.i_name = GeoCUtil.randomStr(GeoCUtil.randomNumber(14, 24, benchmark.rng()));
                item.i_price = GeoCUtil.randomNumber(100, 10000, benchmark.rng()) / 100.0;

                // i_data
                int randPct = GeoCUtil.randomNumber(1, 100, benchmark.rng());
                int len = GeoCUtil.randomNumber(26, 50, benchmark.rng());
                if (randPct > 10) {
                    // 90% of time i_data isa random string of length [26 .. 50]
                    item.i_data = GeoCUtil.randomStr(len);
                } else {
                    // 10% of time i_data has "ORIGINAL" crammed somewhere in
                    // middle
                    int startORIGINAL = GeoCUtil.randomNumber(2, (len - 8), benchmark.rng());
                    item.i_data = GeoCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + GeoCUtil.randomStr(len - startORIGINAL - 9);
                }

                item.i_im_id = GeoCUtil.randomNumber(1, 10000, benchmark.rng());

                int idx = 1;
                itemPrepStmt.setLong(idx++, item.i_id);
                itemPrepStmt.setString(idx++, item.i_name);
                itemPrepStmt.setDouble(idx++, item.i_price);
                itemPrepStmt.setString(idx++, item.i_data);
                itemPrepStmt.setLong(idx, item.i_im_id);
                itemPrepStmt.addBatch();
                batchSize++;

                if (batchSize == workConf.getBatchSize()) {
                    itemPrepStmt.executeBatch();
                    itemPrepStmt.clearBatch();
                    batchSize = 0;
                }
            }


            if (batchSize > 0) {
                itemPrepStmt.executeBatch();
                itemPrepStmt.clearBatch();
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }


    protected void loadWarehouse(Connection conn, int w_id) {

        try (PreparedStatement whsePrepStmt = getInsertStatement(conn, GeoCConstants.TABLENAME_WAREHOUSE)) {
            Warehouse warehouse = new Warehouse();

            warehouse.w_id = w_id;
            warehouse.w_ytd = 300000;

            // random within [0.0000 .. 0.2000]
            warehouse.w_tax = (GeoCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0;
            warehouse.w_name = GeoCUtil.randomStr(GeoCUtil.randomNumber(6, 10, benchmark.rng()));
            warehouse.w_street_1 = GeoCUtil.randomStr(GeoCUtil.randomNumber(10, 20, benchmark.rng()));
            warehouse.w_street_2 = GeoCUtil.randomStr(GeoCUtil.randomNumber(10, 20, benchmark.rng()));
            warehouse.w_city = GeoCUtil.randomStr(GeoCUtil.randomNumber(10, 20, benchmark.rng()));
            warehouse.w_state = GeoCUtil.randomStr(3).toUpperCase();
            warehouse.w_zip = "123456789";

            int idx = 1;
            whsePrepStmt.setLong(idx++, warehouse.w_id);
            whsePrepStmt.setDouble(idx++, warehouse.w_ytd);
            whsePrepStmt.setDouble(idx++, warehouse.w_tax);
            whsePrepStmt.setString(idx++, warehouse.w_name);
            whsePrepStmt.setString(idx++, warehouse.w_street_1);
            whsePrepStmt.setString(idx++, warehouse.w_street_2);
            whsePrepStmt.setString(idx++, warehouse.w_city);
            whsePrepStmt.setString(idx++, warehouse.w_state);
            whsePrepStmt.setString(idx, warehouse.w_zip);
            whsePrepStmt.execute();

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }

    protected void loadStock(Connection conn, int w_id, int numItems, long numWarehouses, int whseSpecificItems) {

        int k = 0;

        try (PreparedStatement stockPreparedStatement = getInsertStatement(conn, GeoCConstants.TABLENAME_STOCK)) {

            for (int i = 1; i <= numItems; i++) {
                if (i <= whseSpecificItems && (i % numWarehouses) + 1 != w_id) {
                    // Items below `whseSpecificItems` are unique to one warehouse,
                    // so their stock should only be added to that warehouse.
                    continue;
                }

                Stock stock = new Stock();
                stock.s_i_id = i;
                stock.s_w_id = w_id;
                stock.s_quantity = GeoCUtil.randomNumber(10, 100, benchmark.rng());
                stock.s_ytd = 0;
                stock.s_order_cnt = 0;
                stock.s_remote_cnt = 0;

                // s_data
                int randPct = GeoCUtil.randomNumber(1, 100, benchmark.rng());
                int len = GeoCUtil.randomNumber(26, 50, benchmark.rng());
                if (randPct > 10) {
                    // 90% of time i_data isa random string of length [26 ..
                    // 50]
                    stock.s_data = GeoCUtil.randomStr(len);
                } else {
                    // 10% of time i_data has "ORIGINAL" crammed somewhere
                    // in middle
                    int startORIGINAL = GeoCUtil.randomNumber(2, (len - 8), benchmark.rng());
                    stock.s_data = GeoCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + GeoCUtil.randomStr(len - startORIGINAL - 9);
                }

                int idx = 1;
                stockPreparedStatement.setLong(idx++, stock.s_w_id);
                stockPreparedStatement.setLong(idx++, stock.s_i_id);
                stockPreparedStatement.setLong(idx++, stock.s_quantity);
                stockPreparedStatement.setDouble(idx++, stock.s_ytd);
                stockPreparedStatement.setLong(idx++, stock.s_order_cnt);
                stockPreparedStatement.setLong(idx++, stock.s_remote_cnt);
                stockPreparedStatement.setString(idx++, stock.s_data);
                stockPreparedStatement.setString(idx++, GeoCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, GeoCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, GeoCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, GeoCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, GeoCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, GeoCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, GeoCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, GeoCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, GeoCUtil.randomStr(24));
                stockPreparedStatement.setString(idx, GeoCUtil.randomStr(24));
                stockPreparedStatement.addBatch();

                k++;

                if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                    stockPreparedStatement.executeBatch();
                    stockPreparedStatement.clearBatch();
                }
            }

            stockPreparedStatement.executeBatch();
            stockPreparedStatement.clearBatch();

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }

    protected void loadDistricts(Connection conn, int w_id, int districtsPerWarehouse) {

        try (PreparedStatement distPrepStmt = getInsertStatement(conn, GeoCConstants.TABLENAME_DISTRICT)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {
                District district = new District();
                district.d_id = d;
                district.d_w_id = w_id;
                district.d_ytd = 30000;

                // random within [0.0000 .. 0.2000]
                district.d_tax = (float) ((GeoCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0);

                district.d_next_o_id = GeoCConfig.configCustPerDist + 1;
                district.d_name = GeoCUtil.randomStr(GeoCUtil.randomNumber(6, 10, benchmark.rng()));
                district.d_street_1 = GeoCUtil.randomStr(GeoCUtil.randomNumber(10, 20, benchmark.rng()));
                district.d_street_2 = GeoCUtil.randomStr(GeoCUtil.randomNumber(10, 20, benchmark.rng()));
                district.d_city = GeoCUtil.randomStr(GeoCUtil.randomNumber(10, 20, benchmark.rng()));
                district.d_state = GeoCUtil.randomStr(3).toUpperCase();
                district.d_zip = "123456789";

                int idx = 1;
                distPrepStmt.setLong(idx++, district.d_w_id);
                distPrepStmt.setLong(idx++, district.d_id);
                distPrepStmt.setDouble(idx++, district.d_ytd);
                distPrepStmt.setDouble(idx++, district.d_tax);
                distPrepStmt.setLong(idx++, district.d_next_o_id);
                distPrepStmt.setString(idx++, district.d_name);
                distPrepStmt.setString(idx++, district.d_street_1);
                distPrepStmt.setString(idx++, district.d_street_2);
                distPrepStmt.setString(idx++, district.d_city);
                distPrepStmt.setString(idx++, district.d_state);
                distPrepStmt.setString(idx, district.d_zip);
                distPrepStmt.executeUpdate();
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }

    protected void loadCustomers(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

        int k = 0;

        try (PreparedStatement custPrepStmt = getInsertStatement(conn, GeoCConstants.TABLENAME_CUSTOMER)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {
                for (int c = 1; c <= customersPerDistrict; c++) {
                    Timestamp sysdate = new Timestamp(System.currentTimeMillis());

                    Customer customer = new Customer();
                    customer.c_id = c;
                    customer.c_d_id = d;
                    customer.c_w_id = w_id;

                    // discount is random between [0.0000 ... 0.5000]
                    customer.c_discount = (float) (GeoCUtil.randomNumber(1, 5000, benchmark.rng()) / 10000.0);

                    if (GeoCUtil.randomNumber(1, 100, benchmark.rng()) <= 10) {
                        customer.c_credit = "BC"; // 10% Bad Credit
                    } else {
                        customer.c_credit = "GC"; // 90% Good Credit
                    }
                    if (c <= 1000) {
                        customer.c_last = GeoCUtil.getLastName(c - 1);
                    } else {
                        customer.c_last = GeoCUtil.getNonUniformRandomLastNameForLoad(benchmark.rng());
                    }
                    customer.c_first = GeoCUtil.randomStr(GeoCUtil.randomNumber(8, 16, benchmark.rng()));
                    customer.c_credit_lim = 50000;

                    customer.c_balance = -10;
                    customer.c_ytd_payment = 10;
                    customer.c_payment_cnt = 1;
                    customer.c_delivery_cnt = 0;

                    customer.c_street_1 = GeoCUtil.randomStr(GeoCUtil.randomNumber(10, 20, benchmark.rng()));
                    customer.c_street_2 = GeoCUtil.randomStr(GeoCUtil.randomNumber(10, 20, benchmark.rng()));
                    customer.c_city = GeoCUtil.randomStr(GeoCUtil.randomNumber(10, 20, benchmark.rng()));
                    customer.c_state = GeoCUtil.randomStr(3).toUpperCase();
                    // TPC-C 4.3.2.7: 4 random digits + "11111"
                    customer.c_zip = GeoCUtil.randomNStr(4) + "11111";
                    customer.c_phone = GeoCUtil.randomNStr(16);
                    customer.c_since = sysdate;
                    customer.c_middle = "OE";
                    customer.c_data = GeoCUtil.randomStr(GeoCUtil.randomNumber(300, 500, benchmark.rng()));

                    int idx = 1;
                    custPrepStmt.setLong(idx++, customer.c_w_id);
                    custPrepStmt.setLong(idx++, customer.c_d_id);
                    custPrepStmt.setLong(idx++, customer.c_id);
                    custPrepStmt.setDouble(idx++, customer.c_discount);
                    custPrepStmt.setString(idx++, customer.c_credit);
                    custPrepStmt.setString(idx++, customer.c_last);
                    custPrepStmt.setString(idx++, customer.c_first);
                    custPrepStmt.setDouble(idx++, customer.c_credit_lim);
                    custPrepStmt.setDouble(idx++, customer.c_balance);
                    custPrepStmt.setDouble(idx++, customer.c_ytd_payment);
                    custPrepStmt.setLong(idx++, customer.c_payment_cnt);
                    custPrepStmt.setLong(idx++, customer.c_delivery_cnt);
                    custPrepStmt.setString(idx++, customer.c_street_1);
                    custPrepStmt.setString(idx++, customer.c_street_2);
                    custPrepStmt.setString(idx++, customer.c_city);
                    custPrepStmt.setString(idx++, customer.c_state);
                    custPrepStmt.setString(idx++, customer.c_zip);
                    custPrepStmt.setString(idx++, customer.c_phone);
                    custPrepStmt.setTimestamp(idx++, customer.c_since);
                    custPrepStmt.setString(idx++, customer.c_middle);
                    custPrepStmt.setString(idx++, customer.c_data);
                    custPrepStmt.setNull(idx, java.sql.Types.NULL);
                    custPrepStmt.addBatch();

                    k++;

                    if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                        custPrepStmt.executeBatch();
                        custPrepStmt.clearBatch();
                    }
                }
            }

            custPrepStmt.executeBatch();
            custPrepStmt.clearBatch();

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }

    protected void loadIndividuals(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict, int individualsPerCustomer) {
        int k = 0;

        try (PreparedStatement indPrepStmt = getInsertStatement(conn, GeoCConstants.TABLENAME_INDIVIDUAL)) {
            for (int d = 1; d <= districtsPerWarehouse; d++) {
                for (int c = 1; c <= customersPerDistrict; c++) {
                    for (int i = 1; i <= individualsPerCustomer; i++) {
                        Individual individual = new Individual();

                        individual._ind_id = i;
                        individual._ind_name = GeoCUtil.randomStr(GeoCUtil.randomNumber(16, 32, benchmark.rng()));
                        individual._ind_c_id = c;
                        individual._ind_d_id = d;
                        individual._ind_w_id = w_id;

                        int idx = 1;
                        indPrepStmt.setInt(idx++, individual._ind_id);
                        indPrepStmt.setString(idx++, individual._ind_name);
                        indPrepStmt.setInt(idx++, individual._ind_c_id);
                        indPrepStmt.setInt(idx++, individual._ind_d_id);
                        indPrepStmt.setInt(idx, individual._ind_w_id);
                        indPrepStmt.addBatch();

                        k++;

                        if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                            indPrepStmt.executeBatch();
                            indPrepStmt.clearBatch();
                        }
                    }
                }
            }

            indPrepStmt.executeBatch();
            indPrepStmt.clearBatch();
        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }
    }
    
    protected void updateCustomersWithSupervisors(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {
        int k = 0;

        try (PreparedStatement custPrepStmt = conn.prepareStatement(
                "UPDATE " + GeoCConstants.TABLENAME_CUSTOMER + " SET _C_IND_ID = ?" 
                    + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?")) {
            for (int d = 1; d <= districtsPerWarehouse; d++) {
                for (int c = 1; c <= customersPerDistrict; c++) {
                    int idx = 1;
                    custPrepStmt.setInt(idx++, 1);
                    custPrepStmt.setInt(idx++, w_id);
                    custPrepStmt.setInt(idx++, d);
                    custPrepStmt.setInt(idx, c);
                    custPrepStmt.addBatch();

                    k++;

                    if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                        custPrepStmt.executeBatch();
                        custPrepStmt.clearBatch();
                    }
                }
            }

            custPrepStmt.executeBatch();
            custPrepStmt.clearBatch();
        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }
    }

    protected void loadCustomerHistory(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

        int k = 0;

        try (PreparedStatement histPrepStmt = getInsertStatement(conn, GeoCConstants.TABLENAME_HISTORY)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {
                for (int c = 1; c <= customersPerDistrict; c++) {
                    Timestamp sysdate = new Timestamp(System.currentTimeMillis());

                    History history = new History();
                    history.h_c_id = c;
                    history.h_c_d_id = d;
                    history.h_c_w_id = w_id;
                    history.h_d_id = d;
                    history.h_w_id = w_id;
                    history.h_date = sysdate;
                    history.h_amount = 10;
                    history.h_data = GeoCUtil.randomStr(GeoCUtil.randomNumber(10, 24, benchmark.rng()));


                    int idx = 1;
                    histPrepStmt.setInt(idx++, history.h_c_id);
                    histPrepStmt.setInt(idx++, history.h_c_d_id);
                    histPrepStmt.setInt(idx++, history.h_c_w_id);
                    histPrepStmt.setInt(idx++, history.h_d_id);
                    histPrepStmt.setInt(idx++, history.h_w_id);
                    histPrepStmt.setTimestamp(idx++, history.h_date);
                    histPrepStmt.setDouble(idx++, history.h_amount);
                    histPrepStmt.setString(idx, history.h_data);
                    histPrepStmt.addBatch();

                    k++;

                    if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                        histPrepStmt.executeBatch();
                        histPrepStmt.clearBatch();
                    }
                }
            }

            histPrepStmt.executeBatch();
            histPrepStmt.clearBatch();

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }

    protected void loadOpenOrders(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

        int k = 0;

        try (PreparedStatement openOrderStatement = getInsertStatement(conn, GeoCConstants.TABLENAME_OPENORDER)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {
                // TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
                int[] c_ids = new int[customersPerDistrict];
                for (int i = 0; i < customersPerDistrict; ++i) {
                    c_ids[i] = i + 1;
                }
                // Collections.shuffle exists, but there is no
                // Arrays.shuffle
                for (int i = 0; i < c_ids.length - 1; ++i) {
                    int remaining = c_ids.length - i - 1;
                    int swapIndex = benchmark.rng().nextInt(remaining) + i + 1;

                    int temp = c_ids[swapIndex];
                    c_ids[swapIndex] = c_ids[i];
                    c_ids[i] = temp;
                }

                for (int c = 1; c <= customersPerDistrict; c++) {

                    Oorder oorder = new Oorder();
                    oorder.o_id = c;
                    oorder.o_w_id = w_id;
                    oorder.o_d_id = d;
                    oorder.o_c_id = c_ids[c - 1];
                    // o_carrier_id is set *only* for orders with ids < 2101
                    // [4.3.3.1]
                    if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
                        oorder.o_carrier_id = GeoCUtil.randomNumber(1, 10, benchmark.rng());
                    } else {
                        oorder.o_carrier_id = null;
                    }
                    oorder.o_ol_cnt = getRandomCount(w_id, c, d);
                    oorder.o_all_local = 1;
                    oorder.o_entry_d = new Timestamp(System.currentTimeMillis());


                    int idx = 1;
                    openOrderStatement.setInt(idx++, oorder.o_w_id);
                    openOrderStatement.setInt(idx++, oorder.o_d_id);
                    openOrderStatement.setInt(idx++, oorder.o_id);
                    openOrderStatement.setInt(idx++, oorder.o_c_id);
                    if (oorder.o_carrier_id != null) {
                        openOrderStatement.setInt(idx++, oorder.o_carrier_id);
                    } else {
                        openOrderStatement.setNull(idx++, Types.INTEGER);
                    }
                    openOrderStatement.setInt(idx++, oorder.o_ol_cnt);
                    openOrderStatement.setInt(idx++, oorder.o_all_local);
                    openOrderStatement.setTimestamp(idx, oorder.o_entry_d);
                    openOrderStatement.addBatch();

                    k++;

                    if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                        openOrderStatement.executeBatch();
                        openOrderStatement.clearBatch();
                    }

                }

            }

            openOrderStatement.executeBatch();
            openOrderStatement.clearBatch();

        } catch (SQLException se) {
            LOG.error(se.getMessage(), se);
        }

    }

    private int getRandomCount(int w_id, int c, int d) {
        Customer customer = new Customer();
        customer.c_id = c;
        customer.c_d_id = d;
        customer.c_w_id = w_id;

        Random random = new Random(customer.hashCode());

        return GeoCUtil.randomNumber(5, 15, random);
    }

    protected void loadNewOrders(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

        int k = 0;

        try (PreparedStatement newOrderStatement = getInsertStatement(conn, GeoCConstants.TABLENAME_NEWORDER)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {

                for (int c = 1; c <= customersPerDistrict; c++) {

                    // 900 rows in the NEW-ORDER table corresponding to the last
                    // 900 rows in the ORDER table for that district (i.e.,
                    // with NO_O_ID between 2,101 and 3,000)
                    if (c >= FIRST_UNPROCESSED_O_ID) {
                        NewOrder new_order = new NewOrder();
                        new_order.no_w_id = w_id;
                        new_order.no_d_id = d;
                        new_order.no_o_id = c;

                        int idx = 1;
                        newOrderStatement.setInt(idx++, new_order.no_w_id);
                        newOrderStatement.setInt(idx++, new_order.no_d_id);
                        newOrderStatement.setInt(idx, new_order.no_o_id);
                        newOrderStatement.addBatch();

                        k++;
                    }

                    if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                        newOrderStatement.executeBatch();
                        newOrderStatement.clearBatch();
                    }

                }

            }

            newOrderStatement.executeBatch();
            newOrderStatement.clearBatch();

        } catch (SQLException se) {
            LOG.error(se.getMessage(), se);
        }

    }

    protected void loadOrderLines(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict, int whseSpecificItems) {

        int k = 0;

        try (PreparedStatement orderLineStatement = getInsertStatement(conn, GeoCConstants.TABLENAME_ORDERLINE)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {

                for (int c = 1; c <= customersPerDistrict; c++) {

                    int count = getRandomCount(w_id, c, d);

                    for (int l = 1; l <= count; l++) {
                        OrderLine order_line = new OrderLine();
                        order_line.ol_w_id = w_id;
                        order_line.ol_d_id = d;
                        order_line.ol_o_id = c;
                        order_line.ol_number = l; // ol_number
                        order_line.ol_i_id = GeoCUtil.randomNumber(1, GeoCConfig.configItemCount, benchmark.rng());
                        
                        // Keep generating an item ID while the item is specific to a different warehouse.
                        while (order_line.ol_i_id <= whseSpecificItems && (order_line.ol_i_id % numWarehouses) + 1 != w_id) {
                            order_line.ol_i_id = GeoCUtil.randomNumber(1, GeoCConfig.configItemCount, benchmark.rng());
                        }
                        
                        if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
                            order_line.ol_delivery_d = new Timestamp(System.currentTimeMillis());
                            order_line.ol_amount = 0;
                        } else {
                            order_line.ol_delivery_d = null;
                            // random within [0.01 .. 9,999.99]
                            order_line.ol_amount = (float) (GeoCUtil.randomNumber(1, 999999, benchmark.rng()) / 100.0);
                        }
                        order_line.ol_supply_w_id = order_line.ol_w_id;
                        order_line.ol_quantity = 5;
                        order_line.ol_dist_info = GeoCUtil.randomStr(24);

                        int idx = 1;
                        orderLineStatement.setInt(idx++, order_line.ol_w_id);
                        orderLineStatement.setInt(idx++, order_line.ol_d_id);
                        orderLineStatement.setInt(idx++, order_line.ol_o_id);
                        orderLineStatement.setInt(idx++, order_line.ol_number);
                        orderLineStatement.setLong(idx++, order_line.ol_i_id);
                        if (order_line.ol_delivery_d != null) {
                            orderLineStatement.setTimestamp(idx++, order_line.ol_delivery_d);
                        } else {
                            orderLineStatement.setNull(idx++, 0);
                        }
                        orderLineStatement.setDouble(idx++, order_line.ol_amount);
                        orderLineStatement.setLong(idx++, order_line.ol_supply_w_id);
                        orderLineStatement.setDouble(idx++, order_line.ol_quantity);
                        orderLineStatement.setString(idx, order_line.ol_dist_info);
                        orderLineStatement.addBatch();

                        k++;

                        if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                            orderLineStatement.executeBatch();
                            orderLineStatement.clearBatch();
                        }

                    }

                }

            }

            orderLineStatement.executeBatch();
            orderLineStatement.clearBatch();

        } catch (SQLException se) {
            LOG.error(se.getMessage(), se);
        }

    }

}
