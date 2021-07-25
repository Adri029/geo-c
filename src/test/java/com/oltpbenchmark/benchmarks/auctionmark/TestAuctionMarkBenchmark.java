///******************************************************************************
// *  Copyright 2015 by OLTPBenchmark Project                                   *
// *                                                                            *
// *  Licensed under the Apache License, Version 2.0 (the "License");           *
// *  you may not use this file except in compliance with the License.          *
// *  You may obtain a copy of the License at                                   *
// *                                                                            *
// *    http://www.apache.org/licenses/LICENSE-2.0                              *
// *                                                                            *
// *  Unless required by applicable law or agreed to in writing, software       *
// *  distributed under the License is distributed on an "AS IS" BASIS,         *
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
// *  See the License for the specific language governing permissions and       *
// *  limitations under the License.                                            *
// ******************************************************************************/
//
//
//package com.oltpbenchmark.benchmarks.auctionmark;
//
//import java.io.File;
//
//import com.oltpbenchmark.api.AbstractTestBenchmarkModule;
//import com.oltpbenchmark.benchmarks.auctionmark.procedures.*;
//
//public class TestAuctionMarkBenchmark extends AbstractTestBenchmarkModule<AuctionMarkBenchmark> {
//
//    public static final Class<?> PROC_CLASSES[] = {
//        GetItem.class,
//        GetUserInfo.class,
//        NewBid.class,
//        NewComment.class,
//        NewCommentResponse.class,
//        NewFeedback.class,
//        NewItem.class,
//        NewPurchase.class,
//        UpdateItem.class
//    };
//
//	@Override
//	protected void setUp() throws Exception {
//		super.setUp(AuctionMarkBenchmark.class, PROC_CLASSES);
//		AuctionMarkProfile.clearCachedProfile();
//	}
//
//	/**
//	 * testGetDataDir
//	 */
//	public void testGetDataDir() throws Exception {
//	    File data_dir = this.benchmark.getDataDir();
//	    System.err.println("Data Dir: " + data_dir);
//	    assertNotNull(data_dir);
//	    assertTrue(data_dir.exists());
//	}
//
////	/**
////	 * testSupplementalClasses
////	 */
////	public void testSupplementalClasses() throws Exception {
////	    // Check to make sure that we have something...
////	    Map<TransactionType, Procedure> procs = this.benchmark.getProcedures();
////	    assertNotNull(procs);
////	    System.err.println("*****************");
////	    System.err.println(StringUtil.formatMaps(procs));
////	    System.err.println("*****************");
////	}
//
//}
