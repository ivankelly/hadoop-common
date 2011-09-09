/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import java.net.URI;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.bookkeeper.util.LocalBookKeeper;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FilenameFilter;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.setupEdits;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.AbortSpec;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.TXNS_PER_ROLL;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.TXNS_PER_FAIL;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;

import org.apache.hadoop.hdfs.server.namenode.bkjournal.BookKeeperJournalManager;
import com.google.common.collect.ImmutableList;

import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestBookKeeperJournalManager {
  static final Log LOG = LogFactory.getLog(TestBookKeeperJournalManager.class);
  
  private static final long DEFAULT_SEGMENT_SIZE = 1000;

  private static Thread bkthread;
  protected static Configuration conf = new Configuration();
  private ZooKeeper zkc;

  @BeforeClass
  public static void setupBookkeeper() throws Exception {
    bkthread = new Thread() {
        public void run() {
          try {
            String[] args = new String[1];
            args[0] = "5";
            LOG.info("Starting bk");
            LocalBookKeeper.main(args);
          } catch (InterruptedException e) {
            // go away quietly
          } catch (Exception e) {
            LOG.error("Error starting local bk", e);
          }
        }
      };
    bkthread.start();
    
    if (!LocalBookKeeper.waitForServerUp("localhost:2181", 10000)) {
      throw new Exception("Error starting zookeeper/bookkeeper");
    }
    Thread.sleep(10);
    conf.set(BookKeeperJournalManager.BKJM_ZOOKEEPER_QUORUM, "localhost:2181");
  }
  
  @Before
  public void setup() throws Exception {
    zkc = new ZooKeeper("localhost:2181", 3600, new Watcher() {
        public void process(WatchedEvent event) {}
      });
  }

  @After
  public void teardown() throws Exception {
    zkc.close();
  }

  @AfterClass
  public static void teardownBookkeeper() throws Exception {
    if (bkthread != null) {
      bkthread.interrupt();
      bkthread.join();
    }
  }

  @Test
  public void testSimpleWrite() throws Exception {
    conf.set(BookKeeperJournalManager.BKJM_ZOOKEEPER_PATH, "/hdfsjournal-simplewrite");
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
                                                                 URI.create("bookkeeper://foobar"));
    long txid = 1;
    EditLogOutputStream out = bkjm.startLogSegment(1);
    for (long i = 1 ; i <= 100; i++) {
      FSEditLogOp op = FSEditLogOp.NoOp.getInstance(FSEditLogOpCodes.OP_END_LOG_SEGMENT);
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(1, 100);
 
    String zkpath = bkjm.finalizedLedgerZNode(1, 100);
    
    assertNotNull(zkc.exists(zkpath, false));
    assertNull(zkc.exists(bkjm.inprogressZNode(), false));
  }

  @Test
  public void testNumberOfTransactions() throws Exception {
    conf.set(BookKeeperJournalManager.BKJM_ZOOKEEPER_PATH, "/hdfsjournal-txncount");
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, 
                                                                 URI.create("bookkeeper://foobar"));
    long txid = 1;
    EditLogOutputStream out = bkjm.startLogSegment(1);
    for (long i = 1 ; i <= 100; i++) {
      FSEditLogOp op = FSEditLogOp.NoOp.getInstance(FSEditLogOpCodes.OP_END_LOG_SEGMENT);
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(1, 100);

    long numTrans = bkjm.getNumberOfTransactions(1);
    assertEquals(100, numTrans);
  }

  @Test 
  public void testNumberOfTransactionsWithGaps() throws Exception {
    conf.set(BookKeeperJournalManager.BKJM_ZOOKEEPER_PATH, "/hdfsjournal-gaps");
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, 
                                                                 URI.create("bookkeeper://foobar"));
    long txid = 1;
    for (long i = 0; i < 3; i++) {
      long start = txid;
      EditLogOutputStream out = bkjm.startLogSegment(start);
      for (long j = 1 ; j <= DEFAULT_SEGMENT_SIZE; j++) {
        FSEditLogOp op = FSEditLogOp.NoOp.getInstance(FSEditLogOpCodes.OP_END_LOG_SEGMENT);
        op.setTransactionId(txid++);
        out.write(op);
      }
      out.close();
      bkjm.finalizeLogSegment(start, txid-1);
      assertNotNull(zkc.exists(bkjm.finalizedLedgerZNode(start, txid-1), false));
    }
    zkc.delete(bkjm.finalizedLedgerZNode(DEFAULT_SEGMENT_SIZE+1, DEFAULT_SEGMENT_SIZE*2), -1);
    
    long numTrans = bkjm.getNumberOfTransactions(1);
    assertEquals(DEFAULT_SEGMENT_SIZE, numTrans);
    
    try {
      numTrans = bkjm.getNumberOfTransactions(DEFAULT_SEGMENT_SIZE+1);
      fail("Should have thrown corruption exception by this point");
    } catch (JournalManager.CorruptionException ce) {
      // if we get here, everything is going good
    }

    numTrans = bkjm.getNumberOfTransactions((DEFAULT_SEGMENT_SIZE*2)+1);
    assertEquals(DEFAULT_SEGMENT_SIZE, numTrans);
  }

  @Test
  public void testNumberOfTransactionsWithInprogressAtEnd() throws Exception {
    conf.set(BookKeeperJournalManager.BKJM_ZOOKEEPER_PATH, "/hdfsjournal-inprogressAtEnd");
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, 
                                                                 URI.create("bookkeeper://foobar"));
    long txid = 1;
    for (long i = 0; i < 3; i++) {
      long start = txid;
      EditLogOutputStream out = bkjm.startLogSegment(start);
      for (long j = 1 ; j <= DEFAULT_SEGMENT_SIZE; j++) {
        FSEditLogOp op = FSEditLogOp.NoOp.getInstance(FSEditLogOpCodes.OP_END_LOG_SEGMENT);
        op.setTransactionId(txid++);
        out.write(op);
      }
      
      out.close();
      bkjm.finalizeLogSegment(start, (txid-1));
      assertNotNull(zkc.exists(bkjm.finalizedLedgerZNode(start, (txid-1)), false));
    }
    long start = txid;
    EditLogOutputStream out = bkjm.startLogSegment(start);
    for (long j = 1 ; j <= DEFAULT_SEGMENT_SIZE/2; j++) {
      FSEditLogOp op = FSEditLogOp.NoOp.getInstance(FSEditLogOpCodes.OP_END_LOG_SEGMENT);
      op.setTransactionId(txid++);
      out.write(op);
    }
    out.setReadyToFlush();
    out.flushAndSync();
    out.abort();
    out.close();
    
    long numTrans = bkjm.getNumberOfTransactions(1);
    assertEquals((txid-1), numTrans);
  }

  /**
   * Create a bkjm namespace, write a journal from txid 1, close stream.
   * Try to create a new journal from txid 1. Should throw an exception.
   */
  @Test
  public void testWriteRestartFrom1() throws Exception {
    conf.set(BookKeeperJournalManager.BKJM_ZOOKEEPER_PATH, "/hdfsjournal-restartFrom1");
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, 
                                                                 URI.create("bookkeeper://foobar"));
    long txid = 1;
    long start = txid;
    EditLogOutputStream out = bkjm.startLogSegment(txid);
    for (long j = 1 ; j <= DEFAULT_SEGMENT_SIZE; j++) {
      FSEditLogOp op = FSEditLogOp.NoOp.getInstance(FSEditLogOpCodes.OP_END_LOG_SEGMENT);
      op.setTransactionId(txid++);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(start, (txid-1));
    
    txid = 1;
    try {
      out = bkjm.startLogSegment(txid);
      fail("Shouldn't be able to start another journal from " + txid
          + " when one already exists");
    } catch (Exception ioe) {
      LOG.info("Caught exception as expected", ioe);
    }

    // test border case
    txid = DEFAULT_SEGMENT_SIZE;
    try {
      out = bkjm.startLogSegment(txid);
      fail("Shouldn't be able to start another journal from " + txid
          + " when one already exists");
    } catch (IOException ioe) {
      LOG.info("Caught exception as expected", ioe);
    }

    // open journal continuing from before
    txid = DEFAULT_SEGMENT_SIZE + 1;
    start = txid;
    out = bkjm.startLogSegment(start);
    assertNotNull(out);

    for (long j = 1 ; j <= DEFAULT_SEGMENT_SIZE; j++) {
      FSEditLogOp op = FSEditLogOp.NoOp.getInstance(FSEditLogOpCodes.OP_END_LOG_SEGMENT);
      op.setTransactionId(txid++);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(start, (txid-1));

    // open journal arbitarily far in the future
    txid = DEFAULT_SEGMENT_SIZE * 4;
    out = bkjm.startLogSegment(txid);
    assertNotNull(out);
  }

  @Test
  public void testTwoWriters() throws Exception {
    conf.set(BookKeeperJournalManager.BKJM_ZOOKEEPER_PATH, "/hdfsjournal-dualWriter");
    long start = 1;
    BookKeeperJournalManager bkjm1 = new BookKeeperJournalManager(conf, 
                                                                  URI.create("bookkeeper://foobar"));
    BookKeeperJournalManager bkjm2 = new BookKeeperJournalManager(conf, 
                                                                  URI.create("bookkeeper://foobar"));
    
    EditLogOutputStream out1 = bkjm1.startLogSegment(start);
    try {
      EditLogOutputStream out2 = bkjm2.startLogSegment(start);
      fail("Shouldn't have been able to open the second writer");
    } catch (IOException ioe) {
      LOG.info("Caught exception as expected", ioe);
    }
  }

  @Test
  public void testSimpleRead() throws Exception {
    conf.set(BookKeeperJournalManager.BKJM_ZOOKEEPER_PATH, "/hdfsjournal-simpleread");
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, 
                                                                 URI.create("bookkeeper://foobar"));
    long txid = 1;
    final long numTransactions = 10000;
    EditLogOutputStream out = bkjm.startLogSegment(1);
    for (long i = 1 ; i <= numTransactions; i++) {
      FSEditLogOp op = FSEditLogOp.NoOp.getInstance(FSEditLogOpCodes.OP_END_LOG_SEGMENT);
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(1, numTransactions);

    EditLogInputStream in = bkjm.getInputStream(1);
    FSEditLogLoader.EditLogValidation validation;
    try {
      validation = FSEditLogLoader.validateEditLog(in);
    } finally {
      in.close();
    }
    assertEquals(numTransactions, validation.getNumTransactions());
  }

  @Test
  public void testSimpleRecovery() throws Exception {
    conf.set(BookKeeperJournalManager.BKJM_ZOOKEEPER_PATH, "/hdfsjournal-simplerecovery");
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, 
                                                                 URI.create("bookkeeper://foobar"));
    EditLogOutputStream out = bkjm.startLogSegment(1);
    long txid = 1;
    for (long i = 1 ; i <= 100; i++) {
      FSEditLogOp op = FSEditLogOp.NoOp.getInstance(FSEditLogOpCodes.OP_END_LOG_SEGMENT);
      op.setTransactionId(i);
      out.write(op);
    }
    out.setReadyToFlush();
    out.flushAndSync();

    out.abort();
    out.close();


    assertNull(zkc.exists(bkjm.finalizedLedgerZNode(1, 100), false));
    assertNotNull(zkc.exists(bkjm.inprogressZNode(), false));

    bkjm.recoverUnfinalizedSegments();

    assertNotNull(zkc.exists(bkjm.finalizedLedgerZNode(1, 100), false));
    assertNull(zkc.exists(bkjm.inprogressZNode(), false));
  }
}