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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.Writable;

import java.net.URI;
import java.io.IOException;



public class TestGenericJournalConf {
  /** 
   * Test that an exception is thrown if a journal class doesn't exist
   * in the configuration 
   */
  @Test(expected=IllegalArgumentException.class)
  public void testNotConfigured() throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();

    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
             "dummy://test");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    cluster.shutdown();
  }

  /**
   * Test that an exception is thrown if a journal class doesn't
   * exist in the classloader.
   */
  @Test(expected=IllegalArgumentException.class)
  public void testClassDoesntExist() throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();

    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_PLUGIN_BASE + ".dummy",
             "org.apache.hadoop.nonexistent");
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
             "dummy://test");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    cluster.shutdown();
  }

  /**
   * Test that a implementation of JournalManager wihtout a 
   * (Configuration,URI) constructor throws an exception
   */
  @Test(expected=IllegalArgumentException.class)
  public void testBadConstructor() throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();

    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_PLUGIN_BASE + ".dummy",
             BadConstructorJournalManager.class.getCanonicalName());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
             "dummy://test");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    cluster.shutdown();
  }

  /**
   * Test that a dummy implementation of JournalManager can
   * be initialised on startup
   */
  @Test
  public void testDummyJournalManager() throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();

    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_PLUGIN_BASE + ".dummy",
             DummyJournalManager.class.getCanonicalName());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
             "dummy://test");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    cluster.shutdown();
  }
}

class DummyJournalManager implements JournalManager {
  public DummyJournalManager(Configuration conf, URI u) {}

  @Override
  public EditLogOutputStream startLogSegment(long txId) throws IOException {
    return new DummyEditLogOutputStream();
  }
    
  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId) throws IOException {
    // noop
  }

  @Override
  public EditLogInputStream getInputStream(long fromTxnId) throws IOException {
    return null;
  }

  @Override
  public long getNumberOfTransactions(long fromTxnId) 
      throws IOException {
    return 0;
  }

  @Override
  public void setOutputBufferCapacity(int size) {}

  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep)
      throws IOException {}

  @Override
  public void recoverUnfinalizedSegments() throws IOException {}

  @Override
  public void close() throws IOException {}
}

class BadConstructorJournalManager extends DummyJournalManager {
  public BadConstructorJournalManager() {
    super(null, null);
  }
}

class DummyEditLogOutputStream extends EditLogOutputStream {
  DummyEditLogOutputStream() throws IOException {
    super();
  }

  @Override
  public void write(FSEditLogOp op) throws IOException {}

  @Override
  public void writeRaw(byte[] bytes, int offset, int length)
      throws IOException {}

  @Override
  public void create() throws IOException {}

  @Override
  public void close() throws IOException {}

  @Override
  public void abort() throws IOException {}

  @Override
  public void setReadyToFlush() throws IOException {}

  @Override
  protected void flushAndSync() throws IOException {}
}
