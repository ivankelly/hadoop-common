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
package org.apache.hadoop.hdfs.server.namenode.bkjournal;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;

import java.util.Arrays;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.Writer;

import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DataOutputBuffer;
import java.io.IOException;

import java.util.zip.Checksum;

class BookKeeperEditLogOutputStream extends EditLogOutputStream implements AddCallback {
  private final DataOutputBuffer bufCurrent;
  private final AtomicInteger outstandingRequests;
  private static final int DEFAULT_TRANSMISSION_THRESHOLD = 1024;
  private final int transmissionThreshold;
  private final LedgerHandle lh;
  private CountDownLatch syncLatch;
  private final WriteLock wl;
  private final Writer writer;

  protected BookKeeperEditLogOutputStream(Configuration conf,
                                          LedgerHandle lh, WriteLock wl) throws IOException {
    super();
    
    bufCurrent = new DataOutputBuffer();
    outstandingRequests = new AtomicInteger(0);;
    syncLatch = null;
    this.lh = lh;
    this.wl = wl;
    this.wl.acquire();
    this.writer = new Writer(bufCurrent);
    this.transmissionThreshold = conf.getInt(BookKeeperJournalManager.BKJM_OUTPUT_BUFFER_SIZE, 
                                             DEFAULT_TRANSMISSION_THRESHOLD);
  }

  @Override
  public void create() throws IOException {
    // noop
  }

  @Override
  public void close() throws IOException {
    setReadyToFlush();
    flushAndSync();
    try {
      lh.close();
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted waiting on close", ie);
    }
  }

  @Override
  public void abort() throws IOException {
    try {
      lh.close();
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted waiting on close", ie);
    }

    wl.release();
  }

  @Override
  public void writeRaw(final byte[] data, int off, int len) throws IOException {
    throw new IOException("Not supported for BK");
  }

  @Override
  public void write(FSEditLogOp op) throws IOException {
    wl.checkWriteLock();

    writer.writeOp(op);
    
    if (bufCurrent.getLength() > transmissionThreshold) {
      transmit();
    }
  }

  @Override
  public void setReadyToFlush() throws IOException {
    wl.checkWriteLock();

    transmit();
    
    synchronized(outstandingRequests) {
      syncLatch = new CountDownLatch(outstandingRequests.get());
    }
  }

  @Override
  public void flushAndSync() throws IOException {
    wl.checkWriteLock();
        
    assert(syncLatch != null);
    try {
      syncLatch.await();
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted waiting on latch", ie);
    }

    syncLatch = null;
    // wait for whatever we wait on
  }
  
  /**
   * Transmit the current buffer to bookkeeper.
   * Synchronised at the FSEditLog level. #write() and #setReadyToFlush() 
   * are never called at the same time.
   */
  private void transmit() throws IOException {
    wl.checkWriteLock();

    if (bufCurrent.getLength() > 0) {
      byte[] entry = Arrays.copyOf(bufCurrent.getData(), bufCurrent.getLength());
      lh.asyncAddEntry(entry, this, null);
      bufCurrent.reset();
      outstandingRequests.incrementAndGet();
    }
  }

  public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
    synchronized(outstandingRequests) {
      outstandingRequests.decrementAndGet();
      CountDownLatch l = syncLatch;
      if (l != null) {
        l.countDown();
      }
    }
  }

  @Override
  public long length() throws IOException {
    return lh.getLength();
  }

  @Override
  public String getName() {
    return "IKFIXME";
  }

  @Override
  public JournalType getType() {
    assert (false);
    return null;
  }
}
