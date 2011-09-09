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

import java.io.ByteArrayInputStream;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BookKeeperEditLogInputStream extends EditLogInputStream {
  static final Log LOG = LogFactory.getLog(BookKeeperEditLogInputStream.class);

  private final long firstTxId;
  private final long lastTxId;
  private final int logVersion;
  private static final long READ_BLOCK_SIZE = 100; //IKTODO make configurable
  final LedgerHandle lh;

  private final FSEditLogOp.Reader reader;
  private final FSEditLogLoader.PositionTrackingInputStream tracker;

  BookKeeperEditLogInputStream(LedgerHandle lh, 
                               int logVersion,
                               long firstTxId, 
                               long lastTxId) throws IOException{
    this.lh = lh;
    this.firstTxId = firstTxId;
    this.lastTxId = lastTxId;
    this.logVersion = logVersion;

    BufferedInputStream bin = new BufferedInputStream(new LedgerInputStream(lh));
    tracker = new FSEditLogLoader.PositionTrackingInputStream(bin);
    DataInputStream in = new DataInputStream(tracker);

    reader = new FSEditLogOp.Reader(in, logVersion);
  }

  public long getFirstTxId() throws IOException {
    return firstTxId;
  }

  public long getLastTxId() throws IOException {
    return lastTxId;
  }
  
  @Override
  public int getVersion() throws IOException {
    return logVersion;
  }


  public int available() throws IOException {
    throw new IOException("Doesn't make sense");
  }

  @Override
  public FSEditLogOp readOp() throws IOException {
    FSEditLogOp op = reader.readOp();
    LOG.info("IKTODO txid " + op.txid + " pos " + getPosition());
    return op;
  }


  public void close() throws IOException {
    try {
      lh.close();
    } catch (Exception e) {
      throw new IOException("Exception closing ledger", e);
    }
  }

  public long getPosition() {
    return tracker.getPos();
  }

  public long length() throws IOException {
    return lh.getLength();
  }
  
  @Override
  public String getName() {
    return String.format("BookKeeper[%s,first=%ld,last=%ld]", 
        lh.toString(), firstTxId, lastTxId);
  }

  @Override
  public JournalType getType() {
    assert (false);
    return null;
  }

  private static class LedgerInputStream extends InputStream {
    private long readEntries = 0;
    InputStream entryStream = null;
    final LedgerHandle lh;
    private final long maxEntry;

    LedgerInputStream(LedgerHandle lh) throws IOException {
      this.lh = lh;
      try {
        maxEntry = lh.getLastAddConfirmed();
        LOG.info("IKTODO maxEntry " + maxEntry);
      } catch (Exception e) {
        throw new IOException("Error reading last entry id", e);
      }
    }
    
    private InputStream nextStream() throws IOException {
      LOG.info("IKTODO nextStream");
      try {        
        if (readEntries > maxEntry) {
          return null;
        }
        Enumeration<LedgerEntry> entries = lh.readEntries(readEntries, readEntries);
        readEntries++;
        if (entries.hasMoreElements()) {
            LedgerEntry e = entries.nextElement();
            assert !entries.hasMoreElements();
            return e.getEntryInputStream();
        }
      } catch (Exception e) {
        throw new IOException("Error reading entries from bookkeeper", e);
      }
      return null;
    }

    public int read() throws IOException {
      byte[] b = new byte[4];
      read(b, 0, 4);
      return (b[0] << 3 | b[1] << 2 | b[2] << 1 | b[3]);
    }
    
    public int read(byte[] b, int off, int len) throws IOException {
      try {
      LOG.info("IKTODO read " + len);
      int read = 0;
      if (entryStream == null) {
        entryStream = nextStream();
      }

      while (read < len) {
        int thisread = entryStream.read(b, off+read, (len-read));
        LOG.info("IKTODO thisread " + thisread);
        if (thisread == -1) {
          entryStream = nextStream();
          if (entryStream == null) {
            LOG.info("IKTODO read1 " + read);
                  
            return read;
          }
        } else {
          read += thisread;
        }
      }
      LOG.info("IKTODO read2 " + read);
            return read;
      } catch (IOException e) {
        LOG.info("IKTODO", e);
        throw e;
      }

    }
  }
}
