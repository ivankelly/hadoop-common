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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FilterInputStream;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.EOFException;
import java.io.DataInputStream;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.Storage;

import com.google.common.annotations.VisibleForTesting;

import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

/**
 * An implementation of the abstract class {@link EditLogInputStream}, which
 * reads edits from a local file.
 */
class EditLogFileInputStream extends EditLogInputStream {
  private File file;
  private FileInputStream fStream;
  private int logVersion = 0;
  private FSEditLogOp.Reader reader = null;
  private FSEditLogLoader.PositionTrackingInputStream tracker = null;

  EditLogFileInputStream(File name) throws IOException {
    file = name;
    fStream = new FileInputStream(name);

    BufferedInputStream bin = new BufferedInputStream(fStream);
    DataInputStream in = new DataInputStream(bin);
    logVersion = readLogVersion(in);

    tracker = new FSEditLogLoader.PositionTrackingInputStream(in);
    in = new DataInputStream(tracker);
    
    reader = new FSEditLogOp.Reader(in, logVersion);
  }

  @Override // JournalStream
  public String getName() {
    return file.getPath();
  }

  @Override // JournalStream
  public JournalType getType() {
    return JournalType.FILE;
  }

  @Override
  public FSEditLogOp readOp() throws IOException {
    return reader.readOp();
  }

  @Override
  public int getVersion() throws IOException {
    return logVersion;
  }

  @Override
  public long getPosition() {
    return tracker.getPos();
  }

  @Override
  public void close() throws IOException {
    fStream.close();
  }

  @Override
  long length() throws IOException {
    // file size + size of both buffers
    return file.length();
  }
  
  @Override
  public String toString() {
    return getName();
  }

  /**
   * Read the header of fsedit log
   * @param in fsedit stream
   * @return the edit log version number
   * @throws IOException if error occurs
   */
  @VisibleForTesting
  static int readLogVersion(DataInputStream in) throws IOException {
    int logVersion = 0;
    // Read log file version. Could be missing.
    in.mark(4);
    // If edits log is greater than 2G, available method will return negative
    // numbers, so we avoid having to call available
    boolean available = true;
    try {
      logVersion = in.readByte();
    } catch (EOFException e) {
      available = false;
    }
    if (available) {
      in.reset();
      logVersion = in.readInt();
      if (logVersion < FSConstants.LAYOUT_VERSION) // future version
        throw new IOException(
            "Unexpected version of the file system log file: "
            + logVersion + ". Current version = "
            + FSConstants.LAYOUT_VERSION + ".");
    }
    assert logVersion <= Storage.LAST_UPGRADABLE_LAYOUT_VERSION :
      "Unsupported version " + logVersion;
    return logVersion;
  }
}
