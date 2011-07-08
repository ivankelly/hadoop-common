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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;


import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorageArchivalManager.StorageArchiver;
import org.apache.hadoop.hdfs.server.namenode.JournalManager.CorruptionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Journal manager for the common case of edits files being written
 * to a storage directory.
 *
 * Note: this class is not thread-safe and should be externally
 * synchronized.
 */
public class FileJournalManager implements JournalManager {
  private static final Log LOG = LogFactory.getLog(FileJournalManager.class);

  private final StorageDirectory sd;
  private int outputBufferCapacity = 512*1024;
  private static final Pattern EDITS_REGEX = Pattern.compile(
      NameNodeFile.EDITS.getName() + "_(\\d+)-(\\d+)");
  private static final Pattern EDITS_INPROGRESS_REGEX = Pattern.compile(
      NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+)");

  // Avoid counting the file more than once.
  private static Map<File, EditLogFile> inProgressCache 
    = new ConcurrentHashMap<File, EditLogFile>(0);

  public FileJournalManager(StorageDirectory sd) {
    this.sd = sd;
  }

  @Override
  public EditLogOutputStream startLogSegment(long txid) throws IOException {
    recoverUnclosedStreams();

    File newInProgress = NNStorage.getInProgressEditsFile(sd, txid);
    EditLogOutputStream stm = new EditLogFileOutputStream(newInProgress,
        outputBufferCapacity);
    stm.create();
    return stm;
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    File inProgressFile = NNStorage.getInProgressEditsFile(
        sd, firstTxId);
    File dstFile = NNStorage.getFinalizedEditsFile(
        sd, firstTxId, lastTxId);
    LOG.debug("Finalizing edits file " + inProgressFile + " -> " + dstFile);

    Preconditions.checkState(!dstFile.exists(),
        "Can't finalize edits file " + inprogressFile + " since finalized file " +
        "already exists");
    if (!inprogressFile.renameTo(dstFile)) {
      throw new IOException("Unable to finalize edits file " + inprogressFile);
    }
  }

  @VisibleForTesting
  public StorageDirectory getStorageDirectory() {
    return sd;
  }

  @Override
  public String toString() {
    return "FileJournalManager for storage directory " + sd;
  }

  @Override
  public void setOutputBufferCapacity(int size) {
    this.outputBufferCapacity = size;
  }

  @Override
  public void purgeTransactions(long minTxIdToKeep)
      throws IOException {
    StorageArchiver archiver 
      = new NNStorageArchivalManager.DeletionStorageArchiver();

    File[] files = FileUtil.listFiles(sd.getCurrentDir());
    List<EditLogFile> editLogs = matchEditLogs(files);
    for (EditLogFile log : editLogs) {
      if (log.startTxId < minTxIdToKeep &&
          log.endTxId < minTxIdToKeep) {
        archiver.archiveLog(log.file, log.startTxId, log.endTxId);
      }
    }
  }

  public EditLogInputStream getInputStream(long fromTxId) throws IOException {
    for (EditLogFile elf : getLogFiles(fromTxId)) {
      if (elf.startTxId == fromTxId) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Returning edit stream reading from " + elf.file);
        }
        return new EditLogFileInputStream(elf.file);
      }
    }

    throw new IOException("Cannot find editlog file with " + fromTxId
                          + " as first first txid");
  }

  private long getNumberOfTransactionsInternal(long fromTxId, boolean includeInProgress)
      throws IOException {
    long numTxns = 0L;

    for (EditLogFile elf : getLogFiles(fromTxId)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Counting " + elf);
      }
      if (elf.startTxId > fromTxId) { // there must be a gap
        LOG.warn("Gap in transactions "
                 + fromTxId + " - " + (elf.startTxId - 1));
      } else if (fromTxId == elf.startTxId) {
        if (elf.inProgress && includeInProgress) {
          elf = countTransactionsInInprogress(elf.file);
        } else {
          if (elf.inProgress) {
            break;
          }
        }

        if (elf.corrupt) {
          break;
        }
        fromTxId = elf.endTxId + 1;
        numTxns += fromTxId - elf.startTxId;
        
        if (elf.inProgress) {
          break;
        }
      } // else skip
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Journal " + this + " has " + numTxns 
                + " txns from " + fromTxId);
    }

    if (numTxns == 0 && fromTxId <= getMaxLoadableTransaction()) {
      String error = String.format("Gap in transactions, max txnid is %d"
          + ", 0 txns from %d", getMaxLoadableTransaction(), fromTxId);
      LOG.error(error);
      throw new CorruptionException(error);
    }

    return numTxns;
  }

  private long getMaxLoadableTransaction() 
      throws IOException {
    long max = 0L;
    for (EditLogFile elf : getLogFiles(0)) {
      if (elf.inProgress) {
        if (elf.startTxId > max) {
          max = elf.startTxId;
        }
        elf = countTransactionsInInprogress(elf.file);
      }
      if (elf.endTxId > max) {
        max = elf.endTxId;
      }
    }
    return max;
  }
  
  @Override
  public long getNumberOfTransactions(long fromTxId) throws IOException {
    return getNumberOfTransactionsInternal(fromTxId, true);
  }

  public long getNumberOfFinalizedTransactions(long fromTxId) 
      throws IOException {
    return getNumberOfTransactionsInternal(fromTxId, false);
  }

  private void recoverUnclosedStreams() throws IOException {
    File currentDir = sd.getCurrentDir();
    for (File f : currentDir.listFiles()) {
      // Check for in-progress edits
      Matcher inProgressEditsMatch
        = EDITS_INPROGRESS_REGEX.matcher(f.getName());
      if (inProgressEditsMatch.matches()) {
        EditLogFile elf 
          = countTransactionsInInprogress(f);

        if (elf.corrupt) {
          File src = f;
          File dst = new File(src.getParent(), src.getName() + ".corrupt");
          boolean success = src.renameTo(dst);
          if (!success) {
            LOG.error("Error moving corrupt file aside " + f);
          }
        } else {
          finalizeLogSegment(elf.startTxId, elf.endTxId);
        }
      }
    }
  }

  private EditLogFile countTransactionsInInprogress(File f) 
      throws IOException {
    synchronized(inProgressCache) {
      if (inProgressCache.containsKey(f)) {
        return inProgressCache.get(f);
      }
    }

    EditLogFileInputStream edits = new EditLogFileInputStream(f);
    FSEditLogLoader.EditLogValidation val 
      = FSEditLogLoader.validateEditLog(edits);
    
    EditLogFile elf = new EditLogFile(val.getStartTxId(), val.getEndTxId(), f, 
                                      true, val.getNumTransactions() == 0);
    synchronized(inProgressCache) {
      inProgressCache.put(f, elf);
    }
    return elf;
  }

  RemoteEditLog getRemoteEditLog(long fromTxId) throws IOException {
    List<RemoteEditLog> logs = new ArrayList<RemoteEditLog>();
    for (EditLogFile elf : getLogFiles(fromTxId)) {
      if (elf.startTxId == fromTxId) {
        return new RemoteEditLog(elf.startTxId,
                                 elf.endTxId);
      }
    }
    return null;
  }

  static List<EditLogFile> matchEditLogs(File[] filesInStorage) {
    List<EditLogFile> ret = Lists.newArrayList();
    for (File f : filesInStorage) {
      String name = f.getName();
      // Check for edits
      Matcher editsMatch = EDITS_REGEX.matcher(name);
      if (editsMatch.matches()) {
        try {
          long startTxId = Long.valueOf(editsMatch.group(1));
          long endTxId = Long.valueOf(editsMatch.group(2));
          ret.add(new EditLogFile(startTxId, endTxId, f));
        } catch (NumberFormatException nfe) {
          LOG.error("Edits file " + f + " has improperly formatted " +
                    "transaction ID");
          // skip
        }          
      }
      
      // Check for in-progress edits
      Matcher inProgressEditsMatch = EDITS_INPROGRESS_REGEX.matcher(name);
      if (inProgressEditsMatch.matches()) {
        try {
          long startTxId = Long.valueOf(inProgressEditsMatch.group(1));
          ret.add(
              new EditLogFile(startTxId, EditLogFile.UNKNOWN_TXID, f,
                              true, false));
        } catch (NumberFormatException nfe) {
          LOG.error("In-progress edits file " + f + " has improperly " +
                    "formatted transaction ID");
          // skip
        }          
      }
    }
    return ret;
  }

  List<EditLogFile> getLogFiles(long fromTxId) throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir.listFiles());
    List<EditLogFile> logFiles = new ArrayList<EditLogFile>();

    for (EditLogFile elf : allLogFiles) {
      if (fromTxId > elf.startTxId
          && fromTxId <= elf.endTxId) {
        throw new IOException("Asked for fromTxId " + fromTxId
            + " which is in middle of file " + elf.file);
      }
      if (fromTxId <= elf.startTxId) {
        logFiles.add(elf);
      }
    }

    Collections.sort(logFiles, EditLogFile.COMPARE_BY_START_TXID);

    return logFiles;
  }

  static class EditLogFile {
    final static long UNKNOWN_TXID = -1;
    final long startTxId;
    long endTxId;
    final File file;
    boolean inProgress;
    boolean corrupt = false;

    final static Comparator COMPARE_BY_START_TXID 
      = new Comparator<EditLogFile>() {
      public int compare(EditLogFile o1,
                         EditLogFile o2) {
        if (o1.startTxId < o2.startTxId) {
          return -1;
        } else if (o1.startTxId == o2.startTxId) {
          return 0;
        } else {
          return 1;
        }
      }
    };

    EditLogFile(long startTxId, long endTxId,
                File file, 
                boolean inProgress,
                boolean corrupt) {
      this.startTxId = startTxId;
      this.endTxId = endTxId;
      this.file = file;
      this.inProgress = inProgress;
      this.corrupt = corrupt;
    }

    EditLogFile(long startTxId, long endTxId, File file) {
      this(startTxId, endTxId, file, false, false);
    }    

    public String toString() {
      return String.format("EditLogFile(file=%s,s=%019d,e=%019d,"
          +"inProgress=%b,corrupt=%b)", file.toString(),
          startTxId, endTxId, inProgress, corrupt);
    }
  }
}
