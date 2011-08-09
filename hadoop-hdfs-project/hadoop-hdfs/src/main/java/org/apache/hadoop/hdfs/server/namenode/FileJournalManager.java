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
import java.util.HashMap;
import java.util.Comparator;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorageRetentionManager.StoragePurger;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.EditLogValidation;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.ComparisonChain;

/**
 * Journal manager for the common case of edits files being written
 * to a storage directory.
 * 
 * Note: this class is not thread-safe and should be externally
 * synchronized.
 */
class FileJournalManager implements JournalManager {
  private static final Log LOG = LogFactory.getLog(FileJournalManager.class);

  private final StorageDirectory sd;
  private int outputBufferCapacity = 512*1024;

  private static final Pattern EDITS_REGEX = Pattern.compile(
    NameNodeFile.EDITS.getName() + "_(\\d+)-(\\d+)");
  private static final Pattern EDITS_INPROGRESS_REGEX = Pattern.compile(
    NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+)");

  private final HashMap<File,EditLogFile> validatedEditLogFiles
    = new HashMap<File,EditLogFile>();
  private File currentInProgress = null;
  @VisibleForTesting
  StoragePurger purger
    = new NNStorageRetentionManager.DeletionStoragePurger();

  public FileJournalManager(StorageDirectory sd) {
    this.sd = sd;
  }

  @Override
  synchronized public EditLogOutputStream startLogSegment(long txid) 
      throws IOException {
    currentInProgress = NNStorage.getInProgressEditsFile(sd, txid);
    EditLogOutputStream stm = new EditLogFileOutputStream(currentInProgress,
        outputBufferCapacity);
    stm.create();
    return stm;
  }

  @Override
  synchronized public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    File inprogressFile = currentInProgress;
    if (inprogressFile == null) { // in the case of recovery
      inprogressFile = NNStorage.getInProgressEditsFile(sd, firstTxId);
    }

    File dstFile = NNStorage.getFinalizedEditsFile(
        sd, firstTxId, lastTxId);
    LOG.debug("Finalizing edits file " + inprogressFile + " -> " + dstFile);
    
    Preconditions.checkState(!dstFile.exists(),
        "Can't finalize edits file " + inprogressFile + " since finalized file " +
        "already exists");
    if (!inprogressFile.renameTo(dstFile)) {
      throw new IOException("Unable to finalize edits file " + inprogressFile);
    }
    currentInProgress = null;
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
  public void purgeLogsOlderThan(long minTxIdToKeep)
      throws IOException {
    File[] files = FileUtil.listFiles(sd.getCurrentDir());
    List<EditLogFile> editLogs = 
      FileJournalManager.matchEditLogs(files);
    for (EditLogFile log : editLogs) {
      if (log.getFirstTxId() < minTxIdToKeep &&
          log.getLastTxId() < minTxIdToKeep) {
        purger.purgeLog(log);
      }
    }
  }

  /**
   * Find all editlog segments starting at or above the given txid.
   * @param fromTxId the txnid which to start looking
   * @return a list of remote edit logs
   * @throws IOException if edit logs cannot be listed.
   */
  List<RemoteEditLog> getRemoteEditLogs(long firstTxId) throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(
        FileUtil.listFiles(currentDir));
    List<RemoteEditLog> ret = Lists.newArrayListWithCapacity(
        allLogFiles.size());

    for (EditLogFile elf : allLogFiles) {
      if (elf.isCorrupt() || elf.isInProgress()) continue;
      if (elf.getFirstTxId() >= firstTxId) {
        ret.add(new RemoteEditLog(elf.firstTxId, elf.lastTxId));
      } else if ((firstTxId > elf.getFirstTxId()) &&
                 (firstTxId <= elf.getLastTxId())) {
        throw new IOException("Asked for firstTxId " + firstTxId
            + " which is in the middle of file " + elf.file);
      }
    }
    
    return ret;
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
          ret.add(new EditLogFile(f, startTxId, endTxId));
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
              new EditLogFile(f, startTxId, startTxId, true));
        } catch (NumberFormatException nfe) {
          LOG.error("In-progress edits file " + f + " has improperly " +
                    "formatted transaction ID");
          // skip
        }
      }
    }
    return ret;
  }

  @Override
  synchronized public EditLogInputStream getInputStream(long fromTxId) 
      throws IOException {
    for (EditLogFile elf : getLogFiles(fromTxId)) {
      if (elf.getFirstTxId() == fromTxId) {
        if (elf.isInProgress()) {
          elf = countTransactionsInInprogress(elf);
          if (elf.isCorrupt()) {
            elf.moveAsideCorruptFile();
            break;
          } else if (!elf.getFile().equals(currentInProgress)) {
            finalizeLogSegment(elf.getFirstTxId(), elf.getLastTxId());
            return getInputStream(fromTxId);
          }
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace("Returning edit stream reading from " + elf.getFile());
        }
        return new EditLogFileInputStream(elf.getFile(), 
            elf.getFirstTxId(), elf.getLastTxId());
      }
    }

    throw new IOException("Cannot find editlog file with " + fromTxId
        + " as first first txid");
  }

  @Override
  public long getNumberOfTransactions(long fromTxId) 
      throws IOException, CorruptionException {
    long numTxns = 0L;
    
    for (EditLogFile elf : getLogFiles(fromTxId)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Counting " + elf);
      }
      if (elf.getFirstTxId() > fromTxId) { // there must be a gap
        LOG.warn("Gap in transactions "
            + fromTxId + " - " + (elf.getFirstTxId() - 1));
      } else if (fromTxId == elf.getFirstTxId()) {
        if (elf.isInProgress()) {
          elf = countTransactionsInInprogress(elf);
        } 

        if (elf.isCorrupt()) {
          break;
        }
        fromTxId = elf.getLastTxId() + 1;
        numTxns += fromTxId - elf.getFirstTxId();
        
        if (elf.isInProgress()) {
          break;
        }
      } // else skip
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Journal " + this + " has " + numTxns 
                + " txns from " + fromTxId);
    }

    long max = getMaxLoadableTransaction();
    // fromTxId should be greater than max, as it points to the next 
    // transaction we should expect to find. If it is less than or equal
    // to max, it means that a transaction with txid == max has not been found
    if (numTxns == 0 && fromTxId <= max) { 
      String error = String.format("Gap in transactions, max txnid is %d"
          + ", 0 txns from %d", max, fromTxId);
      LOG.error(error);
      throw new CorruptionException(error);
    }

    return numTxns;
  }

  private List<EditLogFile> getLogFiles(long fromTxId) throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir.listFiles());
    List<EditLogFile> logFiles = Lists.newArrayList();
    
    for (EditLogFile elf : allLogFiles) {
      if (fromTxId > elf.getFirstTxId()
          && fromTxId <= elf.getLastTxId()) {
        throw new IOException("Asked for fromTxId " + fromTxId
            + " which is in middle of file " + elf.file);
      }
      if (fromTxId <= elf.getFirstTxId()) {
        logFiles.add(elf);
      }
    }
    
    Collections.sort(logFiles, EditLogFile.COMPARE_BY_START_TXID);

    return logFiles;
  }

  private long getMaxLoadableTransaction()
      throws IOException {
    long max = 0L;
    for (EditLogFile elf : getLogFiles(0)) {
      if (elf.isInProgress()) {
        max = Math.max(elf.getFirstTxId(), max);
        elf = countTransactionsInInprogress(elf);
      }
      max = Math.max(elf.getLastTxId(), max);
    }
    return max;
  }

  private void recoverUnclosedStreams() throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> logFiles = matchEditLogs(currentDir.listFiles());
    for (EditLogFile f : logFiles) {
      if (f.isInProgress() && !f.getFile().equals(currentInProgress)) {
        EditLogFile elf 
          = countTransactionsInInprogress(f);
        
        if (elf.isCorrupt()) {
          elf.moveAsideCorruptFile();
        } else {
          finalizeLogSegment(elf.getFirstTxId(), elf.getLastTxId());
        }
      }
    }
  }

  EditLogFile countTransactionsInInprogress(EditLogFile f) 
      throws IOException {
    if (f.getFile().equals(currentInProgress)) {
      return f; // don't count as we are currently writing
    }
    /* 
     * validatedEditLogFiles is synchronized as we don't want two 
     * inprogress files to be counted concurrently. 
     */
    synchronized(validatedEditLogFiles) {
      EditLogFile validatedEditLogFile = validatedEditLogFiles.get(f.getFile());
      if (validatedEditLogFile == null) {
        f.validateLog();
        validatedEditLogFiles.put(f.getFile(), f);
        return f;
      } else {
        if (validatedEditLogFile.needsValidation()) {
          validatedEditLogFile.validateLog();
        }
        return validatedEditLogFile;
      }
    }
  }
  
  /**
   * Record of an edit log that has been located and had its filename parsed.
   */
  static class EditLogFile {
    private File file;
    private final long firstTxId;
    private long lastTxId;

    private long lastModifiedAtValidation = 0L;
    private boolean isCorrupt = false;
    private final boolean isInProgress;

    final static Comparator<EditLogFile> COMPARE_BY_START_TXID 
      = new Comparator<EditLogFile>() {
      public int compare(EditLogFile a, EditLogFile b) {
        return ComparisonChain.start()
        .compare(a.getFirstTxId(), b.getFirstTxId())
        .compare(a.getLastTxId(), b.getLastTxId())
        .result();
      }
    };

    EditLogFile(File file,
        long firstTxId, long lastTxId) {
      this(file, firstTxId, lastTxId, false);
      assert (lastTxId != FSConstants.INVALID_TXID)
        && (lastTxId >= firstTxId);
    }
    
    EditLogFile(File file, long firstTxId, 
                long lastTxId, boolean isInProgress) { 
      assert (lastTxId == FSConstants.INVALID_TXID && isInProgress)
        || (lastTxId != FSConstants.INVALID_TXID && lastTxId >= firstTxId);
      assert (firstTxId > 0) || (firstTxId == FSConstants.INVALID_TXID);
      assert file != null;
      
      this.firstTxId = firstTxId;
      this.lastTxId = lastTxId;
      this.file = file;
      this.isInProgress = isInProgress;
    }
    
    long getFirstTxId() {
      return firstTxId;
    }
    
    long getLastTxId() {
      return lastTxId;
    }

    void setLastTxId(long lastTxId) {
      this.lastTxId = lastTxId;
    }

    EditLogValidation validateLog() throws IOException {
      lastModifiedAtValidation = file.lastModified();
      EditLogValidation val = EditLogFileInputStream.validateEditLog(file);
      if (val.getNumTransactions() == 0) {
          markCorrupt();
      } else {
        this.lastTxId = val.getEndTxId();
      }
      return val;
    }
    
    boolean needsValidation() {
      return (lastTxId == FSConstants.INVALID_TXID)
        || (file.lastModified() != lastModifiedAtValidation);
    }

    boolean isInProgress() {
      return isInProgress;
    }

    File getFile() {
      return file;
    }
    
    void markCorrupt() {
      isCorrupt = true;
    }
    
    boolean isCorrupt() {
      return isCorrupt;
    }

    void moveAsideCorruptFile() throws IOException {
      assert isCorrupt;
    
      File src = file;
      File dst = new File(src.getParent(), src.getName() + ".corrupt");
      boolean success = src.renameTo(dst);
      if (!success) {
        throw new IOException(
          "Couldn't rename corrupt log " + src + " to " + dst);
      }
      file = dst;
    }
    
    @Override
    public String toString() {
      return String.format("EditLogFile(file=%s,first=%019d,last=%019d,"
                           +"inProgress=%b,corrupt=%b)", file.toString(),
                           firstTxId, lastTxId, isInProgress(), isCorrupt);
    }
  }
}
