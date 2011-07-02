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

import java.net.URI;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.EditLogValidation;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

class FSImageTransactionalStorageInspector extends FSImageStorageInspector {
  public static final Log LOG = LogFactory.getLog(
    FSImageTransactionalStorageInspector.class);

  private boolean needToSave = false;
  private boolean isUpgradeFinalized = true;
  
  List<FoundFSImage> foundImages = new ArrayList<FoundFSImage>();
  
  private static final Pattern IMAGE_REGEX = Pattern.compile(
    NameNodeFile.IMAGE.getName() + "_(\\d+)");
  
  @Override
  public void inspectDirectory(StorageDirectory sd) throws IOException {
    // Was the directory just formatted?
    if (!sd.getVersionFile().exists()) {
      LOG.info("No version file in " + sd.getRoot());
      needToSave |= true;
      return;
    }
    
    File currentDir = sd.getCurrentDir();
    File filesInStorage[];
    try {
      filesInStorage = FileUtil.listFiles(currentDir);
    } catch (IOException ioe) {
      LOG.warn("Unable to inspect storage directory " + currentDir,
          ioe);
      return;
    }

    for (File f : filesInStorage) {
      LOG.debug("Checking file " + f);
      String name = f.getName();
      
      // Check for fsimage_*
      Matcher imageMatch = IMAGE_REGEX.matcher(name);
      if (imageMatch.matches()) {
        if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
          try {
            long txid = Long.valueOf(imageMatch.group(1));
            foundImages.add(new FoundFSImage(sd, f, txid));
          } catch (NumberFormatException nfe) {
            LOG.error("Image file " + f + " has improperly formatted " +
                      "transaction ID");
            // skip
          }
        } else {
          LOG.warn("Found image file at " + f + " but storage directory is " +
                   "not configured to contain images.");
        }
      }
    }
      
    // set finalized flag
    isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();
  }

  @Override
  public boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }
  
  /**
   * @return the image that has the most recent associated transaction ID.
   * If there are multiple storage directories which contain equal images 
   * the storage directory that was inspected first will be preferred.
   * 
   * Returns null if no images were found.
   */
  FoundFSImage getLatestImage() {
    FoundFSImage ret = null;
    for (FoundFSImage img : foundImages) {
      if (ret == null || img.txId > ret.txId) {
        ret = img;
      }
    }
    return ret;
  }
  
  public List<FoundFSImage> getFoundImages() {
    return ImmutableList.copyOf(foundImages);
  }
  
  @Override
  File getImageFileForLoading() throws IOException {
    if (foundImages.isEmpty()) {
      throw new FileNotFoundException("No valid image files found");
    }

    FoundFSImage recoveryImage = getLatestImage();
    recoveryImage.sd.read();

    return recoveryImage.file;
  }

  @Override
  public boolean needToSave() {
    return false; // TODO do we need to do this ever?
  }
    
    // void planRecovery() throws IOException {
    //   assert hasInProgress || hasFinalized;
      
    //   checkConsistentEndTxIds();
        
    //   if (hasFinalized && hasInProgress) {
    //     planMixedLogRecovery();
    //   } else if (!hasFinalized && hasInProgress) {
    //     planAllInProgressRecovery();
    //   } else if (hasFinalized && !hasInProgress) {
    //     LOG.debug("No recovery necessary for logs starting at txid " +
    //               startTxId);
    //   }
    // }

    
    /**
     * Recovery case for when all of the logs in the group were in progress.
     * This happens if the NN completely crashes and restarts. In this case
     * we check the non-zero lengths of each log file, and any logs that are
     * less than the max of these lengths are considered corrupt.
     */
  //   private void planAllInProgressRecovery() throws IOException {
  //     // We only have in-progress logs. We need to figure out which logs have
  //     // the latest data to reccover them
  //     LOG.warn("Logs beginning at txid " + startTxId + " were are all " +
  //              "in-progress (probably truncated due to a previous NameNode " +
  //              "crash)");
  //     if (logs.size() == 1) {
  //       // Only one log, it's our only choice!
  //       FoundEditLog log = logs.get(0);
  //       if (log.validateLog().numTransactions == 0) {
  //         // If it has no transactions, we should consider it corrupt just
  //         // to be conservative.
  //         // See comment below for similar case
  //         LOG.warn("Marking log at " + log.getFile() + " as corrupt since " +
  //             "it has no transactions in it.");
  //         log.markCorrupt();          
  //       }
  //       return;
  //     }

  //     long maxValidTxnCount = Long.MIN_VALUE;
  //     for (FoundEditLog log : logs) {
  //       long validTxnCount = log.validateLog().numTransactions;
  //       LOG.warn("  Log " + log.getFile() + " valid txns=" + validTxnCount);
  //       maxValidTxnCount = Math.max(maxValidTxnCount, validTxnCount);
  //     }        

  //     for (FoundEditLog log : logs) {
  //       long txns = log.validateLog().numTransactions;
  //       if (txns < maxValidTxnCount) {
  //         LOG.warn("Marking log at " + log.getFile() + " as corrupt since " +
  //                  "it is has only " + txns + " valid txns whereas another " +
  //                  "log has " + maxValidTxnCount);
  //         log.markCorrupt();
  //       } else if (txns == 0) {
  //         // this can happen if the NN crashes right after rolling a log
  //         // but before the START_LOG_SEGMENT txn is written. Since the log
  //         // is empty, we can just move it aside to its corrupt name.
  //         LOG.warn("Marking log at " + log.getFile() + " as corrupt since " +
  //             "it has no transactions in it.");
  //         log.markCorrupt();
  //       }
  //     }
  //   }

  //   /**
  //    * Check for the case when we have multiple finalized logs and they have
  //    * different ending transaction IDs. This violates an invariant that all
  //    * log directories should roll together. We should abort in this case.
  //    */
  //   private void checkConsistentEndTxIds() throws IOException {
  //     if (hasFinalized && endTxIds.size() > 1) {
  //       throw new IOException("More than one ending txid was found " +
  //           "for logs starting at txid " + startTxId + ". " +
  //           "Found: " + StringUtils.join(endTxIds, ','));
  //     }
  //   }

  //   void recover() throws IOException {
  //     for (FoundEditLog log : logs) {
  //       if (log.isCorrupt()) {
  //         log.moveAsideCorruptFile();
  //       } else if (log.isInProgress()) {
  //         log.finalizeLog();
  //       }
  //     }
  //   }    
  // }

  /**
   * Record of an image that has been located and had its filename parsed.
   */
  static class FoundFSImage {
    final StorageDirectory sd;    
    final long txId;
    private final File file;
    
    FoundFSImage(StorageDirectory sd, File file, long txId) {
      assert txId >= 0 : "Invalid txid on " + file +": " + txId;
      
      this.sd = sd;
      this.txId = txId;
      this.file = file;
    } 
    
    File getFile() {
      return file;
    }

    public long getTxId() {
      return txId;
    }
    
    @Override
    public String toString() {
      return file.toString();
    }
  }  
}