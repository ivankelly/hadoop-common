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

import java.io.IOException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility class for storing and reading
 * the max seen txid in zookeeper
 */
class MaxTxId {
  static final Log LOG = LogFactory.getLog(MaxTxId.class);

  final private ZooKeeper zkc;
  final private String path;

  private Stat currentStat;
  private long currentMax;

  MaxTxId(ZooKeeper zkc, String path) {
    this.zkc = zkc;
    this.path = path;
  }

  synchronized void store(long maxTxId) throws IOException {
    long currentMax = get();
    if (currentMax < maxTxId) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Setting maxTxId to " + maxTxId);
      }
      String txidStr = Long.toString(maxTxId);
      try {
        if (currentStat != null) {
          currentStat = zkc.setData(path, txidStr.getBytes("UTF-8"), 
                                    currentStat.getVersion());
        } else {
          zkc.create(path, txidStr.getBytes("UTF-8"), 
                     Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
      } catch (Exception e) {
        throw new IOException("Error writing max tx id", e);
      }
      currentMax = maxTxId;
    }
  }

  synchronized long get() throws IOException {
    try {
      currentStat = zkc.exists(path, false);
      if (currentStat == null) {
        return 0;
      } else {
        byte[] bytes = zkc.getData(path, false, currentStat);
        String txidString = new String(bytes, "UTF-8");
        currentMax = Long.valueOf(txidString);
        return currentMax;
      }
    } catch (Exception e) {
      throw new IOException("Error reading the max tx id from zk", e);
    }
  }
}