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

package org.apache.hadoop.yarn.api;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

public class TestNodeId {
  @Test
  public void testNodeId() {
    NodeId nodeId1 = createNodeId("10.18.52.124", 45454);
    NodeId nodeId2 = createNodeId("10.18.52.125", 45452);
    NodeId nodeId3 = createNodeId("10.18.52.124", 45454);
    NodeId nodeId4 = createNodeId("10.18.52.124", 45453);

    Assert.assertTrue(nodeId1.equals(nodeId3));
    Assert.assertFalse(nodeId1.equals(nodeId2));
    Assert.assertFalse(nodeId3.equals(nodeId4));

    Assert.assertTrue(nodeId1.compareTo(nodeId3) == 0);
    Assert.assertTrue(nodeId1.compareTo(nodeId2) < 0);
    Assert.assertTrue(nodeId3.compareTo(nodeId4) > 0);

    Assert.assertTrue(nodeId1.hashCode() == nodeId3.hashCode());
    Assert.assertFalse(nodeId1.hashCode() == nodeId2.hashCode());
    Assert.assertFalse(nodeId3.hashCode() == nodeId4.hashCode());

    Assert.assertEquals("10.18.52.124:45454", nodeId1.toString());
  }

  private NodeId createNodeId(String host, int port) {
    NodeId nodeId = Records.newRecord(NodeId.class);
    nodeId.setHost(host);
    nodeId.setPort(port);
    return nodeId;
  }
}
