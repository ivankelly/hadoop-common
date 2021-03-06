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
package org.apache.hadoop.hdfs.protocolPB;

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamespaceInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.EndCheckpointRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.EndCheckpointResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.ErrorReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.ErrorReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlockKeysRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlockKeysResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetTransactionIdRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetTransactionIdResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RegisterRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RegisterResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RollEditLogRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RollEditLogResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.StartCheckpointRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.StartCheckpointResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.VersionResponseProto;
import org.apache.hadoop.hdfs.protocolR23Compatible.ProtocolSignatureWritable;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Implementation for protobuf service that forwards requests
 * received on {@link NamenodeProtocolPB} to the
 * {@link NamenodeProtocol} server implementation.
 */
public class NamenodeProtocolServerSideTranslatorPB implements
    NamenodeProtocolPB {
  private final NamenodeProtocol impl;

  public NamenodeProtocolServerSideTranslatorPB(NamenodeProtocol impl) {
    this.impl = impl;
  }

  @Override
  public GetBlocksResponseProto getBlocks(RpcController unused,
      GetBlocksRequestProto request) throws ServiceException {
    DatanodeInfo dnInfo = new DatanodeInfo(PBHelper.convert(request
        .getDatanode()));
    BlocksWithLocations blocks;
    try {
      blocks = impl.getBlocks(dnInfo, request.getSize());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetBlocksResponseProto.newBuilder()
        .setBlocks(PBHelper.convert(blocks)).build();
  }

  @Override
  public GetBlockKeysResponseProto getBlockKeys(RpcController unused,
      GetBlockKeysRequestProto request) throws ServiceException {
    ExportedBlockKeys keys;
    try {
      keys = impl.getBlockKeys();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetBlockKeysResponseProto.newBuilder()
        .setKeys(PBHelper.convert(keys)).build();
  }

  @Override
  public GetTransactionIdResponseProto getTransationId(RpcController unused,
      GetTransactionIdRequestProto request) throws ServiceException {
    long txid;
    try {
      txid = impl.getTransactionID();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetTransactionIdResponseProto.newBuilder().setTxId(txid).build();
  }

  @Override
  public RollEditLogResponseProto rollEditLog(RpcController unused,
      RollEditLogRequestProto request) throws ServiceException {
    CheckpointSignature signature;
    try {
      signature = impl.rollEditLog();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return RollEditLogResponseProto.newBuilder()
        .setSignature(PBHelper.convert(signature)).build();
  }

  @Override
  public ErrorReportResponseProto errorReport(RpcController unused,
      ErrorReportRequestProto request) throws ServiceException {
    try {
      impl.errorReport(PBHelper.convert(request.getRegistration()),
          request.getErrorCode(), request.getMsg());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return ErrorReportResponseProto.newBuilder().build();
  }

  @Override
  public RegisterResponseProto register(RpcController unused,
      RegisterRequestProto request) throws ServiceException {
    NamenodeRegistration reg;
    try {
      reg = impl.register(PBHelper.convert(request.getRegistration()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return RegisterResponseProto.newBuilder()
        .setRegistration(PBHelper.convert(reg)).build();
  }

  @Override
  public StartCheckpointResponseProto startCheckpoint(RpcController unused,
      StartCheckpointRequestProto request) throws ServiceException {
    NamenodeCommand cmd;
    try {
      cmd = impl.startCheckpoint(PBHelper.convert(request.getRegistration()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return StartCheckpointResponseProto.newBuilder()
        .setCommand(PBHelper.convert(cmd)).build();
  }

  @Override
  public EndCheckpointResponseProto endCheckpoint(RpcController unused,
      EndCheckpointRequestProto request) throws ServiceException {
    try {
      impl.endCheckpoint(PBHelper.convert(request.getRegistration()),
          PBHelper.convert(request.getSignature()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return EndCheckpointResponseProto.newBuilder().build();
  }

  @Override
  public GetEditLogManifestResponseProto getEditLogManifest(
      RpcController unused, GetEditLogManifestRequestProto request)
      throws ServiceException {
    RemoteEditLogManifest manifest;
    try {
      manifest = impl.getEditLogManifest(request.getSinceTxId());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetEditLogManifestResponseProto.newBuilder()
        .setManifest(PBHelper.convert(manifest)).build();
  }
  
  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return RPC.getProtocolVersion(NamenodeProtocolPB.class);
  }

  /**
   * The client side will redirect getProtocolSignature to
   * getProtocolSignature2.
   * 
   * However the RPC layer below on the Server side will call getProtocolVersion
   * and possibly in the future getProtocolSignature. Hence we still implement
   * it even though the end client will never call this method.
   */
  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link NamenodeProtocol}
     */
    if (!protocol.equals(RPC.getProtocolName(NamenodeProtocolPB.class))) {
      throw new IOException("Namenode Serverside implements " +
          RPC.getProtocolName(NamenodeProtocolPB.class) +
          ". The following requested protocol is unknown: " + protocol);
    }

    return ProtocolSignature.getProtocolSignature(clientMethodsHash,
        RPC.getProtocolVersion(NamenodeProtocolPB.class),
        NamenodeProtocolPB.class);
  }


  @Override
  public ProtocolSignatureWritable getProtocolSignature2(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link NamenodePBProtocol}
     */
    return ProtocolSignatureWritable.convert(
        this.getProtocolSignature(protocol, clientVersion, clientMethodsHash));
  }

  @Override
  public VersionResponseProto versionRequest(RpcController controller,
      VersionRequestProto request) throws ServiceException {
    NamespaceInfo info;
    try {
      info = impl.versionRequest();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VersionResponseProto.newBuilder()
        .setInfo(convert(info)).build();
  }

  private NamespaceInfoProto convert(NamespaceInfo info) {
    return NamespaceInfoProto.newBuilder()
        .setBlockPoolID(info.getBlockPoolID())
        .setBuildVersion(info.getBuildVersion())
        .setDistUpgradeVersion(info.getDistributedUpgradeVersion())
        .setStorageInfo(PBHelper.convert(info)).build();
  }
}
