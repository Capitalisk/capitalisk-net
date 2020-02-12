const crypto = require('crypto');
const { lookupPeersIPs, filterByParams, consolidatePeers } = require('./utils');
const util = require('util');
const fs = require('fs');
const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);
const path = require('path');
const {
  P2P,
  EVENT_NETWORK_READY,
  EVENT_NEW_INBOUND_PEER,
  EVENT_CLOSE_INBOUND,
  EVENT_CLOSE_OUTBOUND,
  EVENT_CONNECT_OUTBOUND,
  EVENT_DISCOVERED_PEER,
  EVENT_FAILED_TO_FETCH_PEER_INFO,
  EVENT_FAILED_TO_PUSH_NODE_INFO,
  EVENT_OUTBOUND_SOCKET_ERROR,
  EVENT_INBOUND_SOCKET_ERROR,
  EVENT_UPDATED_PEER_INFO,
  EVENT_FAILED_PEER_INFO_UPDATE,
  EVENT_REQUEST_RECEIVED,
  EVENT_MESSAGE_RECEIVED,
  EVENT_BAN_PEER,
  EVENT_UNBAN_PEER
} = require('@liskhq/lisk-p2p');

const DEFAULT_PEER_SAVE_INTERVAL = 10 * 60 * 1000; // 10 mins in ms
const DEFAULT_PEER_LIST_FILE_PATH = path.join(__dirname, 'peers.json');
const MAX_CHANNEL_NAME_LENGTH = 200;

const hasNamespaceReg = /:/;

class LeaseholdNet {
  constructor({alias, logger}) {
    this.options = {};
    this.alias = alias;
    this.logger = logger;
    this.channel = null;
    this.secret = null;
  }

  get dependencies() {
    return ['app'];
  }

  get events() {
    return ['event'];
  }

  get actions() {
    return {
      request: {
        handler: async action => {
          return this.p2p.request({
            procedure: action.params.procedure,
            data: action.params.data
          });
        }
      },
      emit: {
        handler: action => {
          return this.p2p.send({
            event: action.params.event,
            data: action.params.data
          });
        }
      },
      requestFromPeer: {
        handler: async action => {
          return this.p2p.requestFromPeer(
            {
              procedure: action.params.procedure,
              data: action.params.data,
            },
            action.params.peerId
          );
        }
      },
      emitToPeer: {
        handler: action => {
          return this.p2p.sendToPeer(
            {
              event: action.params.event,
              data: action.params.data,
            },
            action.params.peerId
          );
        }
      },
      getPeers: {
        handler: action => {
          let peers = consolidatePeers({
            connectedPeers: this.p2p.getConnectedPeers(),
            disconnectedPeers: this.p2p.getDisconnectedPeers()
          });

          return filterByParams(peers, action.params);
        }
      },
      getPeersCount: {
        handler: action => {
          let peers = consolidatePeers({
            connectedPeers: this.p2p.getConnectedPeers(),
            disconnectedPeers: this.p2p.getDisconnectedPeers()
          });

          let { limit, offset, ...filterWithoutLimitOffset } = action.params;

          return filterByParams(peers, filterWithoutLimitOffset).length;
        }
      },
      getUniqueOutboundConnectedPeersCount: {
        handler: action => {
          let peers = consolidatePeers({
            connectedPeers: this.p2p.getUniqueOutboundConnectedPeers()
          });

          let { limit, offset, ...filterWithoutLimitOffset } = action.params;

          return filterByParams(peers, filterWithoutLimitOffset).length;
        }
      },
      applyPenalty: {
        handler: action => {
          return this.p2p.applyPenalty(action.params.peerId, action.params.penalty);
        }
      }
    };
  }

  async load(channel, options) {
    this.channel = channel;
    this.options = options;

    if (this.options.peerSelectionPluginPath) {
      this.logger.debug(
        `Using peer selection plugin at path ${this.options.peerSelectionPluginPath} relative to leasehold-net module`
      );
      let peerSelectionPlugin = require(this.options.peerSelectionPluginPath);

      this.peerSelectionForConnection = peerSelectionPlugin.peerSelectionForConnection;
      this.peerSelectionForRequest = peerSelectionPlugin.peerSelectionForRequest;
      this.peerSelectionForSend = peerSelectionPlugin.peerSelectionForSend;
    } else {
      this.logger.debug(
        `Using default peer selection`
      );
    }

    let peerListFilePath = this.options.peerListFilePath || DEFAULT_PEER_LIST_FILE_PATH;

    // Load peers from the database that were tried or connected the last time node was running
    let previousPeersStr;
    try {
      previousPeersStr = await readFile(peerListFilePath, 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') {
        this.logger.debug(`No existing peer list file found at ${peerListFilePath}`);
      } else {
        this.logger.error(`Failed to read file ${peerListFilePath} - ${err.message}`);
      }
    }
    let previousPeers = [];
    try {
      previousPeers = previousPeersStr ? JSON.parse(previousPeersStr) : [];
    } catch (err) {
      this.logger.error(`Failed to parse JSON of previous peers - ${err.message}`);
    }

    this.secret = crypto.randomBytes(4).readUInt32BE(0);

    let sanitizeNodeInfo = nodeInfo => ({
      ...nodeInfo,
      wsPort: this.options.wsPort
    });

    let initialNodeInfo = sanitizeNodeInfo(
      await this.channel.invoke('app:getApplicationState')
    );

    let seedPeers = await lookupPeersIPs(this.options.seedPeers, true);
    let blacklistedIPs = this.options.blacklistedIPs || [];

    let fixedPeers = this.options.fixedPeers
      ? this.options.fixedPeers.map(peer => ({
          ipAddress: peer.ip,
          wsPort: peer.wsPort
        }))
      : [];

    let whitelistedPeers = this.options.whitelistedPeers
      ? this.options.whitelistedPeers.map(peer => ({
          ipAddress: peer.ip,
          wsPort: peer.wsPort
        }))
      : [];

    let p2pConfig = {
      nodeInfo: initialNodeInfo,
      hostIp: this.options.hostIp,
      blacklistedIPs,
      fixedPeers,
      whitelistedPeers,
      seedPeers: seedPeers.map(peer => ({
        ipAddress: peer.ip,
        wsPort: peer.wsPort
      })),
      previousPeers,
      maxOutboundConnections: this.options.maxOutboundConnections,
      maxInboundConnections: this.options.maxInboundConnections,
      peerBanTime: this.options.peerBanTime,
      populatorInterval: this.options.populatorInterval,
      sendPeerLimit: this.options.sendPeerLimit,
      maxPeerDiscoveryResponseLength: this.options
        .maxPeerDiscoveryResponseLength,
      maxPeerInfoSize: this.options.maxPeerInfoSize,
      wsMaxPayload: this.options.wsMaxPayload,
      secret: this.secret
    };

    if (this.peerSelectionForConnection) {
      p2pConfig.peerSelectionForConnection = this.peerSelectionForConnection;
    }
    if (this.peerSelectionForRequest) {
      p2pConfig.peerSelectionForRequest = this.peerSelectionForRequest;
    }
    if (this.peerSelectionForSend) {
      p2pConfig.peerSelectionForSend = this.peerSelectionForSend;
    }

    this.p2p = new P2P(p2pConfig);

    this.channel.subscribe('app:state:updated', event => {
      let newNodeInfo = sanitizeNodeInfo(event.data);
      try {
        this.p2p.applyNodeInfo(newNodeInfo);
      } catch (error) {
        this.logger.error(
          `Applying NodeInfo failed because of error: ${error.message ||
            error}`
        );
      }
    });

    this.p2p.on(EVENT_NETWORK_READY, () => {
      this.logger.debug('Node connected to the network');
      this.channel.publish('leasehold_net:ready');
    });

    this.p2p.on(EVENT_CLOSE_OUTBOUND, closePacket => {
      this.logger.debug(
        {
          ipAddress: closePacket.peerInfo.ipAddress,
          wsPort: closePacket.peerInfo.wsPort,
          code: closePacket.code,
          reason: closePacket.reason,
        },
        'EVENT_CLOSE_OUTBOUND: Close outbound peer connection'
      );
    });

    this.p2p.on(EVENT_CLOSE_INBOUND, closePacket => {
      this.logger.debug(
        {
          ipAddress: closePacket.peerInfo.ipAddress,
          wsPort: closePacket.peerInfo.wsPort,
          code: closePacket.code,
          reason: closePacket.reason,
        },
        'EVENT_CLOSE_INBOUND: Close inbound peer connection'
      );
    });

    this.p2p.on(EVENT_CONNECT_OUTBOUND, peerInfo => {
      this.logger.debug(
        {
          ipAddress: peerInfo.ipAddress,
          wsPort: peerInfo.wsPort,
        },
        'EVENT_CONNECT_OUTBOUND: Outbound peer connection'
      );
    });

    this.p2p.on(EVENT_DISCOVERED_PEER, peerInfo => {
      this.logger.trace(
        {
          ipAddress: peerInfo.ipAddress,
          wsPort: peerInfo.wsPort,
        },
        'EVENT_DISCOVERED_PEER: Discovered peer connection'
      );
    });

    this.p2p.on(EVENT_NEW_INBOUND_PEER, peerInfo => {
      this.logger.debug(
        {
          ipAddress: peerInfo.ipAddress,
          wsPort: peerInfo.wsPort,
        },
        'EVENT_NEW_INBOUND_PEER: Inbound peer connection'
      );
    });

    this.p2p.on(EVENT_FAILED_TO_FETCH_PEER_INFO, error => {
      this.logger.error(error.message || error);
    });

    this.p2p.on(EVENT_FAILED_TO_PUSH_NODE_INFO, error => {
      this.logger.trace(error.message || error);
    });

    this.p2p.on(EVENT_OUTBOUND_SOCKET_ERROR, error => {
      this.logger.debug(error.message || error);
    });

    this.p2p.on(EVENT_INBOUND_SOCKET_ERROR, error => {
      this.logger.debug(error.message || error);
    });

    this.p2p.on(EVENT_UPDATED_PEER_INFO, peerInfo => {
      this.logger.trace(
        {
          ipAddress: peerInfo.ipAddress,
          wsPort: peerInfo.wsPort,
        },
        'EVENT_UPDATED_PEER_INFO: Update peer info',
        JSON.stringify(peerInfo)
      );
    });

    this.p2p.on(EVENT_FAILED_PEER_INFO_UPDATE, error => {
      this.logger.error(error.message || error);
    });

    this.p2p.on(EVENT_REQUEST_RECEIVED, async request => {
      this.logger.trace(
        `EVENT_REQUEST_RECEIVED: Received inbound request for procedure ${request.procedure}`
      );
      // If the request has already been handled internally by the P2P library, we ignore.
      if (request.wasResponseSent) {
        return;
      }
      let hasTargetModule = hasNamespaceReg.test(request.procedure);
      // If the request has no target module, default to chain (to support legacy protocol).
      let sanitizedProcedure = hasTargetModule
        ? request.procedure
        : `chain:${request.procedure}`;
      try {
        let result = await this.channel.invokePublic(sanitizedProcedure, {
          data: request.data,
          peerId: request.peerId,
        });
        this.logger.trace(
          `Peer request fulfilled event: Responded to peer request ${request.procedure}`
        );
        request.end(result); // Send the response back to the peer.
      } catch (error) {
        this.logger.error(
          `Peer request not fulfilled event: Could not respond to peer request ${
            request.procedure
          } because of error: ${error.message || error}`
        );
        request.error(error); // Send an error back to the peer.
      }
    });

    this.p2p.on(EVENT_MESSAGE_RECEIVED, async packet => {
      this.logger.trace(
        `EVENT_MESSAGE_RECEIVED: Received inbound message from ${packet.peerId} for event ${packet.event}`
      );
      let targetChannelName = `leasehold_net:event:${packet.event}`;
      if (targetChannelName.length > MAX_CHANNEL_NAME_LENGTH) {
        this.logger.error(
          `Peer ${packet.peerId} tried to publish data to a custom channel name which exceeded the max length of ${MAX_CHANNEL_NAME_LENGTH}`
        );
      } else {
        this.channel.publish(targetChannelName, packet);
      }
      // For backward compatibility with Lisk chain module.
      this.channel.publish('leasehold_net:event', {data: packet});
    });

    this.p2p.on(EVENT_BAN_PEER, peerId => {
      this.logger.error(
        { peerId },
        'EVENT_MESSAGE_RECEIVED: Peer has been banned temporarily'
      );
    });

    this.p2p.on(EVENT_UNBAN_PEER, peerId => {
      this.logger.error(
        { peerId },
        'EVENT_MESSAGE_RECEIVED: Peer ban has expired'
      );
    });

    setInterval(async () => {
      let peersToSave = this.p2p.getConnectedPeers();
      if (peersToSave.length) {
        let peersString = JSON.stringify(peersToSave);
        await writeFile(peerListFilePath, peersString);
      }
    }, DEFAULT_PEER_SAVE_INTERVAL);

    try {
      await this.p2p.start();
    } catch (error) {
      this.logger.fatal(
        {
          message: error.message,
          stack: error.stack,
        },
        `Failed to initialize net module`
      );
      process.emit('cleanup', error);
    }
  }

  async unload() {
    this.logger.info(`Stopping net module...`);
    return this.p2p.stop();
  }
};

module.exports = LeaseholdNet;
