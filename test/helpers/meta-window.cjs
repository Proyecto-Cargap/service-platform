/** @file Arnés del registro Meta real; sustituye sólo productor, identidad y almacenamiento de recibos. */
'use strict';
const { producerWindows } = require('./producer-window.cjs');
const { EventEmitter }    = require('node:events');
const { RealtimeWindows } = require('../../server/library/realtime/windows');
const { connectWindows }  = require('../../server/modules/realtime/services/connect-window.service');
/** @returns {Object} Proyección pública completa sin datos del proveedor. */
function metaState() {
  return {
    conversationCode     : 'META',
    connectionCode       : 'CONNECTION',
    channel              : 'whatsapp',
    remoteId             : 'external',
    displayName          : 'Contacto',
    contactCode          : 'CONTACT',
    chatConversationCode : 'CHAT',
    mode                 : 'automatic',
    operatorUserCode     : '',
    revision             : 0,
    stateRevision        : 0,
    activeRunCode        : null,
    runStatus            : null,
    lastError            : null,
    lastInboundAt        : 0,
    lastActivityAt       : 0,
    deliveryState        : 'pending',
  };
}
/** @param {Function} state Productor canónico. @param {string} id Proceso. @returns {Object} Ventana registrada con HTTP explícito y entregas capturadas. */
function metaWindow(state = metaState, id = 'socket') {
  const receipts = new Map(),
    deliveries   = [],
    calls        = [];
  const identity = {
    userCode    : 'U',
    sessionCode : 'S',
    workspace   : 'W',
    context     : {
      user        : { userCode: 'U' },
      sessionCode : 'S',
      workspace   : 'W',
      policies    : ['connect.meta.view.global'],
    },
  };
  const events   = new EventEmitter();
  const socket   = {
    id,
    on   : events.on.bind(events),
    once : events.once.bind(events),
    /** @param {string} channel Canal. @param {Object} data Entrega. @returns {void} Captura sin simular recepción de comandos. */
    emit(channel, data) {
      deliveries.push({
        channel,
        ...data,
      });
    },
  };
  let chatStatus = 200;
  const windows  = new RealtimeWindows({
    topics   : producerWindows(connectWindows, {
      messaging : {
        /** @param {string} service Propietario. @param {string} action RPC. @param {Object} query Selector. @param {Object} options Identidad. @returns {Promise<Object>} Fuente controlada de estado, no de lógica de ventanas. */
        async request(service, action, query, options) {
          calls.push({
            service,
            action,
            query,
            options,
          });
          return {
            statusCode : action === 'chat-get-one' ? chatStatus : 200,
            data       : action === 'chat-get-one' ? { code: query.code } : await state(),
          };
        },
      },
    }),
    receipts : {
      /** @param {Object} value Reserva. @returns {Promise<void>} Persiste intención. */
      async put(value) {
        receipts.set(value.receiptId, value);
      },
      /** @param {string} key Recibo. @param {string} owner Identidad. @returns {Promise<Object|null>} Aísla contextos. */
      async get(key, owner) {
        const value = receipts.get(key);
        return value?.owner === owner ? value : null;
      },
      /** @param {Object} value Base pública. @returns {Promise<boolean>} Confirma una única lectura. */
      async complete(value) {
        receipts.set(value.receiptId, {
          ...value,
          state : 'ready',
        });
        return true;
      },
    },
  });
  windows.authenticate(
    socket,
    /** @returns {Promise<Object>} Principal vigente y mutable para revocaciones. */ async () =>
      identity,
  );
  const session = windows.sessions.get(id);
  let version   = 0;
  /** @param {string} type Lectura inicial o recuperación. @returns {Promise<Object>} ACK sin datos. */
  function subscribe(type = 'subscribe') {
    return windows.control(session, {
      type,
      subscriptionId : 'meta',
      watchVersion   : ++version,
      topic          : 'connect.meta-conversation',
      section        : 'detail',
      query          : { conversationCode: 'CHAT' },
    });
  }
  /** @param {Object} ack Reserva del servidor. @returns {Promise<Object>} Base exclusivamente HTTP, seguida por bind sin registros del cliente. */
  async function read(ack) {
    const base = await windows.read(
      {
        ...ack,
        topic   : 'connect.meta-conversation',
        section : 'detail',
        query   : { conversationCode: 'CHAT' },
      },
      identity,
    );
    await windows.control(session, {
      ...base.control,
      type : 'bind',
    });
    return base;
  }
  return {
    windows,
    identity,
    calls,
    deliveries,
    subscribe,
    read,
    /** @param {number} status Autorización canónica. @returns {void} Simula revocación de participación. */
    chatStatus(status) {
      chatStatus = status;
    },
  };
}
module.exports = {
  metaState,
  metaWindow,
};
