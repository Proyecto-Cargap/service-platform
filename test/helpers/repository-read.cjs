/** @file Recibos y lecturas canónicas para ejercitar el protocolo de repositorios sin red. */
'use strict';
const { randomUUID }    = require('node:crypto');
const { contentCursor } = require('../../server/library/realtime/window-delta');
const {
  issueReadReceipt,
  confirmReadReceipt,
} = require('../../server/library/messaging/read-receipt');
const {
  repositoryReads,
} = require('../../server/modules/realtime/services/repository-read.service');
/** @param {Object} options Persistencia y productor opcionales para integraciones. @returns {Object} Lector productivo con puertos controlados. */
function reader({ receipts, manifest } = {}) {
  const values = new Map();
  return repositoryReads({
    clock      : Date.now,
    identifier : randomUUID,
    hash       : contentCursor,
    receipts   : receipts || {
      /** @param {Object} value Reserva. @returns {Promise<void>} Conserva metadatos de autoridad. */
      async put(value) {
        values.set(value.receiptId, structuredClone(value));
      },
      /** @param {string} id Identidad. @param {string} owner Propietario. @returns {Promise<Object|null>} Lectura aislada. */
      async get(id, owner) {
        const value = values.get(id);
        return value?.owner === owner ? structuredClone(value) : null;
      },
      /** @param {Object} before Progreso esperado. @param {Object} after Página nueva. @returns {Promise<boolean>} CAS de la misma reserva. */
      async advanceRepository(before, after) {
        const value = values.get(before.receiptId);
        if (
          value.state !== 'registered' ||
          value.count !== before.count ||
          value.nextCursor !== before.nextCursor ||
          value.revision !== before.revision
        )
          return false;
        Object.assign(value, after);
        return true;
      },
    },
    messaging  : {
      /** @param {string} service Productor. @param {string} action Lectura canónica. @param {Object} query Cursor. @param {Object} options Contexto. @returns {Promise<Object>} Simula sólo la frontera RPC, conserva la acreditación real del productor. */
      async request(service, action, query, options) {
        const result = manifest
          ? await manifest(query, options.context)
          : {
              statusCode : 200,
              data       : {
                repository  : { id: query.repositoryId },
                items       : [],
                revision    : 0,
                nextCursor  : null,
                notModified : false,
              },
            };
        return confirmReadReceipt(result, issueReadReceipt(result));
      },
    },
  });
}
/** @param {Object} identity Identidad del socket. @returns {Object} Misma autoridad del Gateway. */
function readIdentity(identity) {
  return {
    context : {
      user        : { userCode: identity.userCode },
      workspace   : identity.activeWorkspace,
      sessionCode : identity.sessionCode,
      delegation  : identity.delegation,
    },
  };
}
module.exports = {
  reader,
  readIdentity,
};
