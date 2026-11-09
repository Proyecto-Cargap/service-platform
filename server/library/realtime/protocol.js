/** @file Canal único realtime: registro de topics y validación del control de suscripciones. */
'use strict';
const { randomUUID } = require('node:crypto');
const sockets        = new WeakMap();
const REQUEST_FIELDS = new Set([
  'type',
  'requestId',
  'subscriptionId',
  'watchVersion',
  'topic',
  'section',
  'query',
  'receiptId',
]);

/** @param {*} value Valor recibido. @returns {boolean} Identificador técnico acotado. */
function identifier(value) {
  return typeof value === 'string' && /^[a-zA-Z0-9._:-]{1,100}$/.test(value);
}

/** @param {*} value Control recibido del cliente. @returns {boolean} Acredita el sobre, no la consulta del dominio. */
function validRequest(value) {
  return (
    !!value &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    Object.keys(value).every(
      /** @param {string} key Campo recibido. @returns {boolean} Sólo admite el contrato estándar. */
      (key) => REQUEST_FIELDS.has(key),
    ) &&
    ['subscribe', 'unsubscribe', 'bind', 'resync'].includes(value.type) &&
    identifier(value.requestId) &&
    identifier(value.subscriptionId) &&
    identifier(value.topic) &&
    identifier(value.section) &&
    Number.isSafeInteger(value.watchVersion) &&
    value.watchVersion > 0 &&
    (value.query === undefined ||
      (value.query && typeof value.query === 'object' && !Array.isArray(value.query))) &&
    (value.receiptId === undefined || identifier(value.receiptId))
  );
}

/**
 * Registra una capacidad bajo el único listener de control de la conexión.
 * @param {Object} socket Conexión Socket.IO.
 * @param {string} topic Nombre registrado por el dominio.
 * @param {Function} handle Controlador que autoriza y valida sección/consulta.
 * @returns {void} No abre conexiones ni hace lecturas iniciales.
 */
function registerRealtimeTopic(socket, topic, handle) {
  if (!identifier(topic) || typeof handle !== 'function') throw new Error('INVALID_REALTIME_TOPIC');
  let topics = sockets.get(socket);
  if (!topics) {
    topics = new Map();
    sockets.set(socket, topics);
    socket.on(
      'realtime',
      /** @param {*} value Solicitud. @param {Function} acknowledge ACK opcional. @returns {Promise<void>} Resuelve exactamente una confirmación. */
      async (value, acknowledge) => {
        let response;
        try {
          if (!validRequest(value) || !topics.has(value.topic))
            response = {
              statusCode      : 400,
              descriptionCode : 'INVALID_REALTIME_REQUEST',
            };
          else response = await topics.get(value.topic)(value);
        } catch (error) {
          response = {
            statusCode : [400, 401, 403, 404, 409, 429].includes(error.statusCode)
              ? error.statusCode
              : 503,
          };
        }
        if (typeof acknowledge === 'function')
          acknowledge({
            ...response,
            requestId      : value?.requestId,
            subscriptionId : value?.subscriptionId,
            watchVersion   : value?.watchVersion,
          });
      },
    );
    socket.on(
      'disconnect',
      /** @returns {void} Descarta registros de la conexión cerrada. */
      () => {
        topics.clear();
        sockets.delete(socket);
      },
    );
  }
  if (topics.has(topic)) throw new Error('DUPLICATE_REALTIME_TOPIC');
  topics.set(topic, handle);
}

/** @param {Object} socket Transporte. @param {Object} delivery Sobre público preparado por el dominio. @returns {void} Entrega exclusivamente cambios y controles por realtime. */
function deliverRealtime(socket, delivery) {
  if (
    !['patch', 'revoked', 'error', 'resync-required'].includes(delivery.type) ||
    !identifier(delivery.topic) ||
    !identifier(delivery.section) ||
    !identifier(delivery.subscriptionId) ||
    !Number.isSafeInteger(delivery.watchVersion) ||
    delivery.watchVersion < 1 ||
    !Number.isSafeInteger(delivery.delivery) ||
    delivery.delivery < 1
  )
    throw new Error('INVALID_REALTIME_DELIVERY');
  socket.emit('realtime', {
    ...delivery,
    eventId    : delivery.eventId || randomUUID(),
    occurredAt : Date.now(),
  });
}

module.exports = {
  registerRealtimeTopic,
  deliverRealtime,
  validRequest,
};
