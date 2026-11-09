/** @file Dobles RPC de pruebas con recibo técnico, sin sustituir autorización, contratos ni coordinadores de ventanas. */
'use strict';
const {
  issueReadReceipt,
  confirmReadReceipt,
} = require('../../server/library/messaging/read-receipt');
/** @param {Function} factory Registro de ventanas de producción. @param {Object} options Productor simulado. @returns {Object} Mismo registro con respuesta JSON y recibo como en el transporte RPC. */
function producerWindows(factory, options) {
  const messaging = {
    ...options.messaging,
    /** @param {...*} args Destino, consulta e identidad. @returns {Promise<Object>} Serialización y recibo del productor simulado. */
    async request(...args) {
      const response = await options.messaging.request(...args);
      const cursor   = issueReadReceipt(response);
      return confirmReadReceipt(JSON.parse(JSON.stringify(response)), cursor);
    },
  };
  return factory({
    ...options,
    messaging,
  });
}
module.exports = { producerWindows };
