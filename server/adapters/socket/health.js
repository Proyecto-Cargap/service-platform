/**
 * @file Registro HTTP de salud del proceso con disponibilidad de la base de datos, transporte e identidad
 * de versión.
 * Registra la salud técnica de Platform, incluyendo su listener Socket.IO.
 */
'use strict';
const health = require('../../library/messaging/health');

/**
 * Registra una acción de salud antes de iniciar el runtime de mensajería. Captura la identidad una vez y
 * conserva el contrato común de MongoDB y AMQP.
 *
 * @param {Object} options Dependencias del proceso aplicativo.
 * @param {Object} options.messaging Fachada pública del runtime de mensajería.
 * @param {Object} options.connection Conexión MongoDB supervisada por el servicio.
 * @param {import('node:http').Server} options.server Listener utilizado por Socket.IO.
 * @param {string} options.releaseFile Archivo de identidad de la release instalada.
 * @returns {Function} Comprobación técnica compartida por RPC y telemetría.
 */
function registerSocketHealth({
  messaging,
  connection,
  server,
  releaseFile,
  /**
   * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
   * @returns {boolean} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  documentsReady = () => true,
}) {
  const identity   = health.captureIdentity(releaseFile);
  const definition = {
    name      : 'health-platform',
    version   : 1,
    execution : 'nonTransactional',
    retrySafe : true,
    /**
     * Añade la disponibilidad del listener HTTP a la salud técnica de base de datos y mensajería.
     * @returns {Promise<Object>} Objeto con statusCode, data.
     */
    handler   : async () => {
      const response          = await health(connection, messaging, identity);
      const address           = server.address();
      const ready             =
        server.listening &&
        address &&
        typeof address === 'object' &&
        Number.isInteger(address.port) &&
        address.port >= 1 &&
        address.port <= 65535;
      const socket            = ready ? 'ready' : 'unavailable';
      const documentsRealtime = documentsReady() ? 'ready' : 'unavailable';
      return {
        ...response,
        statusCode :
          response.statusCode === 200 && socket === 'ready' && documentsRealtime === 'ready'
            ? 200
            : 503,
        data       : {
          ...response.data,
          socket,
          documentsRealtime,
        },
      };
    },
  };
  messaging.registerAction(definition);
  return definition.handler;
}

module.exports = { registerSocketHealth };
