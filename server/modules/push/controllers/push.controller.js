/** @file Frontera RPC de instalaciones propias y eventos acreditados de Communication. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');
/**
 * Compone registro de dispositivo y recepción durable con orígenes explícitos.
 * @param {Object} dependencies Mensajería, caso de uso de registro y puerto de cola.
 * @returns {Object} register y enqueue registrados en el catálogo.
 */
function createPushController({ messaging, registration, repository }) {
  return {
    /** Registra únicamente una instalación de una sesión acreditada por el gateway.
     * @param {Object} message Datos ya validados por DTO y origen del transporte.
     * @returns {Promise<Object>} Sobre 200 sin credenciales ni dirección FCM.
     */
    async register(message) {
      if (message.originService !== 'service-apigateway')
        throw new ApplicationError(403, 'FORBIDDEN');
      return messaging.successResponse(
        200,
        await registration.register(message.data, messaging.getContext()),
      );
    },
    /** Una reentrega del evento recupera exactamente las mismas filas de la cola.
     * @param {Object} message Evento emitido transaccionalmente por Communication.
     * @returns {Promise<void>} Confirma persistencia de todos los destinatarios antes del acuse.
     */
    async enqueue(message) {
      if (message.originService !== 'service-communication')
        throw new ApplicationError(403, 'FORBIDDEN');
      await repository.enqueue(message.data);
    },
  };
}
module.exports = { createPushController };
