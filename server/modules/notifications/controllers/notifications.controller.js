/** @file Adaptación RPC de la creación canónica de notificaciones; delega permisos y efectos al caso de uso. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');
/** Compone el controlador con un caso de uso ya construido en el catálogo.
 * @param {Object} dependencies Mensajería de respuesta y capacidad create.
 * @returns {Object} Manejador RPC sin persistencia ni construcción de servicios.
 */
function notificationsController({ messaging, service }) {
  return {
    /** Delega una creación y representa su referencia estable con estado 201.
     * @param {Object} message Solicitud cuyos datos valida el DTO de la acción.
     * @returns {Promise<Object>} Sobre público con referencia canónica.
     * @throws {ApplicationError} Propaga autorización y entrega incierta del caso de uso.
     */
    async create(message = {}) {
      const data = message.data;
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      return messaging.successResponse(201, await service.create(data));
    },
  };
}
module.exports = { notificationsController };
