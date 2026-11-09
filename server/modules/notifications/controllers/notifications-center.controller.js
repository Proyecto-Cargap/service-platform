/** @file Adaptación RPC del centro personal, administración y trabajos de difusión. */
'use strict';
const notificationsCenterService = require('../services/notifications-center.service');
const { ApplicationError }       = require('../../../shared/application-error');
/**
 * Construye handlers RPC con un servicio por operación y dependencias explícitas.
 * @param {Object} dependencies Mensajería y repositorio.
 * @returns {Object} Operación pública execute con nombres internos cerrados.
 */
function notificationsCenterController(dependencies) {
  return {
    /**
     * Delega exclusivamente las operaciones declaradas por el catálogo local.
     * @param {string} operation Caso de uso seleccionado por el catálogo, nunca por el navegador.
     * @param {Object} mqMessage Payload público y contexto verificado por el transporte.
     * @returns {Promise<Object>} Envelope público, incluido 409 con nueva audiencia a confirmar.
     */
    async execute(operation, mqMessage, snapshot = false) {
      if (
        !snapshot &&
        [
          'personalList',
          'personalOne',
          'personalCount',
          'adminList',
          'adminOne',
          'broadcasts',
          'status',
        ].includes(operation)
      )
        return dependencies.repository.snapshot(
          /** @returns {Promise<Object>} Caso de uso completo sobre el mismo estado. */
          () => this.execute(operation, mqMessage, true),
        );
      const data = mqMessage.data || {};
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      const service = notificationsCenterService(dependencies);
      if (operation === 'confirm') {
        const result = await service.confirm(data);
        return result.audienceChanged
          ? {
              statusCode      : 409,
              descriptionCode : 'NOTIFICATION_AUDIENCE_CHANGED',
              data            : {
                ...result.item,
                audienceChanged : true,
                reasonCode      : 'NOTIFICATION_AUDIENCE_CHANGED',
              },
            }
          : dependencies.messaging.successResponse(202, result.item);
      }
      const bulk   = {
        personalViewAll      : 'personalView',
        personalDeleteViewed : 'personalDelete',
      };
      const result = bulk[operation]
        ? await service[bulk[operation]](data, true)
        : await service[operation](data);
      const status = operation === 'preview' ? 201 : operation === 'retry' ? 202 : 200;
      return dependencies.messaging.successResponse(status, result);
    },
  };
}
module.exports = { notificationsCenterController };
