/** @file Hechos mínimos de Platform persistidos con la modificación del dominio. */
'use strict';
const { ApplicationError } = require('../../shared/application-error');
const { validate }         = require('../../contracts/action-dto');
/** @param {Object} dependencies Mensajería y conexión propietaria. @returns {Object} Publicación atómica sin contenido privado. */
function platformChanges({ messaging, connection }) {
  return {
    /** @param {Function} work Escritura y hechos en la misma sesión. @returns {Promise<*>} Resultado confirmado por MongoDB. */
    async commit(work) {
      try {
        return await connection.transaction(work, { readConcern: { level: 'snapshot' } });
      } catch (error) {
        if (
          error.hasErrorLabel?.('UnknownTransactionCommitResult') ||
          /Mongo(Network|ServerSelection)/.test(error.name || '')
        )
          throw new ApplicationError(503, 'PLATFORM_WRITE_UNCERTAIN', {
            uncertain : true,
            cause     : error,
          });
        throw error;
      }
    },
    /** @param {string} resource Recurso registrado. @param {Object} item Identidad del cambio, nunca una proyección pública. @param {Object} session Transacción de negocio. @param {Object} flags Alta o retirada. @returns {Promise<void>} Outbox confirmado junto con el cambio. */
    async emit(resource, item, session, flags = {}) {
      if (!session) throw new TypeError('PLATFORM_CHANGE_SESSION_REQUIRED');
      const data   = {
        resource,
        code      : item.notificationCode || item.broadcastCode || item.releaseCode || item.code || '',
        workspace : item.workspace || 'allWorkspaces',
        userCode  : ['notification', 'preference'].includes(resource) ? item.userCode : '',
        created   : flags.created === true,
        erased    : flags.erased === true,
      };
      const name   = 'platform.resource.changed';
      const schema = require('../../contracts/service.json').publishedEvents[name];
      if (!validate(data, schema)) throw new TypeError('INVALID_PLATFORM_CHANGE');
      await messaging.publishEvent(name, data, {
        session,
        context : {},
      });
    },
  };
}
module.exports = { platformChanges };
