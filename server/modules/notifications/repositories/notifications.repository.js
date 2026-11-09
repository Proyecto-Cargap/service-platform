/** @file Persistencia local de notifications; el catálogo proporciona el modelo. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');
const policy               = require('../policies/notifications.policy');

/**
 * Compone createNotificationsRepository con las dependencias recibidas y devuelve su interfaz.
 * @param {Object} options Colaboradores u opciones: model, messaging.
 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
module.exports = function createNotificationsRepository({ model, messaging }) {
  const changes = require('../../../adapters/mongodb/platform-changes').platformChanges({
    messaging,
    connection : model.db,
  });
  return {
    /** Persiste una entrega interna acreditada con identidad única por evento y destinatario.
     * @param {Object} input Aviso ya revalidado por el manejador de dominio.
     * @returns {Promise<Object>} Referencia estable; los reintentos nunca crean una segunda fila.
     */
    async createEvent(input) {
      const key = {
        sourceEventKey : input.sourceEventKey,
        workspace      : input.workspace,
        userAssigned   : input.userAssigned,
      };
      let saved;
      try {
        saved = await changes.commit(
          /** @param {Object} session Unidad idempotente del aviso. @returns {Promise<Object>} Documento existente o alta con su hecho. */
          async (session) => {
            const existing = await model.findOne(key).session(session).lean();
            if (existing) return existing;
            const [item] = await model.create([input], { session });
            await changes.emit('notification', item, session, { created: true });
            return item;
          },
        );
      } catch (error) {
        if (error.code !== 11000) throw error;
        saved = await model.findOne(key).lean();
      }
      if (!saved) throw new ApplicationError(503, 'NOTIFICATION_PERSISTENCE_UNAVAILABLE');
      return {
        notificationCode : saved.notificationCode,
        workspace        : saved.workspace,
      };
    },
    /** Guarda una notificación autorizada y devuelve únicamente su referencia canónica.
     * @param {Object} input Entrada que se procesa con las reglas del módulo.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @returns {Promise<{notificationCode:string,workspace:string}>} Referencia simple del registro persistido; propaga errores antes de publicar.
     */
    async create(input, scope = '') {
      if (!input || typeof input !== 'object' || Array.isArray(input))
        throw new ApplicationError(400, 'INVALID_INPUT');
      const authorizedInput = policy.authorize('create', input, scope, messaging.getContext());
      if (!authorizedInput) throw new ApplicationError(403, 'FORBIDDEN');
      const saved = await changes.commit(
        /** @param {Object} session Escritura acreditada. @returns {Promise<Object>} Alta y publicación indivisibles. */
        async (session) => {
          const document = await new model(authorizedInput).save({ session });
          await changes.emit('notification', document, session, { created: true });
          return document;
        },
      );
      return {
        notificationCode : saved.notificationCode,
        workspace        : saved.workspace,
      };
    },
  };
};
