/** @file Configuración única global: selección autorizada y metadatos de creación canónicos. */
'use strict';
const { ApplicationError }                        = require('../../../shared/application-error');
const policy                                      = require('../policies/configuration.policy');
const { configurationTag, validConfigurationTag } = require('../../../shared/configuration-tag');
/**
 * Compone createConfigurationRepository con las dependencias recibidas y devuelve su interfaz.
 * @param {Object} options Colaboradores u opciones: model.
 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
module.exports = function createConfigurationRepository({ model, messaging }) {
  const events = require('../../../adapters/mongodb/platform-changes').platformChanges({
    connection : model.db,
    messaging,
  });
  return {
    /**
     * Obtiene el valor solicitado.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    get() {
      return model
        .findOne(policy.authorize('view', { _id: 'global' }, policy.trustedScopes.guest), {
          'data.menus' : 1,
        })
        .lean();
    },
    /**
     * Resuelve getInformation aplicando los filtros y colaboradores del módulo.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    getInformation() {
      return model
        .findOne(policy.authorize('view', { _id: 'global' }, policy.trustedScopes.guest), {
          'data.title'             : 1,
          'data.publicUrl'         : 1,
          'data.logos.email'       : 1,
          'data.theme.primary.900' : 1,
        })
        .lean();
    },
    /**
     * Ejecuta put dentro del flujo de configuration.repository.
     * @param {Object} data Datos de entrada de la operación.
     * @param {Object} context Contexto de identidad y alcance de la operación.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @param {*} expectedTag Valor de expectedTag utilizado por esta operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async put(data, context, scope = '', expectedTag) {
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      const changes = {
        $set : {
          'data.menus' : data.menus,
          updatedAt    : new Date(),
          updatedBy    : context.user?.userCode,
        },
      };
      let filter    = policy.authorize('update', { _id: 'global' }, scope, {
        ...context,
        changes,
      });
      if (!filter) throw new ApplicationError(403, 'FORBIDDEN');
      return events.commit(
        /** @param {Object} session Unidad atómica. @returns {Promise<Object>} Menú confirmado junto con su hecho. */ async (
          session,
        ) => {
          if (expectedTag !== undefined) {
            if (!validConfigurationTag(expectedTag))
              throw new ApplicationError(400, 'N0005-CONFIG-INVALID');
            const snapshot = await model
              .findOne(filter, { 'data.menus': 1 })
              .session(session)
              .lean();
            if (!snapshot) throw new ApplicationError(403, 'FORBIDDEN');
            if (configurationTag(snapshot.data) !== expectedTag)
              throw new ApplicationError(412, 'N0005-CONFIG-CHANGED');
            filter = { $and: [filter, { 'data.menus': snapshot.data.menus }] };
          }
          const result = await model
            .findOneAndUpdate(filter, changes, {
              session,
              returnDocument : 'after',
              runValidators  : true,
              projection     : { 'data.menus': 1 },
            })
            .lean();
          if (!result)
            throw new ApplicationError(
              expectedTag === undefined ? 403 : 412,
              expectedTag === undefined ? 'FORBIDDEN' : 'N0005-CONFIG-CHANGED',
            );
          await events.emit(
            'configuration',
            {
              code      : 'global',
              workspace : 'allWorkspaces',
            },
            session,
          );
          return result;
        },
      );
    },
  };
};
