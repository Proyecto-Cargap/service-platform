/** @file Parámetros técnicos globales; el servidor selecciona guest y fija su autoría. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');
const policy               = require('../policies/settings.policy');
/**
 * Compone createSettingsRepository con las dependencias recibidas y devuelve su interfaz.
 * @param {Object} options Colaboradores u opciones: model, messaging.
 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
module.exports = function createSettingsRepository({ model, messaging }) {
  return {
    /**
     * Crea el registro y devuelve el resultado de la operación.
     * @param {*} parameter Valor de parameter utilizado por esta operación.
     * @param {Object} data Datos de entrada de la operación.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async create(parameter, data, scope = '') {
      if (typeof parameter !== 'string' || !parameter)
        throw new ApplicationError(400, 'INVALID_INPUT');
      const context = messaging?.getContext() || {};
      const input   = policy.authorize(
        'create',
        {
          parameter,
          data,
          workspace    : 'allWorkspaces',
          userCreate   : context.user?.userCode || '',
          userAssigned : '',
        },
        scope,
        context,
      );
      if (!input) throw new ApplicationError(403, 'FORBIDDEN');
      return new model(input).save();
    },
    /**
     * Obtiene el valor solicitado.
     * @param {*} parameter Valor de parameter utilizado por esta operación.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async get(parameter, scope = '') {
      if (typeof parameter !== 'string' || !parameter)
        throw new ApplicationError(400, 'INVALID_INPUT');
      const filter = policy.authorize('view', { parameter }, scope, messaging?.getContext() || {});
      return filter ? model.findOne(filter) : null;
    },
    /**
     * Resuelve getAll aplicando los filtros y colaboradores del módulo.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async getAll(scope = '') {
      const filter = policy.authorize('view', {}, scope, messaging?.getContext() || {});
      if (!filter) return [];
      const items = await model.find(filter).limit(101);
      if (items.length > 100) throw new ApplicationError(409, 'SETTINGS_COLLECTION_TOO_LARGE');
      return items;
    },
    /**
     * Actualiza los campos autorizados del registro.
     * @param {*} parameter Valor de parameter utilizado por esta operación.
     * @param {Object} data Datos de entrada de la operación.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async update(parameter, data, scope = '') {
      if (typeof parameter !== 'string' || !parameter)
        throw new ApplicationError(400, 'INVALID_INPUT');
      const changes = { data };
      const filter  = policy.authorize('update', { parameter }, scope, {
        ...(messaging?.getContext() || {}),
        changes,
      });
      if (!filter) throw new ApplicationError(403, 'FORBIDDEN');
      return model.findOneAndUpdate(filter, changes, {
        returnDocument : 'after',
        runValidators  : true,
      });
    },
  };
};
