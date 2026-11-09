/** @file Navegación pública e información central de plataforma para consumidores del servidor. */
'use strict';
const { ApplicationError }    = require('../../../shared/application-error');
const contract                = require('../contracts/configuration.contract.json');
const policy                  = require('../policies/configuration.policy');
const { platformInformation } = require('../contracts/configuration-information');
/**
 * Ejecuta failure dentro del flujo de configuration.service.
 * @param {number} statusCode Valor de statusCode utilizado por esta operación.
 * @param {string} code Identificador público del recurso.
 * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function failure(statusCode, code) {
  return new ApplicationError(statusCode, `N0005-CONFIG-${code}`);
}
/**
 * Comprueba las condiciones de validateConfiguration antes de continuar.
 * @param {Object} data Datos de entrada de la operación.
 * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function validateConfiguration(data) {
  if (
    !data ||
    typeof data !== 'object' ||
    Array.isArray(data) ||
    Object.keys(data).length !== 1 ||
    !Object.hasOwn(data, 'menus') ||
    !data.menus ||
    typeof data.menus !== 'object' ||
    Array.isArray(data.menus) ||
    Object.keys(data.menus).length !== 2 ||
    !contract.menus.every(
      /**
       * Comprueba la condición requerida para cada elemento.
       * @param {string} key Clave del campo o recurso que se procesa.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      (key) => Object.hasOwn(data.menus, key),
    )
  )
    throw new Error('INVALID_CONFIGURATION');
  for (const [name, privateMenu] of [
    ['public', false],
    ['main', true],
  ]) {
    const seen    = new Set();
    const pending = [
      {
        items : data.menus[name],
        depth : 0,
      },
    ];
    while (pending.length) {
      const { items, depth } = pending.pop();
      if (!Array.isArray(items) || items.length > 100 || depth > 3)
        throw new Error('INVALID_CONFIGURATION');
      for (const item of items) {
        if (
          !item ||
          typeof item !== 'object' ||
          Array.isArray(item) ||
          typeof item.key !== 'string' ||
          !/^[a-z][a-z0-9-]{0,79}$/.test(item.key) ||
          seen.has(item.key) ||
          typeof item.label !== 'string' ||
          !item.label.trim() ||
          item.label.length > 120 ||
          !contract.icons.includes(item.icon)
        )
          throw new Error('INVALID_CONFIGURATION');
        seen.add(item.key);
        const branch = Object.hasOwn(item, 'items');
        const keys   = ['key', 'label', 'icon', branch ? 'items' : 'routerLink'];
        if (Object.hasOwn(item, 'policyIdentifier')) {
          if (
            !privateMenu ||
            typeof item.policyIdentifier !== 'string' ||
            !new RegExp(contract.navigationPolicyPattern).test(item.policyIdentifier)
          )
            throw new Error('INVALID_CONFIGURATION');
          keys.push('policyIdentifier');
        }
        if (
          Object.keys(item).length !== keys.length ||
          !keys.every(
            /**
             * Comprueba la condición requerida para cada elemento.
             * @param {string} key Clave del campo o recurso que se procesa.
             * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
             */
            (key) => Object.hasOwn(item, key),
          )
        ) {
          throw new Error('INVALID_CONFIGURATION');
        }
        if (branch) {
          if (!Array.isArray(item.items) || !item.items.length)
            throw new Error('INVALID_CONFIGURATION');
          pending.push({
            items : item.items,
            depth : depth + 1,
          });
        } else if (
          typeof item.routerLink !== 'string' ||
          !/^\/(?!\/)[a-zA-Z0-9/_-]*$/.test(item.routerLink) ||
          (privateMenu
            ? !/^\/app(?:\/|$)/.test(item.routerLink)
            : /^\/app(?:\/|$)/.test(item.routerLink))
        ) {
          throw new Error('INVALID_CONFIGURATION');
        }
      }
    }
  }
  return data;
}
/**
 * Compone configurationService con las dependencias recibidas y devuelve su interfaz.
 * @param {Object} [options] Colaboradores u opciones: repository.
 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function configurationService({ repository } = {}) {
  return {
    /**
     * Obtiene el valor solicitado.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async get() {
      if (!policy.authorize('view', {}, policy.trustedScopes.guest))
        throw failure(403, 'FORBIDDEN');
      const row = await repository.get();
      try {
        return validateConfiguration({ menus: row?.data?.menus });
      } catch {
        throw failure(503, 'UNAVAILABLE');
      }
    },
    /**
     * Resuelve getInformation aplicando los filtros y colaboradores del módulo.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async getInformation() {
      try {
        return platformInformation((await repository.getInformation())?.data);
      } catch {
        throw failure(503, 'INFORMATION-UNAVAILABLE');
      }
    },
    /**
     * Ejecuta put dentro del flujo de configuration.service.
     * @param {Object} data Datos de entrada de la operación.
     * @param {Object} context Contexto de identidad y alcance de la operación.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @param {*} expectedTag Valor de expectedTag utilizado por esta operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async put(data, context, scope = '', expectedTag) {
      if (
        !policy.authorize('update', {}, scope, {
          ...context,
          changes : { data },
        })
      )
        throw failure(403, 'FORBIDDEN');
      try {
        validateConfiguration(data);
      } catch {
        throw failure(400, 'INVALID');
      }
      const row = await repository.put(data, context, scope, expectedTag);
      return validateConfiguration({ menus: row?.data?.menus });
    },
  };
}
module.exports = {
  configurationService,
  validateConfiguration,
};
