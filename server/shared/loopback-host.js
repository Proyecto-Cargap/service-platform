/** @file Implementación de loopback-host en service-platform. */
'use strict';

/** Resolve an explicit loopback address before opening service dependencies.
 * @param {*} value Valor de value utilizado por esta operación.
 * @param {*} variable Valor de variable utilizado por esta operación.
 * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function loopbackHost(value, variable) {
  const host = value === undefined || value === '' ? '127.0.0.1' : value;
  if (host !== '127.0.0.1' && host !== '::1') {
    const error = new Error('Listener host must be an explicit loopback address');
    error.code = 'INVALID_' + variable;
    throw error;
  }
  return host;
}

module.exports = { loopbackHost };
