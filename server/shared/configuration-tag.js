/** @file Token de concurrencia del contenido público; no depende de fechas ni del orden de claves. */
'use strict';
const { createHash } = require('node:crypto');
/** Serializa objetos de forma determinista y conserva el orden significativo de los arreglos.
 * @param {*} value Valor de value utilizado por esta operación.
 * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object')
    return `{${Object.keys(value)
      .sort()
      .map(
        /**
         * Proyecta cada elemento en la estructura requerida por la operación.
         * @param {string} key Clave del campo o recurso que se procesa.
         * @returns {string} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        (key) => `${JSON.stringify(key)}:${canonical(value[key])}`,
      )
      .join(',')}}`;
  return JSON.stringify(value);
}
/** Devuelve un único token fuerte para el contenido completo de configuración.
 * @param {Object} data Datos de entrada de la operación.
 * @returns {string} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function configurationTag(data) {
  return `"cfg-${createHash('sha256').update(canonical(data)).digest('hex')}"`;
}
/** Sólo admite un token emitido por esta capacidad; no admite comodines ni comparaciones débiles.
 * @param {*} value Valor de value utilizado por esta operación.
 * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function validConfigurationTag(value) {
  return typeof value === 'string' && /^"cfg-[0-9a-f]{64}"$/.test(value);
}
module.exports = {
  configurationTag,
  validConfigurationTag,
};
