/** @file Instantáneas del productor local y comprobación del resultado de una lectura; no emite cursores para respuestas de servicios remotos. */
'use strict';
const { contentCursor } = require('./window-delta');
/** @param {*} data Consulta canónica ejecutada dentro del servicio propietario. @returns {Object} Datos exactos y cursor emitido por ese productor local. */
function produceSnapshot(data) {
  return {
    data,
    cursor : contentCursor(data),
  };
}
/** @param {Object} value Resultado del productor local o recibo RPC acreditado. @returns {Object} Snapshot explícito; no reconstruye un cursor ausente. */
function validatedSnapshot(value) {
  if (
    !value ||
    !Object.hasOwn(value, 'data') ||
    typeof value.cursor !== 'string' ||
    !/^[a-f0-9]{64}$/.test(value.cursor)
  )
    throw new Error('PRODUCER_CURSOR_REQUIRED');
  return value;
}
module.exports = {
  produceSnapshot,
  validatedSnapshot,
};
