/** @file Diferencias por ítem de ventanas públicas; no realiza consultas ni entrega instantáneas. */
'use strict';
const { isDeepStrictEqual } = require('node:util');
const { createHash }        = require('node:crypto');

/** @param {string} value Segmento de una ruta. @returns {string} Segmento JSON Pointer escapado. */
function segment(value) {
  return value.replace(/~/g, '~0').replace(/\//g, '~1');
}

/** @param {*} value Datos JSON públicos. @returns {*} Representación con claves estables. */
function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') {
    const entries = [];
    for (const key of Object.keys(value).sort()) entries.push([key, canonical(value[key])]);
    return Object.fromEntries(entries);
  }
  return value;
}

/** @param {*} value Representación pública exacta. @returns {string} Cursor de contenido, no reloj ni secuencia de eventos. */
function contentCursor(value) {
  return createHash('sha256')
    .update(JSON.stringify(canonical(value)))
    .digest('hex');
}

/**
 * Compara datos públicos conservando identificadores y orden de colecciones declaradas.
 * @param {*} previous Base entregada por HTTP.
 * @param {*} current Nueva lectura autorizada del productor.
 * @param {Object<string,string>} collections Rutas de arreglos y campos de identidad del dominio.
 * @returns {Object[]} Operaciones JSON Patch ordenadas; nunca modifica sus argumentos.
 */
function windowDelta(previous, current, collections = {}) {
  const operations = [];
  /** @param {*} before Valor anterior. @param {*} after Valor nuevo. @param {string} path Ruta pública. @returns {void} Acumula sólo diferencias. */
  function compare(before, after, path) {
    if (isDeepStrictEqual(before, after)) return;
    const key = collections[path];
    /** @param {Object} item Fila pública. @returns {unknown} Identidad simple o anidada definida exclusivamente por el registro del servidor. */
    function identity(item) {
      let value = item;
      for (const part of key.split('.'))
        value =
          value && typeof value === 'object' && Object.hasOwn(value, part)
            ? value[part]
            : undefined;
      return value;
    }

    if (key && Array.isArray(before) && Array.isArray(after)) {
      const work        = before.slice();
      const ids         = new Set();
      const previousIds = new Set();
      for (const item of before) {
        const id = identity(item);
        if (typeof id !== 'string' || !id || previousIds.has(id))
          throw new Error('INVALID_WINDOW_IDENTITY');
        previousIds.add(id);
      }
      for (const item of after) {
        const id = identity(item);
        if (typeof id !== 'string' || !id || ids.has(id))
          throw new Error('INVALID_WINDOW_IDENTITY');
        ids.add(id);
      }
      for (let index = work.length - 1; index >= 0; index--)
        if (!ids.has(identity(work[index]))) {
          operations.push({
            op   : 'remove',
            path : `${path}/${index}`,
          });
          work.splice(index, 1);
        }
      for (let index = 0; index < after.length; index++) {
        const item  = after[index];
        const found = work.findIndex(
          /** @param {Object} candidate Registro anterior. @returns {boolean} Conserva la identidad del ítem. */
          (candidate) => identity(candidate) === identity(item),
        );
        if (found === -1) {
          operations.push({
            op    : 'add',
            path  : `${path}/${index}`,
            value : item,
          });
          work.splice(index, 0, item);
        } else {
          if (found !== index) {
            operations.push({
              op   : 'move',
              from : `${path}/${found}`,
              path : `${path}/${index}`,
            });
            work.splice(index, 0, work.splice(found, 1)[0]);
          }
          compare(work[index], item, `${path}/${index}`);
          work[index] = item;
        }
      }
      return;
    }
    if (
      before &&
      after &&
      typeof before === 'object' &&
      typeof after === 'object' &&
      !Array.isArray(before) &&
      !Array.isArray(after)
    ) {
      for (const field of Object.keys(before))
        if (!Object.hasOwn(after, field))
          operations.push({
            op   : 'remove',
            path : `${path}/${segment(field)}`,
          });
      for (const field of Object.keys(after)) {
        const target = `${path}/${segment(field)}`;
        if (!Object.hasOwn(before, field))
          operations.push({
            op    : 'add',
            path  : target,
            value : after[field],
          });
        else compare(before[field], after[field], target);
      }
      return;
    }
    operations.push({
      op    : 'replace',
      path,
      value : after,
    });
  }
  compare(previous, current, '');
  return operations;
}

module.exports = {
  contentCursor,
  windowDelta,
};
