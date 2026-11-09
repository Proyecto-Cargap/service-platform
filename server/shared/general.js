/** @file Transformaciones puras para ordenación, texto e identificadores visibles. */
'use strict';
const { ApplicationError } = require('./application-error');
/**
 * Ejecuta pad dentro del flujo de general.
 * @param {*} value Valor de value utilizado por esta operación.
 * @param {number|string} size Valor de size utilizado por esta operación.
 * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function pad(value, size) {
  return String(value).padStart(size, '0');
}
/**
 * Resuelve getSortQuery aplicando los filtros y colaboradores del módulo.
 * @param {string} field Campo utilizado por la operación.
 * @param {*} [sort] Valor de sort utilizado por esta operación.
 * @param {*} [order] Valor de order utilizado por esta operación.
 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function getSortQuery(field, sort = null, order = null) {
  const key = typeof sort === 'string' && sort ? sort : field;
  if (
    !/^[a-zA-Z_][a-zA-Z0-9_.]*$/.test(key) ||
    ['__proto__', 'constructor', 'prototype'].includes(key)
  )
    throw new TypeError('Invalid sort field');
  const direction = order == null ? 'asc' : order;
  if (!['asc', 'desc', 'ascending', 'descending', 1, -1, '1', '-1'].includes(direction))
    throw new TypeError('Invalid sort direction');
  return { [key]: direction };
}
/**
 * Ejecuta escapeRegex dentro del flujo de general.
 * @param {*} value Valor de value utilizado por esta operación.
 * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function escapeRegex(value) {
  return String(value ?? '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
/**
 * Valida la paginación pública y convierte la página de base uno a base cero para persistencia.
 * @param {Object} [input] Entrada con page y perPage opcionales; admite valores numéricos de transporte.
 * @returns {{page: number, perPage: number}} Página interna y tamaño; cero indica ausencia de límite.
 * @throws {ApplicationError} INVALID_PAGINATION si un valor no es entero o está fuera de rango.
 */
function getPagination(input = {}) {
  for (const key of ['page', 'perPage']) {
    if (
      input[key] !== undefined &&
      (!Number.isInteger(Number(input[key])) ||
        Number(input[key]) < (key === 'page' ? 1 : 0) ||
        Number(input[key]) > 10000)
    )
      throw new ApplicationError(400, 'INVALID_PAGINATION');
  }
  const perPage = parseInt(input.perPage) || 0;
  const page    = (parseInt(input.page) || 1) - 1;
  return {
    page,
    perPage,
  };
}

module.exports = {
  pad,
  getSortQuery,
  escapeRegex,
  getPagination,
};

/** Paginación pública acotada; las lecturas internas completas usan un contrato separado.
 * @param {Object} [input] Entrada que se procesa con las reglas del módulo.
 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function getPublicPagination(input = {}) {
  if (!input || typeof input !== 'object' || Array.isArray(input))
    throw new ApplicationError(400, 'INVALID_PAGINATION');
  const values = {};
  for (const [key, fallback, maximum] of [
    ['page', 1, 10000],
    ['perPage', 20, 100],
  ]) {
    const value = input[key] === undefined ? fallback : input[key];
    if (
      (typeof value !== 'number' && typeof value !== 'string') ||
      (typeof value === 'string' && !/^\d+$/.test(value)) ||
      !Number.isSafeInteger(Number(value)) ||
      Number(value) < 1 ||
      Number(value) > maximum
    )
      throw new ApplicationError(400, 'INVALID_PAGINATION');
    values[key] = Number(value);
  }
  return {
    page    : values.page - 1,
    perPage : values.perPage,
  };
}
module.exports.getPublicPagination = getPublicPagination;

/** Recorre un contrato público por lotes; falla si cambian conteos o falta una página.
 * @param {*} readPage Valor de readPage utilizado por esta operación.
 * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
module.exports.collectPages = async function collectPages(readPage) {
  const items = [];
  let total;
  for (let page = 1; page <= 10000; page++) {
    const result = await readPage({
      page,
      perPage : 100,
    });
    const meta   = result?.pagination;
    if (
      !meta ||
      !Array.isArray(result.items) ||
      !Number.isSafeInteger(meta.total) ||
      meta.total < 0 ||
      meta.page !== page ||
      meta.perPage !== 100 ||
      (total !== undefined && total !== meta.total) ||
      result.items.length !== Math.min(100, Math.max(0, meta.total - items.length))
    )
      throw new ApplicationError(409, 'PAGINATION_CHANGED');
    total = meta.total;
    items.push(...result.items);
    if (items.length === total)
      return {
        items,
        pagination : {
          total,
          page    : 1,
          perPage : items.length,
        },
      };
  }
  throw new ApplicationError(400, 'COLLECTION_TOO_LARGE');
};
