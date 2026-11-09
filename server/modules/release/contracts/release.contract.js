/** @file Campos seguros del catálogo global de versiones y validación de textos editoriales. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');
const FIELDS               = 'releaseCode version releaseDate state description webBuild -_id';
const INDEXES              = [
  {
    key                     : { version: 1 },
    name                    : 'release_global_version',
    unique                  : true,
    partialFilterExpression : {
      workspace : 'allWorkspaces',
      erased    : false,
    },
  },
  {
    key                     : { state: 1 },
    name                    : 'release_global_current',
    unique                  : true,
    partialFilterExpression : {
      workspace : 'allWorkspaces',
      erased    : false,
      state     : '1-current',
    },
  },
];
/**
 * Ejecuta globalFilter dentro del flujo de release.contract.
 * @param {Object} [condition] Valor de condition utilizado por esta operación.
 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function globalFilter(condition = {}) {
  return {
    $and : [
      {
        workspace : 'allWorkspaces',
        erased    : false,
      },
      condition,
    ],
  };
}
/**
 * Ejecuta publishedRelease dentro del flujo de release.contract.
 * @param {*} value Valor de value utilizado por esta operación.
 * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function publishedRelease(value) {
  const result = Object.fromEntries(
    ['releaseCode', 'version', 'releaseDate', 'state', 'description'].map(
      /**
       * Proyecta cada elemento en la estructura requerida por la operación.
       * @param {string} key Clave del campo o recurso que se procesa.
       * @returns {Array<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      (key) => [key, value[key]],
    ),
  );
  result.webBuild = value.webBuild
    ? Object.fromEntries(
        ['id', 'sourceSha', 'environment'].map(
          /**
           * Proyecta cada elemento en la estructura requerida por la operación.
           * @param {string} key Clave del campo o recurso que se procesa.
           * @returns {Array<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          (key) => [key, value.webBuild[key]],
        ),
      )
    : null;
  return result;
}
/**
 * Ejecuta releaseText dentro del flujo de release.contract.
 * @param {*} value Valor de value utilizado por esta operación.
 * @param {string} field Campo utilizado por la operación.
 * @param {number} limit Valor de limit utilizado por esta operación.
 * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function releaseText(value, field, limit) {
  if (typeof value !== 'string' || !value.trim() || value.trim().length > limit)
    throw new ApplicationError(400, `INVALID_RELEASE_${field.toUpperCase()}`);
  return value.trim();
}
module.exports = {
  FIELDS,
  INDEXES,
  globalFilter,
  publishedRelease,
  releaseText,
};
