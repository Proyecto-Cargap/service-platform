/** @file Página documental autorizada: consulta canónica, pertenencia y recuperación de ancestros. */
'use strict';
const { readSnapshot }     = require('../../../library/messaging/read-receipt');
const { ApplicationError } = require('../../../shared/application-error');
const { validate }         = require('../../../contracts/action-dto');
const contract             = require('../../../contracts/service.json').endpoints.find(
  /** @param {Object} endpoint Contrato registrado. @returns {boolean} Lectura estándar. */
  (endpoint) => endpoint.name === 'realtime-read',
);
/** @param {Object} dependencies Transporte con contexto autenticado. @returns {Object} Secciones explícitas de Documents. */
function documentsWindows({ messaging }) {
  return {
    page : {
      collections : { '/listItem': 'id' },
      /** @param {Object} query Filtros recibidos. @returns {Object} Consulta cerrada sin autoridad ni registros del cliente. */
      validate(query) {
        if (
          !validate(query, contract.request.oneOf[0].properties.query) ||
          (query.favorites && query.trash) ||
          (!query.favorites && !query.repositoryId)
        )
          throw new ApplicationError(400, 'INVALID_DOCUMENT_VIEW');
        return {
          ...query,
          trash  : query.trash === true,
          search : query.search.normalize('NFC'),
        };
      },
      /** @param {Object} query Consulta. @param {Object} identity Principal vigente. @returns {Promise<Object>} Instantánea pública del productor. */
      read(query, identity) {
        return this.readWindow(query, identity, null);
      },
      /** @param {Object} query Consulta original. @param {Object} identity Principal vigente. @param {Object|null} anchor Base acreditada por el backend. @returns {Promise<Object>} Página canónica; ancestros proceden exclusivamente de una lectura del productor. */
      async readWindow(query, identity, anchor) {
        const response = await messaging.request(
          'service-documents',
          'documents-view-get',
          {
            ...query,
            ...(anchor
              ? {
                  page     : anchor.page,
                  parentId : anchor.parentId,
                }
              : {}),
            ancestors : (anchor?.breadcrumbs || [])
              .map(
                /** @param {Object} item Ancestro acreditado. @returns {string|null} Identificador. */
                (item) => item.id,
              )
              .filter(Boolean),
          },
          {
            timeoutMs : 15000,
            context   : identity.context,
          },
        );
        if (response?.statusCode !== 200)
          throw new ApplicationError(
            response?.statusCode || 503,
            response?.descriptionCode || 'DOCUMENT_VIEW_UNAVAILABLE',
          );
        if (!validate(response.data, contract.successData.oneOf[0].properties.data))
          throw new ApplicationError(503, 'INVALID_DOCUMENT_PROJECTION');
        return readSnapshot(response);
      },
      /** @param {Object} query Ventana. @param {Object} change Hecho durable o revocación. @param {Object} identity Principal. @param {Object} base Proyección autorizada instalada. @returns {boolean} Sólo ventanas que pueden cambiar por ese hecho. */
      affects(query, change, identity, base) {
        if (change.control)
          return change.control.kind === 'user'
            ? change.control.code === identity.userCode
            : identity.context.groups?.includes(change.control.code) === true;
        if (change.resourceAccess)
          return query.favorites || !base || base.repository?.kind === 'resource';
        if (change.operation === 'favorite' && change.userCode !== identity.userCode) return false;
        return (
          query.favorites ||
          query.selectedId === change.entryId ||
          query.repositoryId === change.repositoryId ||
          query.repositoryId === change.previousRepositoryId
        );
      },
    },
  };
}
module.exports = { documentsWindows };
