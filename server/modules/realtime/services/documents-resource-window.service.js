/** @file Catálogo, fuentes, referencias y configuración documentales con lecturas canónicas autorizadas. */
'use strict';
const { readSnapshot }     = require('../../../library/messaging/read-receipt');
const { ApplicationError } = require('../../../shared/application-error');
const { validate }         = require('../../../contracts/action-dto');
const contract             = require('../../../contracts/service.json').endpoints.find(
  /** @param {Object} entry Contrato. @returns {boolean} Lectura estándar. */
  (entry) => entry.name === 'realtime-read',
);
const actions              = {
  'documents.repository/collection' : 'documents-repositories-get-list',
  'documents.repository/resource'   : 'documents-repository-resolve',
  'documents.entry/page'            : 'documents-entries-get-list',
  'documents.entry/references'      : 'documents-entry-references',
  'documents.entry/detail'          : 'documents-entry-get-one',
  'documents.settings/detail'       : 'documents-settings-get',
};
/** @param {Object} dependencies Transporte acreditado. @returns {Object} Secciones registradas sin snapshots por socket. */
function documentsResourceWindows({ messaging }) {
  const topics = {};
  for (const request of contract.request.oneOf) {
    const topic = request.properties.topic.enum[0],
      section   = request.properties.section.enum[0];
    const action = actions[topic + '/' + section];
    if (!action) continue;
    const output = contract.successData.oneOf.find(
      /** @param {Object} schema Sobre. @returns {boolean} Misma sección. */
      (schema) =>
        schema.properties.control.properties.topic.enum[0] === topic &&
        schema.properties.control.properties.section.enum[0] === section,
    ).properties.data;
    topics[topic] ||= {};
    topics[topic][section] = {
      delegated   : topic === 'documents.repository' && section === 'collection',
      collections : ['page', 'collection', 'references'].includes(section)
        ? { '/listItem': 'id' }
        : {},
      /** @param {Object} query Consulta sin autoridad. @returns {Object} Consulta cerrada. */
      validate(query) {
        if (!validate(query, request.properties.query))
          throw new ApplicationError(400, 'INVALID_DOCUMENT_QUERY');
        return query;
      },
      /** @param {Object} query Consulta. @param {Object} identity Contexto vigente. @returns {Promise<Object>} Datos y cursor del propietario. */
      async read(query, identity) {
        const response = await messaging.request('service-documents', action, query, {
          timeoutMs : 15000,
          context   : identity.context,
        });
        if ([403, 404].includes(response?.statusCode))
          throw new ApplicationError(403, 'DOCUMENT_RESOURCE_UNAVAILABLE');
        if (response?.statusCode !== 200)
          throw new ApplicationError(
            response?.statusCode || 503,
            response?.descriptionCode || 'DOCUMENT_READ_UNAVAILABLE',
          );
        if (!validate(response.data, output))
          throw new ApplicationError(503, 'INVALID_DOCUMENT_PROJECTION');
        return readSnapshot(response);
      },
      /** @param {Object} query Consulta. @param {Object} change Hecho mínimo. @param {Object} identity Audiencia. @param {Object} base Ventana acreditada. @returns {boolean} Sólo ventanas potencialmente afectadas. */
      affects(query, change, identity, base) {
        if (change.control)
          return change.control.kind === 'user'
            ? change.control.code === identity.userCode
            : identity.context.groups?.includes(change.control.code) === true;
        if (topic === 'documents.settings') return change.parameter === 'documents-settings';
        if (change.resourceAccess)
          return (
            section === 'resource' ||
            base?.repository?.kind === 'resource' ||
            (section === 'references' &&
              (!base ||
                base.listItem?.some(
                  /** @param {Object} item Referencia. @returns {boolean} Depende de permisos del recurso. */
                  (item) => item.repository?.kind === 'resource',
                ) === true))
          );
        if (change.parameter) return false;
        if (change.operation === 'favorite' && change.userCode !== identity.userCode) return false;
        if (section === 'collection') {
          if (change.kind === 'resource') return false;
          return (
            base?.listItem?.some(
              /** @param {Object} item Raíz acreditada. @returns {boolean} Identidad del catálogo visible. */
              (item) => item.id === change.repositoryId || item.id === change.previousRepositoryId,
            ) === true ||
            change.workspace === identity.workspace ||
            (change.workspace === 'allWorkspaces' &&
              (!change.ownerCode || change.ownerCode === identity.userCode))
          );
        }
        if (section === 'references')
          return (
            query.entryIds.includes(change.entryId) ||
            (!base && Boolean(change.repositoryId)) ||
            base?.listItem?.some(
              /** @param {Object} item Referencia acreditada. @returns {boolean} Su raíz o permiso pudo cambiar. */
              (item) =>
                item.repositoryId === change.repositoryId ||
                item.repositoryId === change.previousRepositoryId,
            ) === true
          );
        const repositoryId = query.repositoryId || base?.repository?.id;
        return (
          (Boolean(query.entryId) && query.entryId === change.entryId) ||
          (Boolean(repositoryId) &&
            (repositoryId === change.repositoryId ||
              repositoryId === change.previousRepositoryId)) ||
          (!base && ['resource', 'detail'].includes(section) && Boolean(change.repositoryId))
        );
      },
    };
  }
  return topics;
}
module.exports = { documentsResourceWindows };
