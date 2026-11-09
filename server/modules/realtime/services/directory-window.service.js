/** @file Ventanas de Directory: consultas canónicas, representación autorizada y referencias causales del productor. */
'use strict';
const { readSnapshot }     = require('../../../library/messaging/read-receipt');
const { ApplicationError } = require('../../../shared/application-error');
const { validate }         = require('../../../contracts/action-dto');
const contract             = require('../../../contracts/service.json').endpoints.find(
  /** @param {Object} endpoint Contrato. @returns {boolean} Lecturas por Gateway. */ (endpoint) =>
    endpoint.name === 'realtime-read',
);
/** @param {Object} dependencies Transporte acreditado. @returns {Object} Páginas, fichas y parámetros del registro cerrado. */
function directoryWindows({ messaging }) {
  const topics = {};
  for (const request of contract.request.oneOf) {
    const topic = request.properties.topic.enum[0];
    if (!topic.startsWith('directory.')) continue;
    const resource = topic.slice('directory.'.length),
      section      = request.properties.section.enum[0];
    const output = contract.successData.oneOf.find(
      /** @param {Object} schema Sobre. @returns {boolean} La misma sección pública. */
      (schema) =>
        schema.properties.control.properties.topic.enum[0] === topic &&
        schema.properties.control.properties.section.enum[0] === section,
    ).properties.data;
    const action =
      resource === 'settings'
        ? 'contact-visibility-state-get'
        : resource === 'visibility'
          ? 'contact-visibility-get'
          : resource + (['page', 'references'].includes(section) ? '-get-list' : '-get-one');
    topics[topic] ||= {};
    topics[topic][section] = {
      collections : ['page', 'references'].includes(section)
        ? { '/items': resource === 'fiscal-catalog' ? 'code' : resource + 'Code' }
        : {},
      /** @param {Object} query Consulta recibida. @returns {Object} Límites explícitos sin proyecciones o filtros de autoridad aportados por el cliente. */
      validate(query) {
        if (!validate(query, request.properties.query))
          throw new ApplicationError(400, 'INVALID_DIRECTORY_QUERY');
        return query;
      },
      /** @param {Object} query Consulta. @param {Object} identity Principal revalidado. @returns {Promise<Object>} Página o ficha del propietario con su recibo de consistencia. */
      async read(query, identity) {
        if (
          !['visibility', 'fiscal-catalog'].includes(resource) &&
          !identity.context.policies?.some(
            /** @param {string} policy Facultad vigente. @returns {boolean} Permite leer este recurso en algún ámbito; Directory determina cuáles filas. */
            (policy) => policy.startsWith(`directory.${resource}.view.`),
          )
        )
          throw new ApplicationError(403, 'FORBIDDEN');
        const ownerQuery =
          section === 'references'
            ? {
                page      : 1,
                perPage   : 100,
                sort      : resource + 'Code',
                order     : 'asc',
                condition : { [resource + 'Code']: { $in: query.codes } },
              }
            : query;
        const response   = await messaging.request('service-directory', action, ownerQuery, {
          context : identity.context,
        });
        if (section === 'detail' && response?.statusCode === 404)
          throw new ApplicationError(403, 'DIRECTORY_RESOURCE_UNAVAILABLE');
        if (response?.statusCode !== 200)
          throw new ApplicationError(
            response?.statusCode || 503,
            response?.descriptionCode || 'DIRECTORY_UNAVAILABLE',
          );
        if (!validate(response.data, output))
          throw new ApplicationError(503, 'INVALID_DIRECTORY_PROJECTION');
        return readSnapshot(response);
      },
      /** @param {Object} query Consulta. @param {Object} change Referencia mínima del productor. @param {Object} identity Principal. @param {Object} base Ventana autorizada cuando ya está estable. @returns {boolean} Considera reasignaciones y etiquetas derivadas, nunca datos enviados por el cliente como base. */
      affects(query, change, identity, base) {
        if (change.control)
          return change.control.kind === 'user'
            ? change.control.code === identity.userCode
            : identity.context.groups?.includes(change.control.code) === true;
        if (resource === 'settings' || resource === 'visibility')
          return change.resource === 'settings' && change.code === 'contact-visibility';
        if (resource === 'fiscal-catalog')
          return (
            change.resource === 'fiscal-catalog' &&
            (change.code === query.kind ||
              (change.code === 'taxpayer-type' && !!query.taxpayerType))
          );
        const reference =
          resource === 'contact' && ['role', 'department', 'job'].includes(change.resource);
        if (reference) {
          if (!base) return true;
          const rows = ['page', 'references'].includes(section) ? base.items : [base];
          return rows.some(
            /** @param {Object} item Fila previamente acreditada. @returns {boolean} Cambia una etiqueta de esta ventana, incluso si la referencia es histórica de otro workspace. */
            (item) => item[change.resource + 'Code'] === change.code,
          );
        }
        if (resource !== change.resource && !reference) return false;
        if (section === 'references' && !query.codes.includes(change.code)) return false;
        if (!reference && section === 'detail' && (query.contactCode || query.code) !== change.code)
          return false;
        return (
          query.scope === 'global' ||
          identity.context.policies?.includes(`directory.${resource}.view.global`) ||
          [change.workspace, change.previousWorkspace].some(
            /** @param {string|null} workspace Ámbito anterior o actual del cambio. @returns {boolean} Puede cambiar la pertenencia o etiqueta de esta ventana. */
            (workspace) => workspace === identity.workspace || workspace === 'allWorkspaces',
          )
        );
      },
    };
  }
  return topics;
}
module.exports = { directoryWindows };
