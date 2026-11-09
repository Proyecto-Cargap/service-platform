/** @file Ventanas registradas de Intelligence; la autoridad y la representación pertenecen al productor. */
'use strict';
const { readSnapshot }     = require('../../../library/messaging/read-receipt');
const { ApplicationError } = require('../../../shared/application-error');
const { validate }         = require('../../../contracts/action-dto');
const contract             = require('../../../contracts/service.json').endpoints.find(
  /** @param {Object} endpoint Contrato. @returns {boolean} Lectura estándar. */
  (endpoint) => endpoint.name === 'realtime-read',
);
/** @param {Object} dependencies Mensajería acreditada. @returns {Object} Registro cerrado de ventanas. */
function intelligenceWindows({ messaging }) {
  const topics = {};
  for (const request of contract.request.oneOf) {
    const topic = request.properties.topic.enum[0];
    if (!topic.startsWith('intelligence.')) continue;
    const resource = topic.slice('intelligence.'.length);
    const key      = {
      agent            : 'agentCode',
      team             : 'teamCode',
      tool             : 'toolCode',
      'knowledge-base' : 'knowledgeBaseCode',
      trigger          : 'triggerCode',
      settings         : 'parameter',
      run              : 'runCode',
    }[resource];
    const section  = request.properties.section.enum[0];
    const output   = contract.successData.oneOf.find(
      /** @param {Object} schema Sobre. @returns {boolean} Misma ventana. */
      (schema) =>
        schema.properties.control.properties.topic.enum[0] === topic &&
        schema.properties.control.properties.section.enum[0] === section,
    ).properties.data;
    topics[topic] ||= {};
    topics[topic][section] = {
      collections :
        section === 'page' ? { '/items': key } : section === 'collection' ? { '': key } : {},
      /** @param {Object} query Consulta sin autoridad. @returns {Object} Consulta validada. */
      validate(query) {
        if (!validate(query, request.properties.query))
          throw new ApplicationError(400, 'INVALID_INTELLIGENCE_QUERY');
        return query;
      },
      /** @param {Object} query Consulta. @param {Object} identity Contexto acreditado. @returns {Promise<Object>} Snapshot canónico público. */
      async read(query, identity) {
        const response = await messaging.request(
          'service-intelligence',
          resource + (['page', 'collection'].includes(section) ? '-get-list' : '-get-one'),
          query,
          {
            timeoutMs : 15000,
            context   : identity.context,
          },
        );
        if ([403, 404].includes(response?.statusCode))
          throw new ApplicationError(403, 'INTELLIGENCE_RESOURCE_UNAVAILABLE');
        if (response?.statusCode !== 200)
          throw new ApplicationError(
            response?.statusCode || 503,
            response?.descriptionCode || 'INTELLIGENCE_UNAVAILABLE',
          );
        if (!validate(response.data, output))
          throw new ApplicationError(503, 'INVALID_INTELLIGENCE_PROJECTION');
        return readSnapshot(response);
      },
      /** @param {Object} query Consulta. @param {Object} change Hecho durable. @param {Object} identity Contexto. @returns {boolean} Únicamente una ventana potencialmente afectada. */
      affects(query, change, identity, base) {
        if (change.control)
          return change.control.kind === 'user'
            ? change.control.code === identity.userCode
            : identity.context.groups?.includes(change.control.code) === true;
        if (resource !== 'run' && change.resource !== resource) return false;
        const code = resource === 'run' ? change.runCode : change.code;
        if (section === 'detail' && query[key] !== code) return false;
        if (query.codes && !query.codes.includes(code)) return false;
        return (
          query.scope === 'global' ||
          identity.context.policies?.includes('intelligence.' + resource + '.view.global') ||
          change.workspace === identity.workspace ||
          change.workspace === 'allWorkspaces' ||
          base?.items?.some(
            /** @param {Object} item Fila autorizada previamente. @returns {boolean} Retirada o cambio de pertenencia de un registro visible. */
            (item) => item[key] === code,
          ) === true
        );
      },
    };
  }
  return topics;
}
module.exports = { intelligenceWindows };
