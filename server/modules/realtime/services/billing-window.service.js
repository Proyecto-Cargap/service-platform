/** @file Ventanas de Billing sobre consultas canónicas autorizadas; sólo los cambios reales recalculan páginas. */
'use strict';
const { readSnapshot }     = require('../../../library/messaging/read-receipt');
const { ApplicationError } = require('../../../shared/application-error');
const { validate }         = require('../../../contracts/action-dto');
const contract             = require('../../../contracts/service.json').endpoints.find(
  /** @param {Object} endpoint Contrato registrado. @returns {boolean} Lectura estándar. */
  (endpoint) => endpoint.name === 'realtime-read',
);
/** @param {Object} dependencies Transporte del productor. @returns {Object} Registro cerrado de consultas públicas de Billing. */
function billingWindows({ messaging }) {
  const topics = {};
  for (const request of contract.request.oneOf) {
    const topic = request.properties.topic.enum[0];
    if (!topic.startsWith('billing.')) continue;
    const section  = request.properties.section.enum[0];
    const resource = topic.slice('billing.'.length);
    const output   = contract.successData.oneOf.find(
      /** @param {Object} schema Sobre público. @returns {boolean} Misma consulta registrada. */
      (schema) =>
        schema.properties.control.properties.topic.enum[0] === topic &&
        schema.properties.control.properties.section.enum[0] === section,
    ).properties.data;
    topics[topic] ||= {};
    topics[topic][section] = {
      collections : section === 'page' ? { '/items': 'code' } : {},
      /** @param {Object} query Filtros públicos. @returns {Object} Consulta cerrada sin autoridad aportada por el cliente. */
      validate(query) {
        if (!validate(query, request.properties.query))
          throw new ApplicationError(400, 'INVALID_BILLING_QUERY');
        return query;
      },
      /** @param {Object} query Filtros o identidad. @param {Object} identity Principal de transporte. @returns {Promise<Object>} Única representación pública del propietario. */
      async read(query, identity) {
        const response = await messaging.request(
          'service-billing',
          'billing-' + resource + (section === 'page' ? '-get-list' : '-get-one'),
          query,
          {
            timeoutMs : 15000,
            context   : identity.context,
          },
        );
        if (response?.statusCode !== 200)
          throw new ApplicationError(
            response?.statusCode || 503,
            response?.descriptionCode || 'BILLING_UNAVAILABLE',
          );
        if (!validate(response.data, output))
          throw new ApplicationError(503, 'INVALID_BILLING_PROJECTION');
        return readSnapshot(response);
      },
      /** @param {Object} query Ventana registrada. @param {Object} change Hecho durable. @param {Object} identity Contexto vigente. @returns {boolean} Recalcula sólo el recurso, registro y workspace potencialmente afectados. */
      affects(query, change, identity) {
        if (change.control)
          return change.control.kind === 'user'
            ? change.control.code === identity.userCode
            : identity.context.groups?.includes(change.control.code) === true;
        if (section === 'detail' && resource !== 'pac' && query.code !== change.item?.code)
          return false;
        if (
          resource === 'issuer' &&
          (section === 'detail' ||
            (identity.workspace === 'allWorkspaces' &&
              identity.context.policies?.includes('billing.issuer.view.global')))
        )
          return true;
        return [change.item?.workspace, change.previousWorkspace].some(
          /** @param {string} workspace Ámbito anterior o actual acreditado por el productor. @returns {boolean} Puede entrar o salir de la consulta. */
          (workspace) => workspace === 'allWorkspaces' || workspace === identity.workspace,
        );
      },
    };
  }
  return topics;
}
module.exports = { billingWindows };
