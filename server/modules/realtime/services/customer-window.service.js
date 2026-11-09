/** @file Ventanas CRM autorizadas: páginas, fichas, referencias y una columna por consulta de tablero. */
'use strict';
const { readSnapshot }     = require('../../../library/messaging/read-receipt');
const { ApplicationError } = require('../../../shared/application-error');
const { validate }         = require('../../../contracts/action-dto');
const contract             = require('../../../contracts/service.json').endpoints.find(
  /** @param {Object} endpoint Capacidad pública. @returns {boolean} Frontera HTTP. */
  (endpoint) => endpoint.name === 'realtime-read',
);
const keys                 = {
  customer        : 'customerCode',
  'pipeline-type' : 'pipelineTypeCode',
  pipeline        : 'pipelineCode',
  opportunity     : 'opportunityCode',
  activity        : 'activityCode',
  settings        : 'settingsCode',
};
/** @param {Object} query Consulta acreditada. @param {Object} change Causa mínima. @param {string} field Relación o contexto. @returns {boolean} Ambos extremos de un traslado pueden cambiar la ventana. */
function matches(query, change, field) {
  return (
    !query[field] ||
    !change[field] ||
    query[field] === change[field] ||
    query[field] === change['previous' + field[0].toUpperCase() + field.slice(1)]
  );
}
/** @param {Object} dependencies Transporte acreditado al propietario. @returns {Object} Esquemas y proyecciones públicas sin listas en ACK. */
function customerWindows({ messaging }) {
  const topics = {};
  for (const request of contract.request.oneOf) {
    const topic = request.properties.topic.enum[0];
    if (!topic.startsWith('customer.')) continue;
    const resource = topic.slice('customer.'.length),
      section      = request.properties.section.enum[0];
    const action =
      resource === 'settings'
        ? 'settings-get'
        : resource +
          '-get-' +
          ({
            page   : 'list',
            detail : 'one',
          }[section] || section);
    const output = contract.successData.oneOf.find(
      /** @param {Object} schema Salida cerrada. @returns {boolean} Misma sección. */
      (schema) =>
        schema.properties.control.properties.topic.enum[0] === topic &&
        schema.properties.control.properties.section.enum[0] === section,
    ).properties.data;
    topics[topic] ||= {};
    topics[topic][section] = {
      collections :
        section === 'board'
          ? {
              '/windows'         : 'stageCode',
              '/windows/0/items' : 'opportunityCode',
              '/stages'          : 'stageCode',
              '/assignees'       : 'userCode',
              '/customers'       : 'customerCode',
            }
          : section === 'summary'
            ? { '/stages': 'stageCode' }
            : ['page', 'assignees', 'customers'].includes(section)
              ? {
                  '/items' :
                    section === 'assignees'
                      ? 'userCode'
                      : section === 'customers'
                        ? 'customerCode'
                        : keys[resource],
                }
              : {},
      /** @param {Object} query Consulta. @returns {Object} Filtros cerrados; cada columna conserva su ventana independiente. */
      validate(query) {
        if (!validate(query, request.properties.query))
          throw new ApplicationError(400, 'INVALID_CUSTOMER_QUERY');
        return query;
      },
      /** @param {Object} query Consulta. @param {Object} identity Principal vigente. @returns {Promise<Object>} Datos y cursor emitidos por el propietario tras autorizar padres y relaciones. */
      async read(query, identity) {
        const response = await messaging.request('service-customer', action, query, {
          context : identity.context,
        });
        if (
          response?.statusCode === 404 ||
          response?.descriptionCode === 'CUSTOMER_PIPELINE_TYPE_CHANGED'
        )
          throw new ApplicationError(403, 'CUSTOMER_RESOURCE_UNAVAILABLE');
        if (response?.statusCode !== 200)
          throw new ApplicationError(
            response?.statusCode || 503,
            response?.descriptionCode || 'CUSTOMER_UNAVAILABLE',
          );
        if (!validate(response.data, output))
          throw new ApplicationError(503, 'INVALID_CUSTOMER_PROJECTION');
        const data =
          section === 'board' && query.windows.length
            ? {
                ...response.data,
                totals : response.data.totals.filter(
                  /** @param {Object} total Agregado autorizado por moneda. @returns {boolean} La columna sólo entrega sus agregados; otras monedas conservan identidad y orden. */
                  (total) => total.stageCode === query.windows[0].stageCode,
                ),
              }
            : response.data;
        return readSnapshot(response, data);
      },
      /** @param {Object} query Consulta. @param {Object} change Causa atómica. @param {Object} identity Principal. @param {Object} base Ventana autorizada anterior. @returns {boolean} Sólo cambios propios o de sus dependencias pueden alterar su contenido o acceso. */
      affects(query, change, identity, base) {
        if (change.source === 'directory') {
          if (change.resource !== 'contact') return false;
          if (resource === 'customer')
            return (
              (section === 'page' && query.sort === 'name') ||
              !base ||
              (section === 'detail'
                ? base.contactCode === change.code
                : base.items.some(
                    /** @param {Object} row Cliente visible. @returns {boolean} Referencia cuya proyección cambia. */
                    (row) => row.contactCode === change.code,
                  ))
            );
          return resource === 'pipeline' && ['board', 'customers', 'assignees'].includes(section);
        }
        if (change.source === 'security')
          return (
            resource === 'pipeline' &&
            ['board', 'assignees'].includes(section) &&
            ['user', 'workspace'].includes(change.resource) &&
            (change.resource !== 'user' ||
              (section === 'board'
                ? !base ||
                  base.windows.some(
                    /** @param {Object} window Columna autorizada. @returns {boolean} El perfil pertenece a una tarjeta entregada. */
                    (window) =>
                      window.items.some(
                        /** @param {Object} row Tarjeta. @returns {boolean} Responsable afectado. */
                        (row) => row.userAssigned === change.code,
                      ),
                  )
                : query.codes.includes(change.code)))
          );
        if (!matches(query, change, 'workspace')) return false;
        if (resource === 'settings')
          return (
            change.resource === 'settings' &&
            change.workspace === (query.workspace || identity.workspace)
          );
        if (resource === change.resource && section === 'detail')
          return query[keys[resource]] === change.code;
        if (resource === 'customer') return change.resource === 'customer';
        if (resource === 'pipeline-type') return change.resource === 'pipeline-type';
        if (resource === 'pipeline' && ['page', 'detail'].includes(section))
          return change.resource === 'pipeline';
        if (resource === 'pipeline') {
          if (change.resource === 'pipeline') return change.code === query.pipelineCode;
          if (change.resource === 'pipeline-type')
            return !base?.pipelineTypeCode || base.pipelineTypeCode === change.code;
          if (change.resource === 'customer')
            return !query.codes || section !== 'customers' || query.codes.includes(change.code);
          if (change.resource !== 'opportunity' || !matches(query, change, 'pipelineCode'))
            return false;
          return (
            section !== 'board' ||
            !query.windows.length ||
            matches({ stageCode: query.windows[0].stageCode }, change, 'stageCode')
          );
        }
        if (change.resource === 'customer')
          return (
            !base ||
            (section === 'detail'
              ? base.customerCode === change.code
              : !query.customerCode || query.customerCode === change.code)
          );
        if (resource === 'opportunity' && change.resource === 'pipeline')
          return (
            (!query.pipelineCode || query.pipelineCode === change.code) &&
            (section !== 'detail' || !base || base.pipelineCode === change.code)
          );
        if (resource === 'activity' && ['pipeline', 'opportunity'].includes(change.resource))
          return (
            section !== 'detail' ||
            !base ||
            change.resource !== 'opportunity' ||
            base.opportunityCode === change.code
          );
        if (resource !== change.resource) return false;
        return ['customerCode', 'pipelineCode', 'opportunityCode'].every(
          /** @param {string} field Relación del recurso. @returns {boolean} Conserva ambos lados de un cambio de pertenencia. */
          (field) => matches(query, change, field),
        );
      },
    };
  }
  return topics;
}
module.exports = { customerWindows };
