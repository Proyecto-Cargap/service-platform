/** @file Ventanas propias de Platform: consultas canónicas locales con contexto acreditado y cursor del productor. */
'use strict';
const { issueReadReceipt } = require('../../../library/messaging/read-receipt');
const { ApplicationError } = require('../../../shared/application-error');
const { validate }         = require('../../../contracts/action-dto');
const { configurationTag } = require('../../../shared/configuration-tag');
const routes               = Object.freeze({
  'platform.notification/personal-page'   : 'personalList',
  'platform.notification/personal-detail' : 'personalOne',
  'platform.notification/count'           : 'personalCount',
  'platform.notification/admin-page'      : 'adminList',
  'platform.notification/admin-detail'    : 'adminOne',
  'platform.broadcast/page'               : 'broadcasts',
  'platform.broadcast/detail'             : 'status',
  'platform.configuration/navigation'     : 'configuration',
  'platform.release/current'              : 'release.current',
  'platform.release/history'              : 'release.history',
  'platform.release/page'                 : 'release.getList',
  'platform.release/detail'               : 'release.getOne',
});
/** @param {Object} dependencies Mensajería y lector canónico propio, sin RPC al mismo servicio. @returns {Object} Secciones cerradas del centro de avisos. */
function platformWindows({ messaging, read }) {
  const contract = require('../../../contracts/service.json').endpoints.find(
    /** @param {Object} item Contrato público. @returns {boolean} Lectura correlacionada. */
    (item) => item.name === 'realtime-read',
  );
  const topics   = {};
  for (const request of contract.request.oneOf) {
    const topic     = request.properties.topic.enum[0];
    const section   = request.properties.section.enum[0];
    const operation = routes[topic + '/' + section];
    if (!operation) continue;
    const output = contract.successData.oneOf.find(
      /** @param {Object} schema Sobre público. @returns {boolean} Misma ventana. */
      (schema) =>
        schema.properties.control.properties.topic.enum[0] === topic &&
        schema.properties.control.properties.section.enum[0] === section,
    ).properties.data;
    topics[topic] ||= {};
    topics[topic][section] = {
      collections :
        section.endsWith('page') || section === 'history'
          ? {
              '/items' :
                topic === 'platform.broadcast'
                  ? 'broadcastCode'
                  : topic === 'platform.release'
                    ? 'releaseCode'
                    : 'notificationCode',
            }
          : {},
      /** @param {Object} query Consulta sin autoridad. @returns {Object} Selector cerrado. */
      validate(query) {
        if (!validate(query, request.properties.query))
          throw new ApplicationError(400, 'INVALID_PLATFORM_QUERY');
        return query;
      },
      /** @param {Object} query Selector público. @param {Object} identity Principal revalidado. @returns {Promise<Object>} Estado coherente y cursor emitido dentro del propietario. */
      async read(query, identity) {
        const response = await messaging.runWithContext(
          identity.context,
          /** @returns {Promise<Object>} Caso de uso canónico en transacción de lectura. */
          async () => {
            try {
              const response = await read(operation, query);
              if (operation === 'configuration' && response.statusCode === 200)
                return {
                  ...response,
                  data : {
                    configuration : response.data,
                    etag          : configurationTag(response.data),
                  },
                };
              return section.endsWith('detail') && response.statusCode === 204
                ? {
                    statusCode : 200,
                    data       : null,
                  }
                : response;
            } catch (error) {
              if (section.endsWith('detail') && error.statusCode === 404)
                return {
                  statusCode      : 200,
                  descriptionCode : 'OK',
                  data            : null,
                };
              throw error;
            }
          },
        );
        if (response.statusCode !== 200)
          throw new ApplicationError(
            response.statusCode || 503,
            response.descriptionCode || 'PLATFORM_UNAVAILABLE',
          );
        if (!validate(response.data, output))
          throw new ApplicationError(503, 'INVALID_PLATFORM_PROJECTION');
        return {
          data   : response.data,
          cursor : issueReadReceipt(response),
        };
      },
      /** @param {Object} query Consulta retenida. @param {Object} change Hecho acreditado. @param {Object} identity Identidad actual. @param {Object} base Última ventana autorizada. @returns {boolean} Sólo ventanas cuyo propietario, alcance o referencia puede cambiar. */
      affects(query, change, identity, base) {
        if (change.source === 'security' || change.source === 'directory') {
          if (!base || change.source === 'directory') return true;
          const items = base.items || [base];
          return items.some(
            /** @param {Object} item Fila autorizada. @returns {boolean} Autor o destinatario referenciado. */
            (item) => [item.userCode, item.userCreate].includes(change.code),
          );
        }
        const personal =
          section.startsWith('personal-') || section === 'count' || topic === 'platform.preference';
        if (personal && change.userCode !== identity.userCode) return false;
        if (
          query.workspace &&
          query.workspace !== 'allWorkspaces' &&
          change.workspace !== 'allWorkspaces' &&
          query.workspace !== change.workspace
        )
          return false;
        if (section.endsWith('detail')) return change.code === query.code || !change.code;
        return true;
      },
    };
  }
  return topics;
}
module.exports = {
  platformWindows,
  platformWindowRoutes : routes,
};
