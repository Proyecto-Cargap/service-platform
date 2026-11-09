/** @file Ventanas de Security con permisos vigentes, DTO canónicos y recibos emitidos por su productor. */
'use strict';
const { readSnapshot }     = require('../../../library/messaging/read-receipt');
const { ApplicationError } = require('../../../shared/application-error');
const { validate }         = require('../../../contracts/action-dto');
const contract             = require('../../../contracts/service.json').endpoints.find(
  /** @param {Object} endpoint Capacidad. @returns {boolean} Frontera HTTP estándar. */
  (endpoint) => endpoint.name === 'realtime-read',
);
/** @param {Object} dependencies Mensajería acreditada. @returns {Object} Registro cerrado por recurso y sección. */
function securityWindows({ messaging }) {
  const topics = {};
  for (const request of contract.request.oneOf) {
    const topic = request.properties.topic.enum[0];
    if (!topic.startsWith('security.')) continue;
    const resource = topic.slice('security.'.length);
    const section  = request.properties.section.enum[0];
    const output   = contract.successData.oneOf.find(
      /** @param {Object} schema Resultado. @returns {boolean} Sección correspondiente a esta consulta. */
      (schema) =>
        schema.properties.control.properties.topic.enum[0] === topic &&
        schema.properties.control.properties.section.enum[0] === section,
    ).properties.data;
    const owner    = resource === 'workspace' ? 'workspaces' : resource;
    const action   =
      resource === 'directory'
        ? 'user-directory-get-list'
        : resource === 'connection'
          ? 'oauth-grant-get-list'
          : section === 'services'
            ? 'policy-get-index'
            : resource === 'access'
              ? 'session-own-access-get'
              : resource === 'profile'
                ? 'session-own-profile-get'
                : owner + (section === 'detail' ? '-get-one' : '-get-list');
    topics[topic] ||= {};
    topics[topic][section] = {
      collections : ['page', 'references'].includes(section)
        ? {
            '/items' :
              resource === 'directory'
                ? 'userCode'
                : resource === 'connection'
                  ? 'code'
                  : (resource === 'workspace' ? 'workspace' : resource) + 'Code',
          }
        : section === 'collection'
          ? { '': 'parameter' }
          : {},
      /** @param {Object} query Entrada pública. @returns {Object} Consulta sin campos privados ni selectores de autoridad. */
      validate(query) {
        if (!validate(query, request.properties.query))
          throw new ApplicationError(400, 'INVALID_SECURITY_QUERY');
        return query;
      },
      /** @param {Object} query Consulta. @param {Object} identity Principal revalidado. @returns {Promise<Object>} Resultado del propietario con cursor ligado a esos datos. */
      async read(query, identity) {
        if (
          !['profile', 'access', 'connection', 'directory'].includes(resource) &&
          !identity.context.policies?.some(
            /** @param {string} policy Permiso. @returns {boolean} Security decidirá después la pertenencia y alcance de cada fila. */
            (policy) => policy.startsWith(`security.${owner}.view.`),
          )
        )
          throw new ApplicationError(403, 'FORBIDDEN');
        const ownerQuery =
          resource === 'directory'
            ? query
            : section === 'references'
              ? {
                  page      : 1,
                  perPage   : 100,
                  sort      : resource + 'Code',
                  order     : 'asc',
                  condition : { [resource + 'Code']: { $in: query.codes } },
                }
              : query;
        const response   = await messaging.request('service-security', action, ownerQuery, {
          context : identity.context,
        });
        if (
          section === 'detail' &&
          ([204, 404].includes(response?.statusCode) ||
            (resource === 'settings' &&
              response?.statusCode === 400 &&
              response?.descriptionCode === 'N0001-C000-E0002:Settings does not exist') ||
            (response?.statusCode === 200 && response.data === null))
        )
          throw new ApplicationError(403, 'SECURITY_RESOURCE_UNAVAILABLE');
        if (response?.statusCode !== 200)
          throw new ApplicationError(
            response?.statusCode || 503,
            response?.descriptionCode || 'SECURITY_UNAVAILABLE',
          );
        if (!validate(response.data, output))
          throw new ApplicationError(503, 'INVALID_SECURITY_PROJECTION');
        return readSnapshot(response);
      },
      /** @param {Object} query Consulta acreditada. @param {Object} change Referencia durable. @param {Object} identity Principal. @param {Object} base Ventana autorizada, cuando ya está estable. @returns {boolean} Selecciona sólo dependencias de esta proyección; nunca acepta una base aportada por el cliente. */
      affects(query, change, identity, base) {
        if (resource === 'directory')
          return change.source === 'directory'
            ? change.resource === 'contact'
            : change.resource === 'user' &&
                (section !== 'references' || query.codes.includes(change.code));
        if (resource === 'connection')
          return (
            change.source !== 'directory' &&
            change.resource === 'grant' &&
            change.userCode === identity.userCode
          );
        if (resource === 'access')
          return (
            change.source !== 'directory' &&
            require('./access-reference.service').matchesAccess(identity, change)
          );
        if (change.source === 'directory') {
          if (resource === 'profile')
            return !base || change.resource !== 'contact' || base.contactCode === change.code;
          if (resource === 'user') {
            if (!['role', 'contact'].includes(change.resource)) return false;
            // Una búsqueda puede incorporar filas que antes no pertenecían a la página.
            if (query.search || !base) return true;
            const rows = ['page', 'references'].includes(section) ? base.items : [base];
            return rows.some(
              /** @param {Object} row Fila visible. @returns {boolean} Etiqueta o contacto usado por la proyección. */
              (row) => row[change.resource + 'Code'] === change.code,
            );
          }
          return resource === 'session' && (section === 'detail' || change.resource === 'role');
        }
        if (resource === 'profile')
          return change.resource === 'user'
            ? change.code === identity.userCode
            : change.resource === 'session' && change.code === identity.sessionCode;
        if (resource === 'session' && ['user', 'workspace'].includes(change.resource)) {
          if (query.search || !base) return true;
          const rows = ['page', 'references'].includes(section) ? base.items : [base];
          return rows.some(
            /** @param {Object} row Sesión autorizada. @returns {boolean} Cambia una referencia de esta ventana. */
            (row) =>
              change.resource === 'user'
                ? row.userCode === change.code
                : section === 'detail' && row.activeWorkspace === change.code,
          );
        }
        return (
          resource === change.resource &&
          (section !== 'references' || query.codes.includes(change.code)) &&
          (section !== 'detail' || (query.code || query.parameter) === change.code)
        );
      },
    };
  }
  return topics;
}
module.exports = { securityWindows };
