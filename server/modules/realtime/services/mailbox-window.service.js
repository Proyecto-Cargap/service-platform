/** @file Ventanas del buzón sobre persistencia de Connect; ninguna actualización consulta IMAP. */
'use strict';
const { readSnapshot }     = require('../../../library/messaging/read-receipt');
const { ApplicationError } = require('../../../shared/application-error');
const { validate }         = require('../../../contracts/action-dto');
const contract             = require('../../../contracts/service.json').endpoints.find(
  /** @param {Object} endpoint Contrato. @returns {boolean} Lectura estándar. */
  (endpoint) => endpoint.name === 'realtime-read',
);
const actions              = {
  status     : 'mailbox-status-get',
  search     : 'mailbox-search-execute',
  messages   : 'mailbox-messages-list',
  message    : 'mailbox-message-get',
  operations : 'mailbox-operations-list',
  operation  : 'mailbox-operation-get',
};
/** @param {Object} dependencies Transporte acreditado. @returns {Object} Secciones con consulta y ámbito propios. */
function mailboxWindows({ messaging }) {
  /** @param {string} action Acción del propietario. @param {Object} query Selector. @param {Object} identity Principal. @returns {Promise<Object>} Conserva fallos y distingue vencimiento de reserva para recuperación puntual. */
  async function invoke(action, query, identity) {
    const response = await messaging.request('service-connect', action, query, {
      context   : identity.context,
      timeoutMs : action === 'mailbox-search-execute' ? 125000 : 15000,
    });
    if (response?.statusCode !== 200) {
      const error = new ApplicationError(
        response?.statusCode || 503,
        response?.descriptionCode || 'MAILBOX_UNAVAILABLE',
      );
      if (action === 'mailbox-search-get' && response?.descriptionCode === 'MAILBOX_SEARCH_EXPIRED')
        error.resyncRequired = true;
      throw error;
    }
    return response;
  }
  const sections = {};
  for (const request of contract.request.oneOf) {
    if (request.properties.topic.enum[0] !== 'connect.mailbox') continue;
    const section = request.properties.section.enum[0];
    const output  = contract.successData.oneOf.find(
      /** @param {Object} schema Respuesta. @returns {boolean} Misma sección del buzón. */
      (schema) =>
        schema.properties.control.properties.topic.enum[0] === 'connect.mailbox' &&
        schema.properties.control.properties.section.enum[0] === section,
    ).properties.data;
    sections[section] = {
      readTimeoutMs : section === 'search' ? 125000 : 15000,
      collections   :
        section === 'status'
          ? { '/folders': 'path' }
          : ['messages', 'operations', 'search'].includes(section)
            ? { '/items': 'id' }
            : {},
      /** @param {Object} query Filtros públicos. @returns {Object} Rechaza identidad y toda búsqueda remota en esta ventana de persistencia. */
      validate(query) {
        if (!validate(query, request.properties.query))
          throw new ApplicationError(400, 'INVALID_MAILBOX_QUERY');
        return query;
      },
      /** @param {Object} query Consulta. @param {Object} identity Principal vigente. @returns {Promise<Object>} Proyección canónica autorizada sin conexiones a proveedores. */
      async read(query, identity, control) {
        const response = await invoke(
          actions[section],
          section === 'search' ? { watchId: control.receiptId } : query,
          identity,
        );
        if (!validate(response.data, output))
          throw new ApplicationError(503, 'INVALID_MAILBOX_PROJECTION');
        return readSnapshot(response);
      },
      /** @param {Object} query Consulta original. @param {Object} identity Principal. @param {Object} anchor Base HTTP. @param {Object[]} changes Hechos. @param {Object} control Reserva privada. @returns {Promise<Object>} Una búsqueda se reconsulta exclusivamente en su materialización local. */
      async readWindow(query, identity, anchor, changes, control) {
        if (section !== 'search') return this.read(query, identity, control);
        const response = await invoke(
          'mailbox-search-get',
          { watchId: control.receiptId },
          identity,
        );
        if (!validate(response.data, output))
          throw new ApplicationError(503, 'INVALID_MAILBOX_PROJECTION');
        return readSnapshot(response);
      },
      ...(section === 'search'
        ? {
            /** @param {Object} query Consulta. @param {Object} identity Principal. @param {string} receiptId Reserva estándar. @returns {Promise<void>} Registra intención, nunca la lista. */
            async open(query, identity, receiptId) {
              const value = await invoke(
                'mailbox-search-open',
                {
                  watchId : receiptId,
                  query,
                },
                identity,
              );
              if (value?.data?.registered !== true)
                throw new ApplicationError(503, 'INVALID_MAILBOX_CONTROL');
            },
            /** @param {string} receiptId Reserva. @param {Object} identity Principal. @returns {Promise<void>} Renovación técnica por heartbeat. */
            async renew(receiptId, identity) {
              const value = await invoke('mailbox-search-renew', { watchId: receiptId }, identity);
              if (value?.data?.renewed !== true)
                throw new ApplicationError(503, 'INVALID_MAILBOX_CONTROL');
            },
            /** @param {string} receiptId Reserva. @param {Object} identity Principal. @returns {Promise<void>} Retira sólo la consulta propia. */
            async close(receiptId, identity) {
              const value = await invoke('mailbox-search-close', { watchId: receiptId }, identity);
              if (value?.data?.released !== true)
                throw new ApplicationError(503, 'INVALID_MAILBOX_CONTROL');
            },
          }
        : {}),
      /** @param {Object} query Filtros. @param {Object} change Hecho confirmado. @param {Object} identity Principal. @param {Object} base Última ventana acreditada. @returns {boolean} Aísla propietario, carpeta y registros visibles sin filtrar contra una lectura en vuelo. */
      affects(query, change, identity, base, control) {
        if (change.control)
          return change.control.kind === 'user'
            ? change.control.code === identity.userCode
            : identity.context.groups?.includes(change.control.code) === true;
        if (change.userCode !== identity.userCode) return false;
        if (change.listChange) return true;
        if (section === 'status') return false;
        if (section === 'message')
          return (
            change.section === 'messages' &&
            (change.id ? change.id === query.id : !base || change.folder === base.folder)
          );
        if (section === 'search')
          return change.section === 'search' && change.id === control?.receiptId;
        if (section === 'operation')
          return change.section === 'operations' && change.id === query.id;
        if (section !== change.section) return false;
        if (section === 'messages' && change.folder !== query.folder) return false;
        return (
          change.kind === 'membership' ||
          !base ||
          base.items.some(
            /** @param {Object} item Fila. @returns {boolean} El cambio sólo modifica un ítem de la ventana. */
            (item) => item.id === change.id,
          )
        );
      },
    };
  }
  return { 'connect.mailbox': sections };
}
module.exports = { mailboxWindows };
