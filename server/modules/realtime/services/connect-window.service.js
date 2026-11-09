/** @file Ventanas Connect: consulta canónica, representación pública y autorización renovada. */
'use strict';
const { readSnapshot }     = require('../../../library/messaging/read-receipt');
const { ApplicationError } = require('../../../shared/application-error');
const { validate }         = require('../../../contracts/action-dto');
const contract             = require('../../../contracts/service.json').endpoints.find(
  /** @param {Object} endpoint Contrato registrado. @returns {boolean} Lectura de ventanas. */
  (endpoint) => endpoint.name === 'realtime-read',
);
/** @param {Object} dependencies Mensajería con contexto acreditado. @returns {Object} Secciones registradas de Connect. */
function connectWindows({ messaging }) {
  const topics = {};
  for (const request of contract.request.oneOf) {
    const topic = request.properties.topic.enum[0];
    if (
      ![
        'connect.mail-account',
        'connect.settings',
        'connect.meta-conversation',
        'connect.template',
        'connect.meta',
      ].includes(topic)
    )
      continue;
    const section = request.properties.section.enum[0];
    const output  = contract.successData.oneOf.find(
      /** @param {Object} schema Respuesta correlacionada. @returns {boolean} Misma sección de cuentas. */
      (schema) =>
        schema.properties.control.properties.topic.enum[0] === topic &&
        schema.properties.control.properties.section.enum[0] === section,
    ).properties.data;
    topics[topic] ||= {};
    topics[topic][section] = {
      collections : ['page', 'connections', 'conversations'].includes(section)
        ? {
            '/items' :
              topic === 'connect.template'
                ? 'templateCode'
                : topic === 'connect.meta'
                  ? section === 'connections'
                    ? 'connectionCode'
                    : 'conversationCode'
                  : 'id',
          }
        : {},
      /** @param {Object} query Selector sin autoridad. @returns {Object} Consulta cerrada validada. */
      validate(query) {
        if (!validate(query, request.properties.query))
          throw new ApplicationError(400, 'INVALID_CONNECT_QUERY');
        return query;
      },
      /** @param {Object} query Selector. @param {Object} identity Principal vigente. @returns {Promise<Object|null>} Snapshot público del productor; no consulta IMAP ni SMTP. */
      async read(query, identity) {
        if (topic === 'connect.meta-conversation') {
          if (!identity.context.policies?.includes('connect.meta.view.global'))
            throw new ApplicationError(403, 'FORBIDDEN');
          const chat = await messaging.request(
            'service-communication',
            'chat-get-one',
            { code: query.conversationCode },
            {
              timeoutMs : 15000,
              context   : identity.context,
            },
          );
          if (chat?.statusCode !== 200)
            throw new ApplicationError(chat?.statusCode || 503, 'CHAT_UNAVAILABLE');
        }
        const response = await messaging.request(
          'service-connect',
          topic === 'connect.meta'
            ? 'meta-window-read'
            : topic === 'connect.template'
              ? 'emailtemplate-window-read'
              : topic === 'connect.meta-conversation'
                ? 'meta-conversation-get'
                : topic === 'connect.settings'
                  ? 'settings-get-one'
                  : section === 'page'
                    ? 'mail-account-get-list'
                    : 'mail-account-get',
          ['connect.template', 'connect.meta'].includes(topic)
            ? {
                section,
                query,
              }
            : query,
          {
            timeoutMs : 15000,
            context   : identity.context,
          },
        );
        if (
          topic === 'connect.settings' &&
          response?.statusCode === 400 &&
          response.descriptionCode === 'N0002-C000-E0002:Settings does not exist'
        )
          return readSnapshot(response, null);
        if (response?.statusCode !== 200)
          throw new ApplicationError(
            response?.statusCode || 503,
            response?.descriptionCode || 'CONNECT_UNAVAILABLE',
          );
        if (
          topic === 'connect.meta-conversation' &&
          response.data &&
          (response.data.chatConversationCode !== query.conversationCode ||
            !Number.isSafeInteger(response.data.stateRevision))
        )
          throw new ApplicationError(503, 'INVALID_META_STATE');
        if (!validate(response.data, output))
          throw new ApplicationError(503, 'INVALID_CONNECT_PROJECTION');
        return readSnapshot(response);
      },
      /** @param {Object} query Selector. @param {Object} change Hecho durable. @param {Object} identity Principal vigente. @param {Object} base Última base acreditada del servidor. @returns {boolean} La página administrativa contiene todas las cuentas y SMTP global; las fichas son personales. */
      affects(query, change, identity, base) {
        if (change.control)
          return change.control.kind === 'user'
            ? change.control.code === identity.userCode
            : identity.context.groups?.includes(change.control.code) === true;
        if (topic === 'connect.meta') {
          if (section === 'settings') return change.resource === 'settings';
          if (section === 'connections')
            return ['settings', 'connection'].includes(change.resource);
          if (section === 'connection')
            return (
              change.resource === 'settings' ||
              (change.resource === 'connection' && change.code === query.connectionCode)
            );
          return (
            change.resource === 'conversation' &&
            (!query.connectionCode || change.connectionCode === query.connectionCode)
          );
        }
        if (topic === 'connect.template')
          return (
            change.workspace === 'allWorkspaces' &&
            (section === 'page' || !change.code || change.code === query.code)
          );
        if (topic === 'connect.meta-conversation' && change.source === 'communication')
          return (
            change.conversationCode === query.conversationCode &&
            change.users.includes(identity.userCode)
          );
        if (topic === 'connect.meta-conversation')
          return (
            change.chatConversationCode === query.conversationCode &&
            (!base || change.stateRevision > base.stateRevision)
          );
        if (topic === 'connect.settings')
          return change.parameter === query.parameter && change.workspace === 'allWorkspaces';
        if (change.parameter)
          return (
            section === 'page' &&
            change.parameter === 'mail-connection' &&
            change.workspace === 'allWorkspaces'
          );
        if (section === 'page') {
          if (change.listChange === 'none') return false;
          return (
            change.listChange === 'membership' ||
            !base ||
            base.items.some(
              /** @param {Object} item Fila de la ventana vigente. @returns {boolean} El orden por propietario es inmutable; una edición ajena no cambia esta página. */
              (item) => item.type === 'personal' && item.userCode === change.userCode,
            )
          );
        }
        return change.userCode === (query.userCode || identity.userCode);
      },
    };
  }
  return topics;
}
module.exports = { connectWindows };
