/** @file Documents comparte el canal estándar de Platform; las páginas y manifiestos se obtienen por Gateway. */
'use strict';
const { registerRealtimeTopic } = require('../../library/realtime/protocol');
const { ApplicationError }      = require('../../shared/application-error');
/** @param {Object} dependencies Transporte canónico y ventanas del proceso. @returns {Object} Ciclo de vida documental sin snapshots por socket. */
function createDocumentsRealtime({
  messaging,
  windows = require('../../modules/realtime/compositions/realtime.composition').realtimeWindows,
  reads = require('../../modules/realtime/compositions/realtime.composition').repositoryReads,
}) {
  const repositories  = require('./documents-repositories-realtime').repositoryWatches({
    messaging,
    reads,
  });
  const sockets       = new Set();
  const authorization = require('./connection-authorization').connectionAuthorization({
    matches : require('../../modules/realtime/services/access-reference.service').matchesAccess,
  });
  let available       = true;
  return {
    /** @returns {number} Cerca de eventos capturada antes del intercambio de credenciales. */
    beginAuthentication() {
      return authorization.begin();
    },
    /** @param {Object} socket Conexión compartida. @returns {void} Registra controles de repositorios delegados. */
    bind(socket) {
      registerRealtimeTopic(
        socket,
        'documents.entries',
        /** @param {Object} value Control estándar. @returns {Promise<Object>} Confirma sin entregar un manifiesto. */
        async (value) => {
          if (
            value.section !== 'repository' ||
            !['subscribe', 'unsubscribe', 'bind', 'resync'].includes(value.type)
          )
            throw new ApplicationError(400, 'INVALID_DOCUMENT_SUBSCRIPTION');
          if (value.type === 'unsubscribe') {
            repositories.unwatch(socket.id, value);
            return { statusCode: 200 };
          }
          if (value.type === 'bind') return repositories.bind(socket, value);
          if (
            !value.query ||
            Object.keys(value.query).some(
              /** @param {string} key Campo recibido. @returns {boolean} No pertenece a la consulta declarada. */
              (key) => key !== 'repositoryId',
            )
          )
            throw new ApplicationError(400, 'INVALID_DOCUMENT_SUBSCRIPTION');
          return repositories.watch(socket, {
            subscriptionId : value.subscriptionId,
            watchVersion   : value.watchVersion,
            repositoryId   : value.query.repositoryId,
            type           : value.type,
          });
        },
      );
    },
    /** @param {Object} socket Conexión delegada. @param {Function} verify Autoridad vigente. @param {Object} options Cerca del handshake. @returns {void} Conserva la renovación de concesiones. */
    authenticateRepositories(socket, verify, options = {}) {
      this.authenticate(socket, verify, {
        ...options,
        preserve : true,
      });
    },
    /** @param {string} socketId Conexión delegada. @returns {void} Solicita la renovación puntual. */
    requireAuthenticationRepositories(socketId) {
      repositories.requireAuthentication(socketId);
      const session = windows().sessions.get(socketId);
      if (session)
        for (const entry of session.windows.values()) {
          entry.window.send('resync-required', { reason: 'AUTH_REQUIRED' });
          entry.window.close();
        }
    },
    /** @param {Object} socket Conexión web o móvil. @param {Function} verify Sesión acreditada por Security. @param {Object} options Conserva únicamente una renovación de identidad delegada verificada. @returns {void} Comparte las ventanas estándar del proceso. */
    authenticate(socket, verify, options = {}) {
      const identityForConnection = authorization.register(socket, verify, options.epoch);
      repositories.authenticate(socket, identityForConnection);
      windows().authenticate(
        socket,
        /** @returns {Promise<Object>} Contexto revalidado antes de consultar o entregar. */
        async () => {
          const identity = await identityForConnection();
          if (!available) throw new ApplicationError(503, 'DOCUMENT_REALTIME_UNAVAILABLE');
          return {
            userCode    : identity.userCode,
            actorType   : identity.actorType,
            delegation  : identity.delegation,
            sessionCode : identity.sessionCode,
            workspace   : identity.activeWorkspace,
            context     : {
              user        : { userCode: identity.userCode },
              sessionCode : identity.sessionCode,
              workspace   : identity.activeWorkspace,
              policies    : identity.policies,
              workspaces  : identity.workspaces,
              groups      : identity.groups,
              ...(identity.delegation ? { delegation: identity.delegation } : {}),
            },
          };
        },
        options,
      );
      sockets.add(socket.id);
    },
    /** @param {string} socketId Conexión sustituida. @returns {void} Descarta únicamente sus suscripciones. */
    reset(socketId) {
      authorization.reset(socketId);
      repositories.reset(socketId);
      if (sockets.delete(socketId)) windows().reset(socketId);
    },
    /** @param {Object} data Cambio de acceso de un recurso instalado. @param {string} originService Productor. @returns {Promise<void>} Reacredita los recursos dependientes. */
    async resourceChanged(data, originService) {
      if (!['service-directory', 'service-customer', 'service-supplier'].includes(originService))
        throw new ApplicationError(403, 'FORBIDDEN');
      if (sockets.size)
        for (const topic of ['documents.view', 'documents.repository', 'documents.entry'])
          await windows().changed(topic, { resourceAccess: true });
    },
    /** @param {Object} data Raíz modificada o retirada. @param {string} originService Productor. @returns {Promise<void>} Actualiza capacidades del manifiesto sin releerlo desde desktop. */
    async repositoryChanged(data, originService) {
      if (originService !== 'service-documents') throw new ApplicationError(403, 'FORBIDDEN');
      await repositories.control(data.repositoryId);
    },
    /** @param {Object} data Referencia documental durable. @param {string} originService Productor acreditado. @returns {Promise<void>} Emite diferencias de ventanas afectadas. */
    async changed(data, originService) {
      if (originService !== 'service-documents') throw new ApplicationError(403, 'FORBIDDEN');
      if (
        !data ||
        !/^[a-fA-F0-9]{24}$/.test(data.entryId) ||
        typeof data.changeCode !== 'string' ||
        !data.changeCode
      )
        throw new ApplicationError(400, 'INVALID_DOCUMENT_EVENT');
      await repositories.changed(data);
      if (sockets.size)
        for (const topic of ['documents.view', 'documents.repository', 'documents.entry'])
          await windows().changed(topic, data);
    },
    /** @param {Object} change Referencia acreditada de Security. @returns {Promise<void>} Revalida las ventanas afectadas, incluidas las concesiones delegadas. */
    async accessChanged(change) {
      await authorization.changed(change);
      await repositories.accessChanged(change);
      if (sockets.size)
        await windows().reauthorize(
          change,
          require('../../modules/realtime/services/access-reference.service').matchesAccess,
        );
    },
    /** @returns {void} Invalida la continuidad sin consultar listas durante una caída. */
    unavailable() {
      authorization.unavailable();
      repositories.unavailable();
      available = false;
      if (sockets.size) windows().unavailable();
    },
    /** @returns {Promise<void>} Recuperación puntual por Gateway cuando vuelve el broker. */
    async recover() {
      await authorization.recover();
      available = true;
      await repositories.recover();
      if (sockets.size) windows().recover();
    },
    /** @returns {void} Libera las ventanas y suscripciones documentales. */
    close() {
      authorization.close();
      repositories.close();
      for (const id of sockets) windows().reset(id);
      sockets.clear();
    },
  };
}
module.exports = { createDocumentsRealtime };
