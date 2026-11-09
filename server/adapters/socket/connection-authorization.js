/** @file Identidad por conexión, invalidada por eventos de acceso y discontinuidad; sin RPC por ventana. */
'use strict';
const { ApplicationError } = require('../../shared/application-error');

/** @param {Object} identity Principal de Security. @returns {string} Frontera de sesión y permisos; permite descartar lecturas iniciadas antes de una revocación. */
function principalKey(identity) {
  return JSON.stringify([
    identity.userCode,
    identity.sessionCode,
    identity.activeWorkspace,
    identity.actorType,
    identity.delegation?.grantCode,
    identity.delegation?.clientId,
    [...(identity.groups || [])].sort(),
    [...(identity.policies || [])].sort(),
    [...(identity.workspaces || [])].sort(),
  ]);
}

/** @param {Object} dependencies Selector de conexiones afectadas. @returns {Object} Acreditación local y recuperación causal de las conexiones. */
function connectionAuthorization({ matches }) {
  const sessions = new Map();
  let available  = true;
  let epoch      = 0;
  let continuity = 0;

  /** @param {Object} entry Asociación vigente. @returns {Promise<void>} Serializa los cambios reales; un fallo nunca habilita el contexto anterior. */
  function refresh(entry) {
    const previous = entry.pending;
    const pending  = (
      /** @returns {Promise<void>} Actualiza únicamente la asociación todavía vigente. */ async () => {
        await previous?.catch(
          /** @returns {void} Un nuevo evento permite recuperar un fallo anterior. */
          () => {},
        );
        if (sessions.get(entry.socket.id) !== entry) return;
        try {
          const identity = await entry.refresh();
          if (sessions.get(entry.socket.id) !== entry) return;
          if (
            principalKey(identity) !== entry.key ||
            !Number.isFinite(identity.expiration) ||
            identity.expiration * 1000 <= Date.now()
          )
            throw new ApplicationError(401, 'REALTIME_CONTEXT_CHANGED');
          entry.identity = identity;
          entry.socket.data.identity = identity;
          entry.error = null;
        } catch (error) {
          entry.error = error;
          if (![401, 403].includes(error.statusCode)) throw error;
        }
      }
    )().finally(
      /** @returns {void} Libera sólo la comprobación terminada. */
      () => {
        if (entry.pending === pending) entry.pending = null;
      },
    );
    entry.pending = pending;
    return pending;
  }

  return {
    /** @returns {number} Cerca de cambios mientras se autentica una nueva conexión. */
    begin() {
      if (!available) throw new ApplicationError(503, 'REALTIME_UNAVAILABLE');
      return epoch;
    },
    /** @param {Object} socket Transporte. @param {Function} verify Consulta remota exclusiva de eventos. @param {number} expectedEpoch Cerca capturada antes de autenticar. @returns {Function} Verificador local compartido por todas las ventanas. */
    register(socket, verify, expectedEpoch = epoch) {
      if (!available || expectedEpoch !== epoch)
        throw new ApplicationError(503, 'REALTIME_ACCESS_CHANGED');
      const entry = {
        socket,
        identity : socket.data.identity,
        key      : principalKey(socket.data.identity),
        refresh  : verify,
        pending  : null,
        error    : null,
      };
      sessions.set(socket.id, entry);
      /** @returns {Promise<Object>} Espera invalidaciones pendientes sin autenticar de nuevo. */
      return async () => {
        while (entry.pending) await entry.pending;
        if (!available) throw new ApplicationError(503, 'REALTIME_UNAVAILABLE');
        if (entry.error) throw entry.error;
        if (
          sessions.get(socket.id) !== entry ||
          !Number.isFinite(entry.identity.expiration) ||
          entry.identity.expiration * 1000 <= Date.now()
        )
          throw new ApplicationError(401, 'REALTIME_CONTEXT_CHANGED');
        return entry.identity;
      };
    },
    /** @param {Object} change Evento acreditado. @returns {Promise<void>} Una consulta por conexión afectada, incluso sin ventanas. */
    async changed(change) {
      epoch++;
      const pending = [];
      for (const entry of sessions.values())
        if (matches(entry.identity, change)) pending.push(refresh(entry));
      await Promise.all(pending);
    },
    /** @returns {void} Suspende controles y entregas hasta recuperar continuidad. */
    unavailable() {
      epoch++;
      continuity++;
      available = false;
    },
    /** @returns {Promise<void>} Reacredita conexiones antes de habilitar recuperación HTTP. */
    async recover() {
      epoch++;
      available = false;
      const ownContinuity = ++continuity;
      await Promise.all([...sessions.values()].map(refresh));
      if (ownContinuity !== continuity) throw new ApplicationError(503, 'REALTIME_DISCONTINUITY');
      available = true;
    },
    /** @param {string} socketId Conexión sustituida. @returns {void} Invalida resultados todavía en vuelo. */
    reset(socketId) {
      sessions.delete(socketId);
    },
    /** @returns {void} Cierra sin dejar credenciales o identidades reutilizables. */
    close() {
      this.unavailable();
      sessions.clear();
    },
  };
}
module.exports = {
  connectionAuthorization,
  principalKey,
};
