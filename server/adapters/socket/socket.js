/** @file Sesiones Socket.IO: autentica tokens con Security y conserva exclusivamente la identidad verificada. */
'use strict';
const { principalKey } = require('./connection-authorization');
/**
 * Ejecuta registerSocketEvents dentro del flujo de socket.
 * @param {Object} options Colaboradores u opciones: io, messaging.
 * @returns {void} Finaliza después de aplicar los efectos de la operación.
 */
function registerSocketEvents({ io, messaging }) {
  const documents = messaging.documentsRealtime;
  io.on(
    'connection',

    /**
     * Atiende el evento emitido por el colaborador.
     * @param {*} socket Valor de socket utilizado por esta operación.
     * @returns {void} Finaliza después de aplicar los efectos de la operación.
     */
    (socket) => {
      let revision  = 0;
      let connected = true;
      let expirationTimer;
      documents?.bind(socket);
      /**
       * Ejecuta reset dentro del flujo de socket.
       * @param {*} [current] Valor de current utilizado por esta operación.
       * @returns {Promise<undefined>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      const reset        = async (current = revision) => {
        if (current !== revision) return;
        clearTimeout(expirationTimer);
        socket.userCode = '';
        socket.data.identity = null;
        documents?.reset(socket.id);
        for (const room of [...socket.rooms]) {
          if (current !== revision) return;
          if (room !== socket.id) await socket.leave(room);
        }
        if (connected && current === revision) await socket.join('guest');
      };
      /**
       * Ejecuta authenticate dentro del flujo de socket.
       * @param {*} [payload] Valor de payload utilizado por esta operación.
       * @param {*} acknowledge Valor de acknowledge utilizado por esta operación.
       * @returns {Promise<undefined>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      const authenticate = async (payload = {}, acknowledge) => {
        const current   = ++revision;
        const delegated = typeof payload?.ticket === 'string';
        const previous  = socket.data.identity;
        if (!delegated) await reset(current);
        if (!connected || current !== revision) return;
        const token = delegated ? payload.ticket : payload?.token;
        if (typeof token !== 'string' || !token || token.length > 16384) {
          if (typeof acknowledge === 'function') acknowledge({ statusCode: 401 });
          return;
        }
        try {
          const epoch    = documents?.beginAuthentication();
          const response = await messaging.request(
            'service-security',
            delegated ? 'oauth-realtime-ticket-consume' : 'session-authenticate',
            delegated
              ? { ticket: token }
              : {
                  token,
                  currentPolicies : true,
                },
            { timeoutMs: 8000 },
          );
          if (!connected || current !== revision) return;
          if (response.statusCode !== 200)
            throw Object.assign(new Error('Authentication failed'), {
              statusCode : [401, 403].includes(response.statusCode) ? 401 : 503,
            });
          const principal = response?.data;
          const identity  =
            delegated && principal
              ? {
                  userCode        : principal.userCode,
                  policies        : principal.policies,
                  workspaces      : principal.workspaces,
                  activeWorkspace : principal.workspace,
                  expiration      : principal.expiresAt / 1000,
                  delegation      : {
                    grantCode : principal.grantCode,
                    clientId  : principal.clientId,
                  },
                }
              : principal;
          if (
            response.statusCode !== 200 ||
            (!delegated && identity?.currentPoliciesVerified !== true) ||
            (delegated &&
              (principal?.channel !== 'realtime' ||
                !principal.scopes?.includes('documents:read'))) ||
            typeof identity?.userCode !== 'string' ||
            !identity.userCode ||
            !Number.isFinite(identity.expiration) ||
            identity.expiration * 1000 <= Date.now()
          )
            throw new Error('Unauthorized');
          const sameDelegation =
            delegated &&
            previous?.actorType === 'documents-delegated' &&
            principalKey(previous) ===
              principalKey({
                ...identity,
                actorType : 'documents-delegated',
              });
          if (delegated && !sameDelegation) await reset(current);
          clearTimeout(expirationTimer);
          await socket.leave('guest');
          if (!connected || current !== revision) return;
          if (!delegated) await socket.join('platform');
          if (!connected || current !== revision) return;
          if (identity.expiration * 1000 <= Date.now()) throw new Error('Unauthorized');
          socket.data.identity = {
            ...(delegated
              ? {
                  actorType  : 'documents-delegated',
                  delegation : identity.delegation,
                }
              : {}),
            userCode        : identity.userCode,
            sessionCode     : identity.sessionCode,
            groups          : Array.isArray(identity.groups)
              ? identity.groups.filter(
                  /**
                   * Determina si el elemento cumple el criterio de selección.
                   * @param {*} value Valor de value utilizado por esta operación.
                   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
                   */
                  (value) => typeof value === 'string' && value,
                )
              : [],
            policies        : Array.isArray(identity.policies)
              ? identity.policies.filter(
                  /**
                   * Determina si el elemento cumple el criterio de selección.
                   * @param {*} value Valor de value utilizado por esta operación.
                   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
                   */
                  (value) => typeof value === 'string' && value,
                )
              : [],
            activeWorkspace :
              typeof identity.activeWorkspace === 'string' ? identity.activeWorkspace : '',
            ...(Array.isArray(identity.workspaces)
              ? {
                  workspaces : identity.workspaces.filter(
                    /**
                     * Determina si el elemento cumple el criterio de selección.
                     * @param {string} code Identificador público del recurso.
                     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
                     */
                    (code) => typeof code === 'string' && code,
                  ),
                }
              : {}),
            expiration      : identity.expiration,
          };
          socket.userCode = identity.userCode;
          /** Revalida el token y los permisos actuales con Security, conservando el vínculo con esta sesión. @returns Resultado acreditado o finalización de los efectos descritos. */
          const verify =
            /**
             * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
             * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
             */
            async () => {
              if (!connected || current !== revision)
                throw Object.assign(new Error('Unauthorized'), { statusCode: 401 });
              const verified = await messaging.request(
                'service-security',
                'session-authenticate',
                {
                  token,
                  currentPolicies : true,
                },
                { timeoutMs: 8000 },
              );
              if (!connected || current !== revision || verified?.statusCode === 401)
                throw Object.assign(new Error('Unauthorized'), { statusCode: 401 });
              if (
                verified?.statusCode !== 200 ||
                verified.data?.currentPoliciesVerified !== true ||
                verified.data?.userCode !== identity.userCode ||
                !Number.isFinite(verified.data.expiration) ||
                verified.data.expiration * 1000 <= Date.now()
              )
                throw Object.assign(new Error('Authentication unavailable'), { statusCode: 503 });
              return verified.data;
            };
          /** @returns {Promise<Object>} Revalida concesión y espacio; una renovación de la misma identidad no descarta trabajo en vuelo. */
          const verifyDelegated = async () => {
            const currentIdentity = socket.data.identity;
            if (
              !connected ||
              currentIdentity?.actorType !== 'documents-delegated' ||
              currentIdentity.expiration * 1000 <= Date.now()
            )
              throw Object.assign(new Error('Unauthorized'), { statusCode: 401 });
            const checked = await messaging.request(
              'service-security',
              'oauth-principal-resolve',
              {
                ...currentIdentity.delegation,
                workspace : currentIdentity.activeWorkspace,
              },
              { timeoutMs: 8000 },
            );
            const fresh   = checked.data;
            if (checked.statusCode !== 200)
              throw Object.assign(new Error('Authorization failed'), {
                statusCode : checked.statusCode || 503,
              });
            if (
              !fresh.scopes.includes('documents:read') ||
              fresh.userCode !== currentIdentity.userCode ||
              fresh.workspace !== currentIdentity.activeWorkspace ||
              socket.data.identity?.delegation?.grantCode !== currentIdentity.delegation.grantCode
            )
              throw Object.assign(new Error('Unauthorized'), { statusCode: 403 });
            return {
              ...currentIdentity,
              policies   : fresh.policies,
              workspaces : fresh.workspaces,
            };
          };
          if (delegated) documents?.authenticateRepositories(socket, verifyDelegated, { epoch });
          else {
            documents?.authenticate(socket, verify, { epoch });
          }
          /**
           * Ejecuta scheduleExpiration dentro del flujo de socket.
           * @returns {undefined} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          const scheduleExpiration = () => {
            if (!connected || current !== revision) return;
            const remaining = identity.expiration * 1000 - Date.now();
            if (remaining <= 0) {
              if (delegated) documents?.requireAuthenticationRepositories(socket.id);
              revision++;
              void reset();
              return;
            }
            expirationTimer = setTimeout(scheduleExpiration, Math.min(remaining, 2147483647));
            expirationTimer.unref?.();
          };
          scheduleExpiration();
          if (typeof acknowledge === 'function')
            acknowledge({
              statusCode : 200,
              ...(delegated ? { expiresAt: identity.expiration * 1000 } : {}),
            });
        } catch (error) {
          if (current === revision) {
            await reset(current);
            if (current === revision && typeof acknowledge === 'function')
              acknowledge({ statusCode: error.statusCode >= 500 ? 503 : 401 });
          }
        }
      };
      void reset();
      socket.on('start-session', authenticate);
      socket.on(
        'end-session',

        /**
         * Atiende el evento emitido por el colaborador.
         * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
         */
        async () => {
          revision++;
          await reset();
        },
      );
      socket.on(
        'disconnect',

        /**
         * Atiende el evento emitido por el colaborador.
         * @returns {void} Finaliza después de aplicar los efectos de la operación.
         */
        () => {
          revision++;
          connected = false;
          clearTimeout(expirationTimer);
          socket.data.identity = null;
          documents?.reset(socket.id);
        },
      );
      if (socket.handshake.auth?.token) void authenticate({ token: socket.handshake.auth.token });
    },
  );
}
module.exports = { registerSocketEvents };
