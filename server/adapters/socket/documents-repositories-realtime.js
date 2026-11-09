/** @file Suscripciones de repositorios completos sobre el canal documental existente; sin temporizadores de consulta. */
'use strict';
const { ApplicationError } = require('../../shared/application-error');
const { deliverRealtime }  = require('../../library/realtime/protocol');
const { contentCursor }    = require('../../library/realtime/window-delta');

/** @param {Object} options Mensajería autenticada compartida. @returns {Object} Observación, invalidación y recuperación de repositorios. */
function repositoryWatches({
  messaging,
  maxPending = 128,
  reads = require('../../modules/realtime/compositions/realtime.composition').repositoryReads,
}) {
  const observe  = require('../telemetry/realtime').realtimeMetrics(
    require('@opentelemetry/api').metrics,
  );
  const sessions = new Map();
  let available  = true;
  /** @param {Object} session Sesión vigente. @param {Object} watch Suscripción. @returns {boolean} Continúa vinculada al mismo socket. */
  function current(session, watch) {
    return sessions.get(session.socket.id) === session && session.watches.get(watch.id) === watch;
  }
  /** @param {Object} session Conexión. @param {Object} watch Suscripción. @param {string} action Evento. @param {Object} data Proyección pública. @returns {void} Entrega una secuencia contigua sin datos de autoridad. */
  function emit(session, watch, action, data = {}) {
    if (!current(session, watch)) return;
    if (action === 'documents-invalidated' && !watch.bound) {
      if (watch.buffer.length >= maxPending) watch.interrupted = true;
      else watch.buffer.push(data);
      return;
    }
    if (action === 'documents-resync-required') watch.interrupted = true;
    if (action === 'documents-invalidated' && !data.metadataOnly && data.cursor <= watch.floor)
      return;
    if (action === 'documents-invalidated' && data.repository) {
      const fingerprint = contentCursor(data.repository);
      if (data.metadataOnly && fingerprint === watch.repositoryFingerprint) return;
      watch.repositoryFingerprint = fingerprint;
    }
    const type =
      action === 'documents-invalidated'
        ? 'patch'
        : action === 'documents-resync-required'
          ? 'resync-required'
          : [401, 403, 404].includes(data.statusCode)
            ? 'revoked'
            : 'error';
    observe(type);
    deliverRealtime(session.socket, {
      type,
      topic            : 'documents.entries',
      section          : 'repository',
      subscriptionId   : watch.id,
      watchVersion     : watch.watchVersion,
      delivery         : ++watch.sequence,
      eventId          : data.changeCode,
      queryFingerprint : watch.queryFingerprint,
      cursor           : data.cursor === undefined ? undefined : String(data.cursor),
      data             : {
        repositoryId : watch.repositoryId,
        ...data,
      },
    });
  }
  /** @param {Object} identity Principal verificado del socket. @returns {Object} Contexto canónico para acreditar el recibo HTTP. */
  function readIdentity(identity) {
    return {
      context : {
        user        : { userCode: identity.userCode },
        workspace   : identity.activeWorkspace,
        sessionCode : identity.sessionCode,
        delegation  : identity.delegation,
      },
    };
  }
  /** @param {Object} session Conexión con principal verificado. @param {Object} watch Raíz solicitada. @returns {Promise<Object>} Sólo un repositorio actualmente legible. */
  async function authorize(session, watch, attempt = 0) {
    const identity = await session.verify();
    const context  = {
      user       : { userCode: identity.userCode },
      workspace  : identity.activeWorkspace,
      workspaces : identity.workspaces,
      policies   : identity.policies,
      ...(identity.delegation ? { delegation: identity.delegation } : {}),
    };
    const result   = await messaging.request(
      'service-documents',
      'documents-repositories-get-list',
      {},
      { context },
    );
    if (result.statusCode !== 200)
      throw new ApplicationError(result.statusCode || 503, 'DOCUMENT_REALTIME_UNAVAILABLE');
    const root = result.data.listItem.find(
      /** @param {Object} item Raíz autorizada. @returns {boolean} Coincide con la suscripción. */
      (item) => item.id === watch.repositoryId && item.capabilities.view,
    );
    if (!root) throw new ApplicationError(403, 'DOCUMENT_REALTIME_FORBIDDEN');
    const verified = await session.verify();
    if (
      JSON.stringify([
        identity.userCode,
        identity.activeWorkspace,
        identity.policies,
        identity.workspaces,
        identity.delegation,
      ]) !==
      JSON.stringify([
        verified.userCode,
        verified.activeWorkspace,
        verified.policies,
        verified.workspaces,
        verified.delegation,
      ])
    ) {
      if (attempt >= 2) throw new ApplicationError(503, 'DOCUMENT_REALTIME_UNAVAILABLE');
      return authorize(session, watch, attempt + 1);
    }
    if (!available) throw new ApplicationError(503, 'DOCUMENT_REALTIME_UNAVAILABLE');
    return root;
  }
  /** @param {Object} session Conexión. @param {Object} watch Suscripción. @param {Function} work Entrega bajo permisos vigentes. @returns {Promise<void>} Serializa eventos y distingue revocación de indisponibilidad. */
  function schedule(session, watch, work) {
    if (watch.overflow) return watch.pending;
    if (watch.queued >= maxPending) {
      watch.overflow = true;
      const recovery = watch.pending.then(
        /** @returns {Promise<void>} Descarta la ráfaga desbordada y solicita una única lectura puntual. */
        async () => {
          if (!current(session, watch)) return;
          try {
            await authorize(session, watch);
            emit(session, watch, 'documents-resync-required', { reason: 'QUEUE_OVERFLOW' });
          } catch (error) {
            emit(session, watch, 'documents-error', { statusCode: error.statusCode || 503 });
            if ([401, 403, 404].includes(error.statusCode)) session.watches.delete(watch.id);
            else throw error;
          } finally {
            watch.overflow = false;
          }
        },
      );
      watch.pending = recovery.catch(
        /** @returns {void} Mantiene el cierre y la recuperación observables sin rechazos huérfanos. */
        () => {},
      );
      return recovery;
    }
    watch.queued++;
    const pending = watch.pending.then(
      /** @returns {Promise<void>} Revalida antes de publicar; una suscripción sustituida no emite. */
      async () => {
        if (!current(session, watch) || watch.overflow) return;
        try {
          const root = await authorize(session, watch);
          if (current(session, watch)) await work(root);
        } catch (error) {
          emit(session, watch, 'documents-error', { statusCode: error.statusCode || 503 });
          if ([401, 403, 404].includes(error.statusCode)) session.watches.delete(watch.id);
          else throw error;
        }
      },
    );
    watch.pending = pending
      .finally(
        /** @returns {void} Libera capacidad también al revocar o sustituir esta suscripción. */
        () => {
          watch.queued--;
        },
      )
      .catch(
        /** @returns {void} Conserva la cola utilizable tras un fallo informado al productor. */ () => {},
      );
    return pending;
  }
  return {
    /** @param {Object} socket Conexión. @param {Function} verify Verificación vigente. @returns {void} Renueva sin perder suscripciones de la misma identidad. */
    authenticate(socket, verify) {
      const session = sessions.get(socket.id);
      if (session) session.verify = verify;
      else
        sessions.set(socket.id, {
          socket,
          verify,
          watches : new Map(),
        });
    },
    /** @param {Object} socket Conexión. @param {Object} value Identidad y versión de la suscripción. @returns {Promise<Object>} Confirmación previa al inventario inicial. */
    async watch(socket, value) {
      const session = sessions.get(socket.id);
      if (!session) throw new ApplicationError(401, 'UNAUTHORIZED');
      if (
        !value ||
        !/^[a-zA-Z0-9_-]{1,100}$/.test(value.subscriptionId) ||
        !/^[a-fA-F0-9]{24}$/.test(value.repositoryId) ||
        !Number.isSafeInteger(value.watchVersion) ||
        value.watchVersion < 1
      )
        throw new ApplicationError(400, 'INVALID_DOCUMENT_SUBSCRIPTION');
      const previous = session.watches.get(value.subscriptionId);
      if (previous && value.watchVersion <= previous.watchVersion)
        throw new ApplicationError(409, 'OLD_DOCUMENT_VIEW');
      if (!previous && session.watches.size >= 100)
        throw new ApplicationError(429, 'DOCUMENT_SUBSCRIPTION_LIMIT');
      const watch = {
        id           : value.subscriptionId,
        repositoryId : value.repositoryId,
        watchVersion : value.watchVersion,
        sequence     : 0,
        queued       : 0,
        overflow     : false,
        pending      : Promise.resolve(),
        seen         : new Set(),
        buffer       : [],
        bound        : false,
        floor        : -1,
        interrupted  : false,
      };
      await authorize(session, watch);
      Object.assign(watch, await reads().register(value, readIdentity(await session.verify())));
      if (sessions.get(socket.id) !== session) throw new ApplicationError(401, 'UNAUTHORIZED');
      if ((session.watches.get(watch.id)?.watchVersion || 0) >= watch.watchVersion)
        throw new ApplicationError(409, 'OLD_DOCUMENT_VIEW');
      session.watches.set(watch.id, watch);
      return {
        statusCode       : 200,
        subscriptionId   : watch.id,
        watchVersion     : watch.watchVersion,
        receiptId        : watch.receiptId,
        queryFingerprint : watch.queryFingerprint,
      };
    },
    /** @param {Object} socket Conexión. @param {Object} value Confirmación del recibo HTTP. @returns {Promise<Object>} Activa únicamente la base emitida por el productor para esta generación. */
    async bind(socket, value) {
      const session = sessions.get(socket.id),
        watch       = session?.watches.get(value.subscriptionId);
      if (
        !watch ||
        watch.watchVersion !== value.watchVersion ||
        watch.receiptId !== value.receiptId ||
        watch.bound
      )
        throw new ApplicationError(409, 'DOCUMENT_READ_DISCONTINUITY');
      await authorize(session, watch);
      const receipt = await reads().bind(
        {
          ...value,
          query : { repositoryId: watch.repositoryId },
        },
        readIdentity(await session.verify()),
      );
      if (!current(session, watch) || watch.bound)
        throw new ApplicationError(409, 'DOCUMENT_READ_DISCONTINUITY');
      watch.floor = receipt.revision;
      watch.bound = true;
      const buffered = watch.buffer.splice(0);
      if (watch.interrupted)
        emit(session, watch, 'documents-resync-required', { reason: 'READ_DISCONTINUITY' });
      else for (const data of buffered) emit(session, watch, 'documents-invalidated', data);
      return {
        statusCode       : 200,
        cursor           : receipt.cursor,
        queryFingerprint : receipt.queryFingerprint,
      };
    },
    /** @param {string} socketId Conexión. @param {Object} value Identidad de suscripción. @returns {void} Retira exclusivamente la versión solicitada. */
    unwatch(socketId, value) {
      const watches = sessions.get(socketId)?.watches;
      if (watches?.get(value.subscriptionId)?.watchVersion === value.watchVersion)
        watches.delete(value.subscriptionId);
    },
    /** @param {Object} data Evento canónico de Documents, acreditado por el adaptador padre. @returns {Promise<void>} Entrega a ambas raíces de un movimiento y deduplica reintentos. */
    async changed(data) {
      if (data.operation === 'favorite') return;
      const tasks = [];
      for (const session of sessions.values())
        for (const watch of session.watches.values()) {
          if (![data.repositoryId, data.previousRepositoryId].includes(watch.repositoryId))
            continue;
          tasks.push(
            schedule(
              session,
              watch,
              /** @param {Object} root Repositorio autorizado. @returns {Promise<void>} Entrega metadatos públicos o retirada explícita de la entrada. */
              async () => {
                if (watch.seen.has(data.changeCode)) return;
                let item      = null;
                const removed =
                  ['delete', 'trash'].includes(data.operation) ||
                  data.repositoryId !== watch.repositoryId;
                if (!removed) {
                  const identity = await session.verify();
                  const result   = await messaging.request(
                    'service-documents',
                    'documents-entry-get-one',
                    { entryId: data.entryId },
                    {
                      context : {
                        user       : { userCode: identity.userCode },
                        workspace  : identity.activeWorkspace,
                        workspaces : identity.workspaces,
                        policies   : identity.policies,
                        ...(identity.delegation ? { delegation: identity.delegation } : {}),
                      },
                    },
                  );
                  if (
                    result.statusCode === 200 &&
                    result.data.item.repositoryId === watch.repositoryId &&
                    !result.data.item.trashedAt
                  )
                    item = result.data.item;
                  else if (![200, 404].includes(result.statusCode))
                    throw new ApplicationError(
                      result.statusCode || 503,
                      'DOCUMENT_REALTIME_UNAVAILABLE',
                    );
                }
                const freshRoot = await authorize(session, watch);
                if (!current(session, watch)) return;
                emit(session, watch, 'documents-invalidated', {
                  entryId              : data.entryId,
                  operation            : data.operation,
                  entryRevision        : item?.revision || data.revision,
                  changeCode           : data.changeCode,
                  repository           : freshRoot,
                  cursor               :
                    watch.repositoryId === data.repositoryId
                      ? data.repositoryRevision
                      : data.previousRepositoryRevision,
                  previousRepositoryId : data.previousRepositoryId || null,
                  previousParentId     : data.previousParentId || null,
                  upserts              : item ? [item] : [],
                  removedIds           : item ? [] : [data.entryId],
                });
                watch.seen.add(data.changeCode);
                if (watch.seen.size > 2048) watch.seen.delete(watch.seen.values().next().value);
              },
            ),
          );
        }
      await Promise.all(tasks);
    },
    /** @returns {void} La pérdida del productor suspende todas las suscripciones sin recurrir a polling. */
    unavailable() {
      available = false;
      for (const session of sessions.values())
        for (const watch of session.watches.values())
          emit(session, watch, 'documents-error', { statusCode: 503 });
    },
    /** @param {string} socketId Conexión cuya credencial expiró. @returns {void} Solicita renovación sin usar otro canal. */
    requireAuthentication(socketId) {
      const session = sessions.get(socketId);
      if (!session) return;
      for (const watch of session.watches.values())
        emit(session, watch, 'documents-auth-required', {
          statusCode : 401,
          reason     : 'AUTH_REQUIRED',
        });
    },
    /** @returns {Promise<void>} El restablecimiento del productor fuerza una reconciliación puntual. */
    async recover() {
      available = true;
      const tasks = [];
      for (const session of sessions.values())
        for (const watch of session.watches.values())
          tasks.push(
            schedule(
              session,
              watch /** @returns {void} Marca discontinuidad sin inventar eventos perdidos. */,
              () => emit(session, watch, 'documents-resync-required'),
            ),
          );
      await Promise.allSettled(tasks);
    },
    /** @param {Object} change Referencia de Security. @returns {Promise<void>} Retira una concesión revocada sin reconciliar manifiestos íntegros. */
    async accessChanged(change) {
      const tasks = [];
      for (const session of sessions.values()) {
        if (
          !require('../../modules/realtime/services/access-reference.service').matchesAccess(
            session.socket.data.identity,
            change,
          )
        )
          continue;
        for (const watch of session.watches.values())
          tasks.push(
            schedule(
              session,
              watch,
              /** @param {Object} repository Raíz reacreditada. @returns {void} Actualiza capacidades sin sustituir entradas ni transferencias. */
              (repository) =>
                emit(session, watch, 'documents-invalidated', {
                  repository,
                  metadataOnly : true,
                  cursor       : repository.revision,
                  upserts      : [],
                  removedIds   : [],
                }),
            ),
          );
      }
      await Promise.all(tasks);
    },
    /** @param {string} repositoryId Raíz afectada. @returns {Promise<void>} Reacredita metadatos sin invalidar manifiestos por un cambio ordinario. */
    async control(repositoryId) {
      if (!available) return;
      const tasks = [];
      for (const session of sessions.values())
        for (const watch of session.watches.values())
          if (watch.repositoryId === repositoryId)
            tasks.push(
              schedule(
                session,
                watch,
                /** @param {Object} repository Raíz acreditada. @returns {void} Conserva entradas, transferencias y revisiones de los archivos. */
                (repository) =>
                  emit(session, watch, 'documents-invalidated', {
                    repository,
                    metadataOnly : true,
                    cursor       : repository.revision,
                    upserts      : [],
                    removedIds   : [],
                  }),
              ),
            );
      await Promise.allSettled(tasks);
    },
    /** @param {string} socketId Conexión cerrada. @returns {void} Descarta trabajo y suscripciones. */
    reset(socketId) {
      sessions.delete(socketId);
    },
    /** @returns {void} Libera todas las referencias al cerrar Platform. */
    close() {
      sessions.clear();
    },
  };
}
module.exports = { repositoryWatches };
