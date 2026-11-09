/** @file Ventanas públicas: lectura HTTP acreditada, recibos del productor y diferencias por suscripción. */
'use strict';
const { performance }                            = require('node:perf_hooks');
const { randomUUID }                             = require('node:crypto');
const { ReadWindow }                             = require('./read-window');
const { contentCursor }                          = require('./window-delta');
const { validatedSnapshot }                      = require('./snapshot');
const { registerRealtimeTopic, deliverRealtime } = require('./protocol');

/** @param {number} statusCode Estado público. @returns {Error} Fallo sin datos privados. */
function failure(statusCode) {
  return Object.assign(new Error('REALTIME_UNAVAILABLE'), { statusCode });
}

/** @param {Object} identity Contexto acreditado por el transporte. @returns {string} Frontera de identidad; no contiene credenciales. */
function audience(identity) {
  if (!identity?.userCode || !identity.workspace) throw failure(401);
  const delegation = identity.context?.delegation || identity.delegation;
  if (delegation) {
    if (!delegation.grantCode || !delegation.clientId) throw failure(401);
    return contentCursor([
      'delegation',
      identity.userCode,
      identity.workspace,
      delegation.grantCode,
      delegation.clientId,
    ]);
  }
  if (!identity.sessionCode) throw failure(401);
  return contentCursor([identity.userCode, identity.sessionCode, identity.workspace]);
}

/** Mantiene sólo ventanas activas; el almacenamiento de recibos pertenece a infraestructura. */
class RealtimeWindows {
  /** @param {Object} options Registro cerrado de dominios, recibos y límites. */
  constructor({
    topics,
    receipts,
    clock = Date.now,
    maxSubscriptions = 32,
    maxBytes = 4 * 1024 * 1024,
    observe,
  }) {
    this.topics = topics;
    this.receipts = receipts;
    this.clock = clock;
    this.maxSubscriptions = maxSubscriptions;
    this.maxBytes = maxBytes;
    this.observe = observe;
    this.sessions = new Map();
    this.registered = new WeakSet();
  }
  /** @param {string} operation Clasificación técnica cerrada, sin consultas ni identidad. @returns {void} La telemetría nunca altera el resultado funcional. */
  record(operation, duration) {
    try {
      this.observe?.(operation, duration);
    } catch {
      /* Un exportador indisponible no interrumpe la sección. */
    }
  }

  /** @param {string} operation Etapa estática. @param {Function} work Trabajo ya autorizado.
   * @returns {Promise<*>} Mismo resultado o error; el reloj monótono sólo mide duración.
   */
  async measure(operation, work) {
    const started = performance.now();
    try {
      return await work();
    } finally {
      this.record(operation, performance.now() - started);
    }
  }

  /** @param {Object} session Asociación vigente. @returns {Promise<Object>} Comparte sólo aperturas simultáneas, nunca una autorización resuelta ni la entrega de datos. */
  subscriptionIdentity(session) {
    if (session.openingIdentity) return session.openingIdentity;
    const pending = Promise.resolve()
      .then(
        /** @returns {Promise<Object>} Una consulta fresca para este grupo de aperturas. */
        () => session.verify(),
      )
      .finally(
        /** @returns {void} La próxima apertura vuelve a consultar Security. */
        () => {
          if (session.openingIdentity === pending) session.openingIdentity = null;
        },
      );
    session.openingIdentity = pending;
    return pending;
  }

  /** @param {Object} request Selección técnica. @param {Object} identity Principal verificado. @returns {Object} Capacidad canónica explícita y admisible para su modalidad de acceso. */
  specification(request, identity) {
    const spec = this.topics[request.topic]?.[request.section];
    if (!spec) throw failure(400);
    if ((identity?.context?.delegation || identity?.delegation) && spec.delegated !== true)
      throw failure(403);
    return spec;
  }

  /** @param {Object} request Consulta HTTP. @param {Object} identity Principal de transporte. @returns {Promise<Object>} Única instantánea, destinada exclusivamente al Gateway. */
  async read(request, identity) {
    const owner            = audience(identity);
    const spec             = this.specification(request, identity);
    const query            = spec.validate(request.query);
    const queryFingerprint = contentCursor(query);
    const registration     = await this.measure(
      'receipt-read',
      /** @returns {Promise<Object>} Recibo privado del servidor. */
      () => this.receipts.get(request.receiptId, owner),
    );
    if (
      !registration ||
      registration.state !== 'registered' ||
      registration.expiresAt.getTime() <= this.clock() ||
      registration.subscriptionId !== request.subscriptionId ||
      registration.watchVersion !== request.watchVersion ||
      registration.topic !== request.topic ||
      registration.section !== request.section ||
      registration.queryFingerprint !== queryFingerprint
    )
      throw failure(409);
    this.record(registration.purpose === 'recovery' ? 'http-recovery' : 'http-read');
    const { data, cursor } = validatedSnapshot(
      await this.measure(
        'producer-read',
        /** @returns {Promise<Object>} Proyección canónica del productor. */
        () => spec.read(query, identity, { receiptId: registration.receiptId }),
      ),
    );
    if (Buffer.byteLength(JSON.stringify(data)) > this.maxBytes) throw failure(413);
    const control   = {
      subscriptionId : request.subscriptionId,
      watchVersion   : request.watchVersion,
      topic          : request.topic,
      section        : request.section,
      queryFingerprint,
      cursor,
      receiptId      : registration.receiptId,
    };
    const completed = await this.measure(
      'receipt-write',
      /** @returns {Promise<boolean>} Instala la base obtenida por HTTP. */
      () =>
        this.receipts.complete({
          ...control,
          owner,
          query,
          data,
          projectionHash : contentCursor(data),
          expiresAt      : new Date(this.clock() + 120000),
        }),
    );
    if (!completed) throw failure(409);
    return {
      data,
      control,
    };
  }

  /** @param {Object} socket Transporte ya autenticado. @param {Function} verify Reacredita identidad y permisos. @param {Object} options Renovación de una concesión cuya identidad ya verificó el transporte. @returns {void} Registra controles sin leer listas. */
  authenticate(socket, verify, { preserve = false } = {}) {
    const prior = this.sessions.get(socket.id);
    if (preserve && prior?.socket === socket) {
      prior.openingIdentity = null;
      prior.verify = verify;
      return;
    }
    this.reset(socket.id);
    const session = {
      socket,
      verify,
      windows : new Map(),
    };
    this.sessions.set(socket.id, session);
    if (this.registered.has(socket)) return;
    this.registered.add(socket);
    if (socket.conn?.on) {
      /** @returns {void} El heartbeat del transporte renueva sólo leases técnicos, nunca consulta datos de negocio. */
      const heartbeat = () => {
        void this.heartbeat(socket.id);
      };
      socket.conn.on('heartbeat', heartbeat);
      socket.once(
        'disconnect',
        /** @returns {void} El listener pertenece a este socket, aunque se comparta la conexión Engine.IO. */
        () => socket.conn.off('heartbeat', heartbeat),
      );
    }
    for (const topic of Object.keys(this.topics)) {
      registerRealtimeTopic(
        socket,
        topic,
        /** @param {Object} request Control estándar. @returns {Promise<Object>} ACK exclusivamente técnico. */
        (request) => this.control(this.sessions.get(socket.id), request),
      );
    }
  }

  /** @param {Object} session Conexión. @param {Object} request Control. @returns {Promise<Object>} Confirma o rechaza la generación. */
  async control(session, request) {
    if (!session || this.sessions.get(session.socket.id) !== session) throw failure(401);
    if (request.type === 'bind') {
      const previous = session.windows.get(request.subscriptionId);
      if (
        !previous ||
        previous.window.watchVersion !== request.watchVersion ||
        previous.window.topic !== request.topic ||
        previous.window.section !== request.section
      )
        throw failure(409);
      const receipt = await this.measure(
        'receipt-read',
        /** @returns {Promise<Object>} Recibo privado del servidor. */
        () => this.receipts.get(request.receiptId, previous.owner),
      );
      if (
        !receipt ||
        receipt.state !== 'ready' ||
        receipt.receiptId !== previous.receiptId ||
        receipt.expiresAt.getTime() <= this.clock() ||
        receipt.subscriptionId !== request.subscriptionId ||
        receipt.watchVersion !== request.watchVersion ||
        receipt.topic !== request.topic ||
        receipt.section !== request.section ||
        receipt.queryFingerprint !== previous.queryFingerprint
      )
        throw failure(409);
      if (previous.window.bound || previous.window.binding || previous.window.closed)
        throw failure(409);
      previous.anchor = receipt.data;
      await this.measure(
        'bind',
        /** @returns {Promise<void>} Revalida permisos después de leer el recibo. */
        () => previous.window.bind(receipt),
      );
      return {
        statusCode       : 200,
        cursor           : receipt.cursor,
        queryFingerprint : receipt.queryFingerprint,
      };
    }
    const identity = await this.measure(
      'authorization',
      /** @returns {Promise<Object>} Identidad y permisos actuales. */
      () =>
        ['subscribe', 'resync'].includes(request.type)
          ? this.subscriptionIdentity(session)
          : session.verify(),
    );
    const owner    = audience(identity);
    if (this.sessions.get(session.socket.id) !== session) throw failure(401);
    const spec     = this.specification(request, identity);
    const previous = session.windows.get(request.subscriptionId);
    if (request.type === 'unsubscribe') {
      if (
        previous &&
        request.watchVersion === previous.window.watchVersion &&
        previous.window.topic === request.topic &&
        previous.window.section === request.section
      ) {
        previous.window.close();
        session.windows.delete(request.subscriptionId);
      }
      return { statusCode: 200 };
    }
    if (!['subscribe', 'resync'].includes(request.type)) throw failure(400);
    if (previous && request.watchVersion <= previous.window.watchVersion) throw failure(409);
    if (!previous && session.windows.size >= this.maxSubscriptions) throw failure(429);
    const query            = spec.validate(request.query);
    const queryFingerprint = contentCursor(query);
    const entry            = {
      owner,
      query,
      queryFingerprint,
      identity,
      receiptId : randomUUID(),
    };
    entry.window = new ReadWindow({
      subscriptionId : request.subscriptionId,
      watchVersion   : request.watchVersion,
      topic          : request.topic,
      section        : request.section,
      collections    : spec.collections,
      /** @returns {Promise<void>} Revalida antes y después de cada lectura. */
      authorize      : async () => {
        if (this.sessions.get(session.socket.id) !== session) throw failure(401);
        entry.identity = await this.measure(
          'authorization',
          /** @returns {Promise<Object>} Identidad y permisos actuales. */
          () => session.verify(),
        );
        if (this.sessions.get(session.socket.id) !== session) throw failure(401);
        this.specification(request, entry.identity);
        if (audience(entry.identity) !== owner) throw failure(401);
      },
      /** @param {Object} input Consulta acreditada por el recibo HTTP. @returns {Promise<Object>} Misma capacidad del productor. */
      read           : (input, changes) => {
        this.record('window-read');
        return spec.readWindow
          ? spec.readWindow(input, entry.identity, entry.anchor, changes, {
              receiptId : entry.receiptId,
            })
          : spec.read(input, entry.identity, { receiptId: entry.receiptId });
      },
      /** @returns {void} Retira la reserva después de resolver su apertura, incluso si se canceló antes del ACK. */
      onClose        : () => {
        if (!entry.openTask || !spec.close) return;
        entry.releasing = entry.openTask
          .catch(
            /** @returns {void} Una apertura de resultado incierto también requiere liberar su identidad. */
            () => {},
          )
          .then(
            /** @returns {Promise<void>} Sólo retira el lease propio con su contexto original. */
            () => spec.close(entry.receiptId, identity),
          )
          .catch(
            /** @returns {void} Si el productor está caído, su vencimiento durable limita la reserva pendiente. */
            () => {},
          );
      },
      /** @param {Object} delivery Diferencia. @returns {void} Descarta entregas de otra conexión. */
      emit           : (delivery) => {
        if (this.sessions.get(session.socket.id) === session) {
          this.record(delivery.type);
          deliverRealtime(session.socket, delivery);
        }
      },
    });
    previous?.window.close();
    session.windows.set(request.subscriptionId, entry);
    try {
      if (spec.open) {
        if (!spec.renew || !spec.close) throw failure(503);
        entry.openTask = Promise.resolve().then(
          /** @returns {Promise<void>} Registra intención sin obtener la lista ni consultar al proveedor. */
          () => spec.open(query, identity, entry.receiptId),
        );
        await entry.openTask;
        if (entry.window.closed) throw failure(409);
      }
      await this.receipts.put({
        receiptId      : entry.receiptId,
        owner,
        query,
        queryFingerprint,
        subscriptionId : request.subscriptionId,
        watchVersion   : request.watchVersion,
        topic          : request.topic,
        section        : request.section,
        state          : 'registered',
        purpose        : request.type === 'resync' ? 'recovery' : 'explicit',
        expiresAt      : new Date(this.clock() + Math.max(120000, (spec.readTimeoutMs || 0) + 15000)),
      });
    } catch (error) {
      entry.window.close();
      if (session.windows.get(request.subscriptionId) === entry)
        session.windows.delete(request.subscriptionId);
      throw error;
    }
    if (
      this.sessions.get(session.socket.id) !== session ||
      session.windows.get(request.subscriptionId) !== entry
    )
      throw failure(409);
    return {
      ...entry.window.ready(),
      queryFingerprint,
      receiptId : entry.receiptId,
    };
  }

  /** @param {string} socketId Conexión vigente. @returns {Promise<void>} Renueva reservas técnicas por heartbeat; no programa sondeos ni solicita snapshots. */
  async heartbeat(socketId) {
    const session = this.sessions.get(socketId);
    if (!session) return;
    const tasks = [];
    let authentication;
    /** @returns {Promise<Object>} Comparte sólo verificaciones simultáneas de este heartbeat; una apertura tardía vuelve a Security. */
    const verify = () => {
      if (!authentication)
        authentication = this.measure(
          'authorization',
          /** @returns {Promise<Object>} Identidad vigente del mismo contexto autenticado. */
          () => session.verify(),
        ).finally(
          /** @returns {void} No conserva permisos después de terminar la consulta. */
          () => {
            authentication = undefined;
          },
        );
      return authentication;
    };
    for (const entry of session.windows.values()) {
      const spec = this.topics[entry.window.topic]?.[entry.window.section];
      if (!entry.openTask || entry.window.closed || entry.renewing || !spec?.renew) continue;
      entry.renewing = true;
      tasks.push(
        (
          /** @returns {Promise<void>} Revalida antes de renovar y descarta una conexión sustituida. */
          async () => {
            try {
              await entry.openTask;
              const identity = await verify();
              if (audience(identity) !== entry.owner) throw failure(401);
              if (entry.window.closed || this.sessions.get(socketId) !== session) return;
              this.record('lease-renewal');
              await spec.renew(entry.receiptId, identity);
            } catch (error) {
              if (entry.window.closed) return;
              entry.window.send(
                [401, 403].includes(error.statusCode)
                  ? 'revoked'
                  : error.statusCode === 409
                    ? 'resync-required'
                    : 'error',
                { reason: 'LEASE_UNAVAILABLE' },
              );
              entry.window.close();
            } finally {
              entry.renewing = false;
            }
          }
        )(),
      );
    }
    await Promise.all(tasks);
  }

  /** @param {string} topic Dominio productor. @param {Object} change Referencia durable. @returns {Promise<void>} Recalcula únicamente las consultas afectadas por un cambio real. */
  async changed(topic, change) {
    const tasks = [];
    for (const session of this.sessions.values()) {
      for (const entry of session.windows.values()) {
        const window = entry.window;
        const spec   = this.topics[topic]?.[window.section];
        if (
          window.topic === topic &&
          spec?.affects(
            entry.query,
            change,
            entry.identity,
            window.task || window.dirty ? undefined : window.base,
            { receiptId: entry.receiptId },
          )
        )
          tasks.push(window.change(change));
      }
    }
    await Promise.all(tasks);
  }

  /** @param {Object} change Referencia de acceso validada. @param {Function} matches Selección del principal afectado. @returns {Promise<void>} Reacredita únicamente las ventanas de ese principal antes de volver a proyectarlas. */
  async reauthorize(change, matches) {
    const tasks = [];
    for (const session of this.sessions.values())
      for (const entry of session.windows.values())
        if (matches(entry.identity, change)) tasks.push(entry.window.change(change));
    await Promise.all(tasks);
  }

  /** @param {string} socketId Conexión revocada. @returns {void} Ordena retirar datos antes de cerrar la asociación de transporte. */
  revoke(socketId) {
    const session = this.sessions.get(socketId);
    if (!session) return;
    for (const entry of session.windows.values())
      entry.window.send('revoked', { reason: 'ACCESS_REVOKED' });
    this.reset(socketId);
  }

  /** @returns {void} Informa indisponibilidad sin provocar lecturas durante la caída; recover confirma después la discontinuidad. */
  unavailable() {
    for (const session of this.sessions.values())
      for (const entry of session.windows.values()) {
        entry.window.send('error', { reason: 'TEMPORARILY_UNAVAILABLE' });
        entry.window.close();
      }
  }

  /** @returns {void} Una discontinuidad exige recuperación HTTP de cada ventana activa. */
  recover() {
    for (const session of this.sessions.values())
      for (const entry of session.windows.values()) {
        entry.window.send('resync-required', { reason: 'CONTINUITY_LOST' });
        entry.window.close();
      }
  }

  /** @param {string} socketId Conexión sustituida. @returns {void} Descarta sus ventanas y lecturas en curso. */
  reset(socketId) {
    const session = this.sessions.get(socketId);
    if (!session) return;
    for (const entry of session.windows.values()) entry.window.close();
    this.sessions.delete(socketId);
  }

  /** @returns {void} Libera exclusivamente las ventanas de este servicio. */
  close() {
    for (const socketId of this.sessions.keys()) this.reset(socketId);
  }
}
module.exports = { RealtimeWindows };
