/** @file Comprueba el transporte real de Platform: una identidad por conexión y revocación por eventos. */
'use strict';
const test                        = require('node:test');
const assert                      = require('node:assert/strict');
const { EventEmitter }            = require('node:events');
const { RealtimeWindows }         = require('../server/library/realtime/windows');
const { produceSnapshot }         = require('../server/library/realtime/snapshot');
const { createDocumentsRealtime } = require('../server/adapters/socket/documents-realtime');
const { registerSocketEvents }    = require('../server/adapters/socket/socket');

/** @returns {Object} Promesa controlada para carreras, sin temporizadores ni red. */
function deferred() {
  let resolve;
  const promise = new Promise(
    /** @param {Function} done Finalización. @returns {void} Expone la continuación de prueba. */ (
      done,
    ) => {
      resolve = done;
    },
  );
  return {
    promise,
    resolve,
  };
}

/** @returns {Object} Socket, Gateway de pruebas y coordinadores reales con Security sustituido. */
function fixture() {
  const events = new EventEmitter(),
    saved      = new Map(),
    deliveries = [];
  const h         = {
    calls        : 0,
    status       : 200,
    readHook     : null,
    securityHook : null,
    value        : 1,
    identity     : {
      userCode                : 'U1',
      sessionCode             : 'S1',
      activeWorkspace         : 'W1',
      groups                  : ['G1'],
      workspaces              : ['W1'],
      policies                : ['directory.contact.view.workspace'],
      expiration              : Date.now() / 1000 + 600,
      currentPoliciesVerified : true,
    },
  };
  const messaging = {
    /** @param {string} service Destino. @param {string} action Acción. @param {Object} data Datos. @returns {Promise<Object>} Cuenta exclusivamente autenticaciones remotas. */
    async request(service, action, data) {
      assert.equal(service, 'service-security');
      assert.equal(action, 'session-authenticate');
      assert.equal(data.currentPolicies, true);
      h.calls++;
      await h.securityHook?.();
      return {
        statusCode : h.status,
        data       : structuredClone(h.identity),
      };
    },
  };
  const windows   = new RealtimeWindows({
    receipts : {
      /** @param {Object} receipt Reserva. @returns {Promise<void>} Guarda recibo privado. */
      async put(receipt) {
        saved.set(receipt.receiptId, receipt);
      },
      /** @param {string} id Recibo. @param {string} owner Identidad. @returns {Promise<Object>} Recibo del propietario. */
      async get(id, owner) {
        const r = saved.get(id);
        return r?.owner === owner ? r : null;
      },
      /** @param {Object} receipt Base HTTP. @returns {Promise<boolean>} Vincula el resultado del productor. */
      async complete(receipt) {
        saved.set(receipt.receiptId, {
          ...receipt,
          state : 'ready',
        });
        return true;
      },
    },
    topics   : {
      'directory.contact' : {
        page : {
          collections : { '/items': 'code' },
          /** @param {Object} query Consulta. @returns {Object} Misma selección. */
          validate(query) {
            return query;
          },
          /** @returns {boolean} El hecho sintético afecta las ventanas abiertas. */
          affects() {
            return true;
          },
          /** @returns {Promise<Object>} Página de cincuenta registros acreditados. */
          async read() {
            const value = h.value;
            await h.readHook?.();
            return produceSnapshot({
              items : Array.from(
                { length: 50 },
                /** @param {*} ignored Vacío. @param {number} i Posición. @returns {Object} Registro sintético. */ (
                  _,
                  i,
                ) => ({
                  code : String(i),
                  value,
                }),
              ),
            });
          },
        },
      },
    },
  });
  const documents = createDocumentsRealtime({
    messaging,
    /** @returns {Object} Coordinador real. */ windows                 : () => windows,
    /** @returns {Object} Este escenario no abre manifiestos. */ reads : () => ({}),
  });
  messaging.documentsRealtime = documents;
  let connected;
  registerSocketEvents({
    messaging,
    io : {
      /** @param {string} event Conexión. @param {Function} callback Entrada del socket. @returns {void} Captura. */
      on(event, callback) {
        connected = callback;
      },
    },
  });
  const socket = {
    id        : 'web',
    data      : {},
    rooms     : new Set(['web']),
    handshake : { auth: {} },
    conn      : new EventEmitter(),
    on        : events.on.bind(events),
    once      : events.once.bind(events),
    /** @param {string} name Sala. @returns {Promise<void>} Incorpora. */ async join(name) {
      this.rooms.add(name);
    },
    /** @param {string} name Sala. @returns {Promise<void>} Retira. */ async leave(name) {
      this.rooms.delete(name);
    },
    /** @param {string} channel Canal. @param {Object} data Entrega. @returns {void} Conserva sólo resultados. */
    emit(channel, data) {
      assert.equal(channel, 'realtime');
      deliveries.push(data);
    },
    /** @param {string} event Entrada. @param {Object} value Cuerpo. @returns {Promise<Object>} Acuse completo. */
    async request(event, value) {
      let result;
      for (const listener of events.listeners(event))
        await listener(
          value,
          /** @param {Object} ack Acuse. @returns {void} Guarda. */ (ack) => {
            result = ack;
          },
        );
      return result;
    },
  };
  connected(socket);
  Object.assign(h, {
    socket,
    documents,
    windows,
    deliveries,
    /** @returns {Promise<Object>} Acredita la sesión antes de suscribir. */
    start() {
      return socket.request('start-session', { token: 'fixture' });
    },
    /** @param {string} id Ventana. @returns {Promise<Object>} Subscribe, HTTP y bind del contrato real. */
    async read(id) {
      const ref = {
        subscriptionId : id,
        watchVersion   : 1,
        topic          : 'directory.contact',
        section        : 'page',
      };
      const ack = await socket.request('realtime', {
        ...ref,
        type      : 'subscribe',
        requestId : id + '-subscribe',
        query     : {},
      });
      assert.equal(ack.statusCode, 200);
      assert.equal(ack.data, undefined);
      const identity = await windows.sessions.get(socket.id).verify();
      const result   = await windows.read(
        {
          ...ref,
          query     : {},
          receiptId : ack.receiptId,
        },
        identity,
      );
      const bound    = await socket.request('realtime', {
        ...ref,
        type      : 'bind',
        requestId : id + '-bind',
        receiptId : ack.receiptId,
      });
      assert.equal(bound.statusCode, 200);
      return result;
    },
    /** @returns {Promise<void>} Libera timers, identidad y ventanas. */
    async close() {
      await socket.request('disconnect');
      documents.close();
    },
  });
  return h;
}

test('cincuenta registros, varias ventanas, patches y heartbeat usan una sola autenticación', /** @returns {Promise<void>} El número de filas y entregas no multiplica RPC de Security. */ async () => {
  const h = fixture();
  try {
    assert.equal((await h.start()).statusCode, 200);
    assert.equal((await h.read('first')).data.items.length, 50);
    await h.read('second');
    h.value++;
    await h.windows.changed('directory.contact', {});
    await h.windows.heartbeat(h.socket.id);
    assert.equal(h.calls, 1);
    assert.equal(
      h.deliveries.filter(
        /** @param {Object} d Entrega. @returns {boolean} Diferencia. */ (d) => d.type === 'patch',
      ).length,
      2,
    );
    await h.documents.accessChanged({
      resource : 'user',
      code     : 'other',
    });
    assert.equal(h.calls, 1);
    await h.documents.accessChanged({
      resource : 'group',
      code     : 'G1',
    });
    assert.equal(h.calls, 2);
    h.identity.policies = [];
    await h.documents.accessChanged({
      resource : 'group',
      code     : 'G1',
    });
    assert.equal(h.calls, 3);
    assert.equal(
      h.deliveries.filter(
        /** @param {Object} d Entrega. @returns {boolean} Revocación. */ (d) =>
          d.type === 'revoked',
      ).length,
      2,
    );
  } finally {
    await h.close();
  }
});

test('un cambio de acceso durante la consulta impide publicar su resultado antiguo', /** @returns {Promise<void>} La segunda cerca es local, pero observa la invalidación real. */ async () => {
  const h   = fixture(),
    entered = deferred(),
    release = deferred();
  try {
    await h.start();
    await h.read('first');
    /** @returns {Promise<void>} Detiene la lectura mientras cambian los permisos. */
    h.readHook = async () => {
      entered.resolve();
      await release.promise;
    };
    h.value++;
    const changing = h.windows.changed('directory.contact', {});
    await entered.promise;
    h.identity.policies = [];
    const revoked = h.documents.accessChanged({
      resource : 'group',
      code     : 'G1',
    });
    release.resolve();
    await Promise.all([changing, revoked]);
    assert.equal(h.calls, 2);
    assert.equal(
      h.deliveries.some(
        /** @param {Object} d Entrega. @returns {boolean} Datos antiguos. */ (d) =>
          d.type === 'patch',
      ),
      false,
    );
    assert.ok(
      h.deliveries.some(
        /** @param {Object} d Entrega. @returns {boolean} Revocación. */ (d) =>
          d.type === 'revoked',
      ),
    );
  } finally {
    await h.close();
  }
});

test('fallo de Security bloquea el contexto anterior y la recuperación reacredita una vez', /** @returns {Promise<void>} Ni controles nuevos ni recuperación reutilizan autorizaciones inciertas. */ async () => {
  const h = fixture();
  try {
    await h.start();
    await h.read('first');
    h.status = 503;
    await assert.rejects(
      h.documents.accessChanged({
        resource : 'session',
        code     : 'S1',
      }),
      { statusCode: 503 },
    );
    await assert.rejects(h.windows.sessions.get(h.socket.id).verify(), { statusCode: 503 });
    h.documents.unavailable();
    assert.equal((await h.start()).statusCode, 503);
    h.status = 200;
    await h.documents.recover();
    await h.start();
    await h.read('after');
    assert.equal(h.calls, 3);
  } finally {
    await h.close();
  }
});

test('una discontinuidad reacredita conexiones sin ventanas y descarta un handshake cruzado', /** @returns {Promise<void>} No pierde una revocación mientras se abre la sesión. */ async () => {
  const h   = fixture(),
    entered = deferred(),
    release = deferred();
  try {
    await h.start();
    h.documents.unavailable();
    await h.documents.recover();
    assert.equal(h.calls, 2);
    /** @returns {Promise<void>} Detiene el handshake antes de instalar su resultado. */
    h.securityHook = async () => {
      entered.resolve();
      await release.promise;
    };
    const start = h.start();
    await entered.promise;
    await h.documents.accessChanged({
      resource : 'user',
      code     : 'U1',
    });
    release.resolve();
    assert.equal((await start).statusCode, 503);
    assert.equal(h.windows.sessions.has(h.socket.id), false);
  } finally {
    await h.close();
  }
});
