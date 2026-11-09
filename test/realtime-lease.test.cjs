/** @file Reservas de consultas activas: heartbeat técnico, cancelación durante apertura y revocación aislada. */
'use strict';
const { produceSnapshot } = require('../server/library/realtime/snapshot');
const test                = require('node:test');
const assert              = require('node:assert/strict');
const { EventEmitter }    = require('node:events');
const { RealtimeWindows } = require('../server/library/realtime/windows');

/** @param {Object} overrides Puertos controlados de apertura y renovación. @returns {Object} Registro real con transporte y recibos en memoria. */
function fixture(overrides = {}) {
  const saved  = new Map(),
    calls      = [],
    deliveries = [],
    events     = new EventEmitter();
  const identity = {
    userCode    : 'u',
    sessionCode : 'session',
    workspace   : 'w',
    context     : {},
  };
  const socket   = {
    id   : 'socket',
    conn : new EventEmitter(),
    on   : events.on.bind(events),
    once : events.once.bind(events),
    /** @param {string} channel Canal. @param {Object} data Control o patch. @returns {void} Conserva las entregas sin devolverlas al listener de control. */
    emit(channel, data) {
      deliveries.push({
        channel,
        data,
      });
    },
  };
  const windows  = new RealtimeWindows({
    maxSubscriptions : overrides.maxSubscriptions,
    observe          : overrides.observe,
    /** @returns {number} Reloj del productor para plazos reproducibles. */
    clock            : () => 1000,
    receipts         : {
      /** @param {Object} value Recibo. @returns {Promise<void>} Reserva correlacionada. */
      async put(value) {
        saved.set(value.receiptId, value);
      },
      /** @param {string} id Identidad técnica. @returns {Promise<Object>} Recibo registrado. */
      async get(id) {
        return saved.get(id);
      },
      /** @param {Object} value Base HTTP. @returns {Promise<boolean>} Confirma una lectura explícita. */
      async complete(value) {
        saved.set(value.receiptId, {
          ...value,
          state : 'ready',
        });
        return true;
      },
    },
    topics           : {
      'test.query' : {
        search : {
          readTimeoutMs : 125000,
          /** @param {Object} value Consulta. @returns {Object} Consulta sin datos de lista. */
          validate(value) {
            return value;
          },
          /** @param {Object} query Consulta. @param {Object} actor Principal. @param {string} receiptId Identidad del servidor. @returns {Promise<void>} Registra sólo intención. */
          async open(query, actor, receiptId) {
            calls.push(['open', receiptId]);
            await overrides.open?.();
          },
          /** @param {string} receiptId Reserva. @param {Object} actor Principal vigente. @returns {Promise<void>} Renueva metadatos técnicos. */
          async renew(receiptId, actor) {
            calls.push(['renew', receiptId]);
            await overrides.renew?.();
          },
          /** @param {string} receiptId Reserva. @returns {Promise<void>} Libera la intención propia. */
          async close(receiptId) {
            calls.push(['close', receiptId]);
          },
          /** @param {Object} query Consulta. @param {Object} actor Contexto. @param {Object} control Correlación privada. @returns {Promise<Object>} Sólo HTTP obtiene la base. */
          async read(query, actor, control) {
            calls.push(['read', control.receiptId]);
            return produceSnapshot(overrides.read ? await overrides.read() : { items: [] });
          },
        },
      },
    },
  });
  windows.authenticate(
    socket,
    /** @returns {Promise<Object>} Principal acreditado. */
    async () => {
      await overrides.verify?.();
      return identity;
    },
  );
  const session   = windows.sessions.get(socket.id);
  /** @param {string} subscriptionId Identificador local. @returns {Promise<Object>} Solicitud estándar con generación explícita. */
  const subscribe = (subscriptionId) =>
    windows.control(session, {
      type         : 'subscribe',
      subscriptionId,
      watchVersion : 1,
      topic        : 'test.query',
      section      : 'search',
      query        : { search: 'body' },
    });
  return {
    windows,
    saved,
    calls,
    deliveries,
    events,
    socket,
    session,
    subscribe,
    identity,
  };
}

test('el ACK y los heartbeats no leen listas y el cierre libera exactamente su reserva' /** @returns {Promise<void>} Comprueba el cableado Engine.IO y el recibo compartido con HTTP. */, async () => {
  let markRenewed;
  const renewed = new Promise(
    /** @param {Function} resolve Señal de renovación. @returns {void} Conserva la barrera. */ (
      resolve,
    ) => {
      markRenewed = resolve;
    },
  );
  const f       = fixture({
    /** @returns {void} Confirma la renovación real. */ renew() {
      markRenewed();
    },
  });
  const ack     = await f.subscribe('one');
  assert.equal('data' in ack, false);
  assert.equal(f.saved.get(ack.receiptId).expiresAt.getTime(), 141000);
  const entry = f.session.windows.get('one');
  f.socket.conn.emit('heartbeat');
  await f.windows.heartbeat('socket');
  await renewed;
  assert.deepEqual(f.calls, [
    ['open', ack.receiptId],
    ['renew', ack.receiptId],
  ]);
  await f.windows.read(
    {
      ...ack,
      topic   : 'test.query',
      section : 'search',
      query   : { search: 'body' },
    },
    f.identity,
  );
  assert.deepEqual(f.calls.at(-1), ['read', ack.receiptId]);
  await f.windows.control(f.session, {
    type           : 'unsubscribe',
    subscriptionId : 'one',
    watchVersion   : 1,
    topic          : 'test.query',
    section        : 'search',
  });
  await entry.releasing;
  assert.deepEqual(f.calls.at(-1), ['close', ack.receiptId]);
  await f.windows.heartbeat('socket');
  assert.equal(f.calls.length, 4);
  f.windows.close();
  f.events.emit('disconnect');
  assert.equal(f.socket.conn.listenerCount('heartbeat'), 0);
});

test('las ventanas del mismo heartbeat comparten sólo la acreditación en vuelo', /** @returns {Promise<void>} Tres reservas renuevan con una consulta fresca, sin cachearla para el heartbeat siguiente. */ async () => {
  let count = 0,
    blocked = false,
    release,
    started;
  const hold    = new Promise(
    /** @param {Function} resolve Libera Security. @returns {void} Conserva la barrera. */ (
      resolve,
    ) => {
      release = resolve;
    },
  );
  const reached = new Promise(
    /** @param {Function} resolve Confirma el inicio. @returns {void} Conserva la señal. */ (
      resolve,
    ) => {
      started = resolve;
    },
  );
  const f       = fixture({
    /** @returns {Promise<void>} Cuenta consultas reales y retiene únicamente la primera renovación. */ async verify() {
      count++;
      if (blocked) {
        started();
        await hold;
      }
    },
  });
  try {
    for (const id of ['one', 'two', 'three']) await f.subscribe(id);
    count = 0;
    blocked = true;
    const work = f.windows.heartbeat('socket');
    await reached;
    assert.equal(count, 1);
    release();
    await work;
    blocked = false;
    assert.equal(
      f.calls.filter(
        /** @param {Array} call Efecto. @returns {boolean} Es renovación. */ (call) =>
          call[0] === 'renew',
      ).length,
      3,
    );
    await f.windows.heartbeat('socket');
    assert.equal(count, 2);
  } finally {
    release();
    f.windows.close();
  }
});

test('una apertura tardía reacredita después de una revocación y no reutiliza permisos resueltos', /** @returns {Promise<void>} Una reserva sana no oculta el rechazo posterior de otra. */ async () => {
  let count = 0,
    revoked = false,
    releaseLate,
    firstRenewed;
  const late    = new Promise(
    /** @param {Function} resolve Finaliza la apertura. @returns {void} Conserva la barrera. */ (
      resolve,
    ) => {
      releaseLate = resolve;
    },
  );
  const renewed = new Promise(
    /** @param {Function} resolve Confirma la primera reserva. @returns {void} Conserva la señal. */ (
      resolve,
    ) => {
      firstRenewed = resolve;
    },
  );
  const f       = fixture({
    /** @returns {void} Rechaza una verificación iniciada después de la revocación. */ verify() {
      count++;
      if (revoked) throw Object.assign(new Error('REVOKED'), { statusCode: 401 });
    },
    /** @returns {void} Confirma que terminó la primera acreditación. */ renew() {
      firstRenewed();
    },
  });
  try {
    await f.subscribe('one');
    await f.subscribe('two');
    count = 0;
    f.session.windows.get('two').openTask = late;
    const work = f.windows.heartbeat('socket');
    await renewed;
    revoked = true;
    releaseLate();
    await work;
    assert.equal(count, 2);
    assert.equal(
      f.calls.filter(
        /** @param {Array} call Efecto. @returns {boolean} Es renovación. */ (call) =>
          call[0] === 'renew',
      ).length,
      1,
    );
    assert.equal(f.deliveries.at(-1).data.type, 'revoked');
    assert.equal(f.deliveries.at(-1).data.subscriptionId, 'two');
    assert.equal(f.session.windows.get('one').window.closed, false);
  } finally {
    releaseLate();
    f.windows.close();
  }
});

test('cancelar durante la apertura espera el resultado antes de liberar y no confirma el ACK' /** @returns {Promise<void>} Una apertura tardía no deja un lease huérfano. */, async () => {
  let release;
  const hold = new Promise(
    /** @param {Function} resolve Apertura pendiente. @returns {void} Conserva el control de la carrera. */
    (resolve) => {
      release = resolve;
    },
  );
  let markOpened;
  const opened  = new Promise(
    /** @param {Function} resolve Señal de apertura. @returns {void} Conserva la barrera. */ (
      resolve,
    ) => {
      markOpened = resolve;
    },
  );
  const f       = fixture({
    /** @returns {Promise<void>} Mantiene la escritura del productor en vuelo. */
    open() {
      markOpened();
      return hold;
    },
  });
  const pending = f.subscribe('one');
  await opened;
  const entry = f.session.windows.get('one');
  f.windows.reset('socket');
  release();
  await assert.rejects(pending, { statusCode: 409 });
  await entry.releasing;
  assert.deepEqual(
    f.calls.map(
      /** @param {Array} call Operación. @returns {string} Orden observado. */
      (call) => call[0],
    ),
    ['open', 'close'],
  );
  assert.deepEqual(f.deliveries, []);
});

test('un lease revocado retira datos sin lecturas ni temporizadores de recuperación' /** @returns {Promise<void>} El control de acceso conserva la misma semántica del canal estándar. */, async () => {
  const f = fixture({
    /** @returns {Promise<void>} Rechazo del productor con permisos vigentes. */
    async renew() {
      throw Object.assign(new Error('FORBIDDEN'), { statusCode: 403 });
    },
  });
  await f.subscribe('one');
  const entry = f.session.windows.get('one');
  await f.windows.heartbeat('socket');
  await entry.releasing;
  assert.equal(f.deliveries[0].channel, 'realtime');
  assert.equal(f.deliveries[0].data.type, 'revoked');
  assert.equal(entry.window.closed, true);
  assert.equal(
    f.calls.some(
      /** @param {Array} call Operación. @returns {boolean} Detecta una consulta no autorizada. */
      (call) => call[0] === 'read',
    ),
    false,
  );
  f.windows.close();
});

test('telemetría separa HTTP explícito, recuperación, lectura por hecho y heartbeat', /** @returns {Promise<void>} El ACK no cuenta como lista y ningún evento observado incluye identidad o consulta. */ async () => {
  const operations = [];
  let items        = [];
  const f          = fixture({
    /** @param {string} operation Clasificación cerrada. @returns {void} Registra sin acceso al contenido. */
    observe(operation, duration) {
      if (duration === undefined) operations.push(operation);
    },
    /** @returns {Object} Proyección del productor. */
    read() {
      return { items };
    },
  });
  try {
    const ack = await f.subscribe('one');
    assert.deepEqual(operations, []);
    const query = {
      ...ack,
      topic   : 'test.query',
      section : 'search',
      query   : { search: 'PRIVATE' },
    };
    query.query = { search: 'body' };
    await f.windows.read(query, f.identity);
    await f.windows.control(f.session, {
      ...query,
      type : 'bind',
    });
    items = [{ id: 'PRIVATE' }];
    await f.session.windows.get('one').window.change({});
    await f.windows.heartbeat('socket');
    const next = await f.windows.control(f.session, {
      ...query,
      type         : 'resync',
      watchVersion : 2,
    });
    await f.windows.read(
      {
        ...query,
        ...next,
      },
      f.identity,
    );
    assert.deepEqual(operations, [
      'http-read',
      'window-read',
      'patch',
      'lease-renewal',
      'http-recovery',
    ]);
    /** @returns {never} Un observador averiado no afecta permisos, lectura ni recuperación. */
    f.windows.observe = () => {
      throw new Error('PRIVATE');
    };
    const third = await f.windows.control(f.session, {
      ...query,
      type         : 'subscribe',
      watchVersion : 3,
    });
    await f.windows.read(
      {
        ...query,
        ...third,
      },
      f.identity,
    );
  } finally {
    f.windows.close();
  }
});

test('una respuesta sin cursor del productor no puede convertirse en base HTTP' /** @returns {Promise<void>} El coordinador nunca fabrica la consistencia de un productor incompleto. */, async () => {
  const f = fixture();
  /** @returns {Promise<Object>} Productor que incumple el puerto de consistencia. */
  f.windows.topics['test.query'].search.read = async () => ({ items: [] });
  try {
    const ack = await f.subscribe('missing');
    await assert.rejects(
      f.windows.read(
        {
          ...ack,
          topic   : 'test.query',
          section : 'search',
          query   : { search: 'body' },
        },
        f.identity,
      ),
      /PRODUCER_CURSOR_REQUIRED/,
    );
    assert.equal(f.saved.get(ack.receiptId).state, 'registered');
    assert.equal(f.deliveries.length, 0);
  } finally {
    f.windows.close();
  }
});

test('el presupuesto de 128 ventanas admite columnas y referencias, rechaza el exceso sin cerrar otras secciones' /** @returns {Promise<void>} Las suscripciones sólo reservan control; liberar una permite abrir otra sin consultas. */, async () => {
  const f = fixture({ maxSubscriptions: 128 });
  try {
    for (let index = 0; index < 128; index++) await f.subscribe('window-' + index);
    await assert.rejects(f.subscribe('overflow'), { statusCode: 429 });
    assert.equal(f.session.windows.size, 128);
    assert.equal(
      f.calls.filter(
        /** @param {Array} call Acción técnica. @returns {boolean} Lectura comercial. */
        (call) => call[0] === 'read',
      ).length,
      0,
    );
    await f.windows.control(f.session, {
      type           : 'unsubscribe',
      subscriptionId : 'window-0',
      watchVersion   : 1,
      topic          : 'test.query',
      section        : 'search',
    });
    await f.subscribe('replacement');
    assert.equal(f.session.windows.size, 128);
    assert.ok(f.session.windows.has('window-127'));
    assert.equal(f.deliveries.length, 0);
  } finally {
    f.windows.close();
  }
});

test('bind revalida una vez tras el recibo y rechaza revocación sin emitir datos', /** @returns {Promise<void>} Conserva la cerca de autorización posterior al I/O. */ async () => {
  let verifications = 0;
  let revoked       = false;
  const f           = fixture({
    /** @returns {void} Simula el estado actual de Security. */
    verify() {
      verifications++;
      if (revoked) throw Object.assign(new Error('REVOKED'), { statusCode: 401 });
    },
  });
  try {
    const ack   = await f.subscribe('one');
    const query = {
      ...ack,
      topic   : 'test.query',
      section : 'search',
      query   : { search: 'body' },
    };
    await f.windows.read(query, f.identity);
    assert.equal(verifications, 1);
    await f.windows.control(f.session, {
      ...query,
      type : 'bind',
    });
    assert.equal(verifications, 2);
    const second = await f.subscribe('two');
    const next   = {
      ...query,
      ...second,
    };
    await f.windows.read(next, f.identity);
    const get = f.windows.receipts.get;
    /** @param {string} id Recibo. @param {string} owner Propietario. @returns {Promise<Object>} Revoca durante el I/O del recibo. */
    f.windows.receipts.get = async (id, owner) => {
      const receipt = await get(id, owner);
      revoked = true;
      return receipt;
    };
    await assert.rejects(
      f.windows.control(f.session, {
        ...next,
        type : 'bind',
      }),
      { statusCode: 401 },
    );
    assert.equal(f.session.windows.get('two').window.bound, false);
    assert.deepEqual(f.deliveries, []);
  } finally {
    f.windows.close();
  }
});
