/** @file Comprueba carreras HTTP/eventos, deltas por ítem y revocaciones del contrato común. */
'use strict';
const { produceSnapshot }            = require('../server/library/realtime/snapshot');
const test                           = require('node:test');
const assert                         = require('node:assert/strict');
const { ReadWindow }                 = require('../server/library/realtime/read-window');
const { windowDelta, contentCursor } = require('../server/library/realtime/window-delta');

test('la diferencia conserva las filas ajenas y expresa entradas, salidas y orden' /** @returns {void} Comprueba una ventana paginada que cambia en ambos bordes. */, () => {
  const before = {
    items : [
      {
        code  : 'a',
        title : 'A',
      },
      {
        code  : 'b',
        title : 'B',
      },
      {
        code  : 'c',
        title : 'C',
      },
    ],
    total : 3,
  };
  const after  = {
    items : [
      {
        code  : 'c',
        title : 'C',
      },
      {
        code  : 'b',
        title : 'Nuevo',
      },
      {
        code  : 'd',
        title : 'D',
      },
    ],
    total : 4,
  };
  assert.deepEqual(windowDelta(before, after, { '/items': 'code' }), [
    {
      op   : 'remove',
      path : '/items/0',
    },
    {
      op   : 'move',
      from : '/items/1',
      path : '/items/0',
    },
    {
      op    : 'replace',
      path  : '/items/1/title',
      value : 'Nuevo',
    },
    {
      op    : 'add',
      path  : '/items/2',
      value : {
        code  : 'd',
        title : 'D',
      },
    },
    {
      op    : 'replace',
      path  : '/total',
      value : 4,
    },
  ]);
  assert.equal(before.items[0].code, 'a');
  assert.equal(
    contentCursor({
      total : 3,
      items : before.items,
    }),
    contentCursor(before),
  );
});

test('suscribirse no consulta ni entrega listas; un cambio durante HTTP se aplica después de vincular' /** @returns {Promise<void>} Simula una escritura confirmada mientras la respuesta HTTP está en vuelo. */, async () => {
  const sent   = [];
  let reads    = 0;
  const base   = {
    items : [
      {
        code  : 'a',
        title : 'Anterior',
      },
    ],
    total : 1,
  };
  const latest = {
    items : [
      {
        code  : 'a',
        title : 'Actual',
      },
    ],
    total : 1,
  };
  const window = new ReadWindow({
    subscriptionId : 'list',
    watchVersion   : 1,
    topic          : 'test.items',
    section        : 'list',
    collections    : { '/items': 'code' },
    /** @returns {Promise<void>} Autoriza la identidad vigente. */
    async authorize() {},
    /** @returns {Promise<Object>} Consulta canónica activada por un cambio real. */
    async read() {
      reads++;
      return produceSnapshot(latest);
    },
    /** @param {Object} value Sobre entregado. @returns {number} Conserva evidencia del canal. */
    emit           : (value) => sent.push(value),
  });
  assert.deepEqual(window.ready(), {
    statusCode     : 200,
    subscriptionId : 'list',
    watchVersion   : 1,
  });
  await window.change();
  assert.equal(reads, 0);
  assert.equal(sent.length, 0);
  await window.bind({
    subscriptionId : 'list',
    watchVersion   : 1,
    data           : base,
    cursor         : contentCursor(base),
    projectionHash : contentCursor(base),
    query          : { page: 1 },
  });
  assert.equal(reads, 1);
  assert.equal(sent.length, 1);
  assert.equal(sent[0].type, 'patch');
  assert.equal(sent[0].data.baseCursor, contentCursor(base));
  assert.deepEqual(sent[0].data.operations, [
    {
      op    : 'replace',
      path  : '/items/0/title',
      value : 'Actual',
    },
  ]);
  await window.change();
  assert.equal(sent.length, 1, 'Un evento duplicado no genera otro cambio visible');
  window.close();
});

test('una lectura tardía no entrega datos después de sustituir o revocar la suscripción' /** @returns {Promise<void>} Verifica la cerca posterior a I/O. */, async () => {
  const sent = [];
  let finish;
  const blocked = new Promise(
    /** @param {Function} resolve Libera el lector. @returns {void} Conserva el control de concurrencia. */
    (resolve) => {
      finish = resolve;
    },
  );
  const window  = new ReadWindow({
    subscriptionId : 'detail',
    watchVersion   : 2,
    topic          : 'test.items',
    section        : 'detail',
    /** @returns {Promise<void>} Autoriza la lectura. */
    async authorize() {},
    /** @returns {Promise<Object>} Mantiene una respuesta en vuelo. */
    read           : () => blocked,
    /** @param {Object} value Sobre. @returns {number} Cuenta entregas. */
    emit           : (value) => sent.push(value),
  });
  const data    = { item: { code: 'a' } };
  await window.bind({
    subscriptionId : 'detail',
    watchVersion   : 2,
    data,
    cursor         : contentCursor(data),
    projectionHash : contentCursor(data),
    query          : { code: 'a' },
  });
  const work = window.change();
  await Promise.resolve();
  window.close();
  finish(produceSnapshot({ item: { code: 'secret' } }));
  await work;
  assert.deepEqual(sent, []);
});

test('un desbordamiento solicita recuperación HTTP sin enviar una lista por socket' /** @returns {Promise<void>} Mantiene acotado el canal y conserva la base del cliente. */, async () => {
  const sent   = [];
  const window = new ReadWindow({
    subscriptionId : 'list',
    watchVersion   : 1,
    topic          : 'test.items',
    section        : 'list',
    maxOperations  : 1,
    collections    : { '/items': 'code' },
    /** @returns {Promise<void>} Autoriza el contexto. */
    async authorize() {},
    /** @returns {Promise<Object>} Devuelve una ventana con varios cambios. */
    async read() {
      return produceSnapshot({
        items : [{ code: 'b' }, { code: 'c' }],
        total : 2,
      });
    },
    /** @param {Object} value Sobre. @returns {number} Registra el control. */
    emit           : (value) => sent.push(value),
  });
  const data   = {
    items : [],
    total : 0,
  };
  await window.bind({
    subscriptionId : 'list',
    watchVersion   : 1,
    data,
    cursor         : contentCursor(data),
    projectionHash : contentCursor(data),
    query          : {},
  });
  await window.change();
  assert.equal(sent.length, 1);
  assert.equal(sent[0].type, 'resync-required');
  assert.deepEqual(sent[0].data, { reason: 'WINDOW_OVERFLOW' });
  window.close();
});

test('la matriz conserva la identidad anidada al mover variantes y rechaza identidades duplicadas', /** @returns {void} Una fila movida conserva su precio y produce cambios puntuales. */ () => {
  const before = {
    items : [
      {
        variant : {
          code : 'a',
          name : 'A',
        },
        prices  : [],
      },
      {
        variant : {
          code : 'b',
          name : 'B',
        },
        prices  : [],
      },
    ],
  };
  const after  = {
    items : [
      {
        variant : {
          code : 'b',
          name : 'Nuevo',
        },
        prices  : [],
      },
      {
        variant : {
          code : 'a',
          name : 'A',
        },
        prices  : [],
      },
    ],
  };
  assert.deepEqual(windowDelta(before, after, { '/items': 'variant.code' }), [
    {
      op   : 'move',
      from : '/items/1',
      path : '/items/0',
    },
    {
      op    : 'replace',
      path  : '/items/0/variant/name',
      value : 'Nuevo',
    },
  ]);
  assert.throws(
    /** @returns {Array} Fila sin identidad. */ () =>
      windowDelta(before, { items: [{ variant: {} }] }, { '/items': 'variant.code' }),
    /INVALID_WINDOW_IDENTITY/,
  );
  assert.throws(
    /** @returns {Array} Identidad repetida. */ () =>
      windowDelta(
        before,
        { items: [after.items[0], after.items[0]] },
        { '/items': 'variant.code' },
      ),
    /INVALID_WINDOW_IDENTITY/,
  );
});
