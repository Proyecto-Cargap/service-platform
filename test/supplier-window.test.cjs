/** @file Supplier: ventanas autorizadas, composición de página y carrera entre HTTP y cambios reales. */
'use strict';
const test                = require('node:test');
const assert              = require('node:assert/strict');
const { supplierWindows } = require('../server/modules/realtime/services/supplier-window.service');
const { producerWindows } = require('./helpers/producer-window.cjs');
const { ReadWindow }      = require('../server/library/realtime/read-window');
const { contentCursor }   = require('../server/library/realtime/window-delta');
/** @returns {Object} Principal sintético con ámbito de trabajo. */
function identity() {
  return {
    userCode  : 'u',
    workspace : 'w',
    context   : {
      user      : { userCode: 'u' },
      workspace : 'w',
      policies  : ['supplier.supplier.view.workspace'],
    },
  };
}
/** @param {string} supplierCode Identidad. @returns {Object} Fila pública mínima conforme al productor. */
function row(supplierCode) {
  return {
    supplierCode,
    workspace    : 'w',
    userCreate   : 'u',
    userAssigned : 'u',
    revision     : 1,
    creationDate : 1,
    updateDate   : 1,
    erased       : false,
  };
}
/** @param {string} code Identidad. @param {number} total Cantidad autorizada. @returns {Object} Página exacta del productor. */
function page(code, total) {
  return {
    items      : [row(code)],
    pagination : {
      page    : 1,
      perPage : 1,
      total,
    },
  };
}
test('Supplier sólo consulta contratos canónicos y selecciona causas de pertenencia o referencia visibles', /** @returns {Promise<void>} Acredita autorización y límites antes de solicitar datos. */ async () => {
  const calls = [];
  const spec  = producerWindows(supplierWindows, {
    messaging : {
      /** @param {...*} args Lectura. @returns {Promise<Object>} Base autorizada. */
      async request(...args) {
        calls.push(args);
        return {
          statusCode : 200,
          data       : page('s', 1),
        };
      },
    },
  })['supplier.supplier'].page;
  const query = {
    page    : 1,
    perPage : 1,
  };
  for (const invalid of [
    {
      ...query,
      perPage : 0,
    },
    {
      ...query,
      perPage : 101,
    },
    {
      ...query,
      condition : {},
    },
    {
      ...query,
      fields : 'secret',
    },
    {
      ...query,
      page : '1',
    },
  ])
    assert.throws(
      /** @returns {Object} Consulta no admitida por el canal estándar. */ () =>
        spec.validate(invalid),
      { statusCode: 400 },
    );
  await spec.read(spec.validate(query), identity());
  assert.deepEqual(calls[0].slice(0, 3), ['service-supplier', 'supplier-get-list', query]);
  await assert.rejects(
    spec.read(query, {
      ...identity(),
      context : { policies: [] },
    }),
    { statusCode: 403 },
  );
  assert.equal(
    spec.affects(
      query,
      {
        item : {
          supplierCode : 's',
          workspace    : 'other',
        },
      },
      identity(),
      page('s', 1),
    ),
    true,
  );
  assert.equal(
    spec.affects(
      query,
      {
        item : {
          supplierCode : 'different',
          workspace    : 'other',
        },
      },
      identity(),
      page('s', 1),
    ),
    false,
  );
  assert.equal(
    spec.affects(
      query,
      {
        source   : 'directory',
        resource : 'contact',
        code     : 'c',
      },
      identity(),
      {
        items : [
          {
            ...row('s'),
            type        : 'person',
            contactCode : 'c',
          },
        ],
      },
    ),
    true,
  );
  assert.equal(
    spec.affects(
      query,
      {
        source   : 'directory',
        resource : 'contact',
        code     : 'other',
      },
      identity(),
      {
        items : [
          {
            ...row('s'),
            type        : 'person',
            contactCode : 'c',
          },
        ],
      },
    ),
    false,
  );
});
test('Supplier no entrega listas en subscribe y aplica cambios ocurridos antes de instalar la respuesta HTTP', /** @returns {Promise<void>} Mantiene composición y total mediante patch, sin lectura del cliente. */ async () => {
  let data = page('a', 1),
    reads  = 0;
  const sent = [],
    query    = {
      page    : 1,
      perPage : 1,
    },
    actor    = identity();
  const spec   = producerWindows(supplierWindows, {
    messaging : {
      /** @returns {Promise<Object>} El productor realiza la consulta autorizada. */ async request() {
        reads++;
        return {
          statusCode : 200,
          data       : structuredClone(data),
        };
      },
    },
  })['supplier.supplier'].page;
  const window = new ReadWindow({
    subscriptionId : 'suppliers',
    watchVersion   : 1,
    topic          : 'supplier.supplier',
    section        : 'page',
    collections    : spec.collections,
    /** @returns {Promise<void>} Principal ya revalidado por el transporte del escenario. */ async authorize() {},
    /** @returns {Promise<Object>} Recálculo causal acotado a esta página. */ read() {
      return spec.read(query, actor);
    },
    /** @param {Object} value Entrega. @returns {void} Registra sólo datos del escenario. */ emit(
      value,
    ) {
      sent.push(value);
    },
  });
  assert.equal(window.ready().statusCode, 200);
  assert.equal(reads, 0);
  const snapshot = await spec.read(query, actor);
  data = page('b', 2);
  await window.change();
  assert.equal(sent.length, 0);
  await window.bind({
    subscriptionId : 'suppliers',
    watchVersion   : 1,
    data           : snapshot.data,
    query,
    cursor         : snapshot.cursor,
    projectionHash : contentCursor(snapshot.data),
  });
  assert.equal(sent.length, 1);
  assert.equal(sent[0].type, 'patch');
  const operations = sent[0].data.operations;
  assert.ok(
    operations.some(
      /** @param {Object} operation Diferencia. @returns {boolean} Nueva fila. */ (operation) =>
        operation.op === 'add',
    ),
  );
  assert.ok(
    operations.some(
      /** @param {Object} operation Diferencia. @returns {boolean} Salida de la ventana. */ (
        operation,
      ) => operation.op === 'remove',
    ),
  );
  assert.ok(
    operations.some(
      /** @param {Object} operation Diferencia. @returns {boolean} Total autoritativo. */ (
        operation,
      ) => operation.path === '/pagination/total' && operation.value === 2,
    ),
  );
  window.close();
});
