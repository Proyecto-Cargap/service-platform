/** @file Ventanas Directory: consultas del propietario, límites, pertenencia, carreras y revocación. */
'use strict';
const test                = require('node:test');
const assert              = require('node:assert/strict');
const { producerWindows } = require('./helpers/producer-window.cjs');
const {
  directoryWindows,
} = require('../server/modules/realtime/services/directory-window.service');
const { ReadWindow } = require('../server/library/realtime/read-window');
const dtos           = require('../server/contracts/registry');
/** @returns {Object} Principal sintético con facultades locales. */
function identity() {
  return {
    userCode  : 'u',
    workspace : 'w',
    context   : {
      user      : { userCode: 'u' },
      workspace : 'w',
      groups    : ['g'],
      policies  : ['directory.role.view.workspace', 'directory.contact.view.workspace'],
    },
  };
}
test('catálogos fiscales conservan HTTP paginado y sólo reaccionan a su clase registrada', /** @returns {Promise<void>} Autoridad del propietario, límites y hecho mínimo de vigencia. */ async () => {
  const calls  = [];
  const result = {
    items      : [],
    pagination : {
      page    : 1,
      perPage : 20,
      total   : 0,
    },
    source     : 'Catálogo de prueba',
  };
  const actor  = {
    ...identity(),
    context : {
      user     : { userCode: 'u' },
      policies : [],
    },
  };
  const topics = producerWindows(directoryWindows, {
    messaging : {
      /** @param {...*} args Consulta canónica y contexto. @returns {Promise<Object>} Página del propietario con recibo del transporte. */
      async request(...args) {
        calls.push(args);
        return {
          statusCode : 200,
          data       : result,
        };
      },
    },
  });
  const spec   = topics['directory.fiscal-catalog'].page;
  const query  = {
    kind    : 'currency',
    page    : 1,
    perPage : 20,
  };
  assert.deepEqual((await spec.read(spec.validate(query), actor)).data, result);
  assert.deepEqual(calls, [
    ['service-directory', 'fiscal-catalog-get-list', query, { context: actor.context }],
  ]);
  assert.deepEqual(spec.collections, { '/items': 'code' });
  const fact = dtos['directory.resource.changed'].input({
    resource          : 'fiscal-catalog',
    code              : 'currency',
    revision          : 2,
    workspace         : 'allWorkspaces',
    previousWorkspace : null,
    erased            : false,
  });
  assert.equal(spec.affects(query, fact, actor), true);
  assert.equal(
    spec.affects(
      {
        ...query,
        taxpayerType : 'legal',
      },
      {
        ...fact,
        code : 'taxpayer-type',
      },
      actor,
    ),
    true,
  );
  assert.equal(
    spec.affects(
      query,
      {
        ...fact,
        code : 'taxpayer-type',
      },
      actor,
    ),
    false,
  );
  assert.equal(
    spec.affects(
      {
        ...query,
        kind : 'tax-regime',
      },
      fact,
      actor,
    ),
    false,
  );
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
      kind : 'unknown',
    },
    {
      ...query,
      workspace : 'foreign',
    },
  ])
    assert.throws(
      /** @returns {Object} Consulta fuera del contrato cerrado. */
      () => spec.validate(invalid),
      { statusCode: 400 },
    );
});
/** @param {string} code Identidad de catálogo. @param {number} total Cantidad. @param {string} name Etiqueta. @returns {Object} Página pública válida. */
function page(code, total, name = code) {
  return {
    items      : [
      {
        roleCode  : code,
        name,
        workspace : 'w',
        revision  : 1,
      },
    ],
    pagination : {
      page    : 1,
      perPage : 1,
      total,
    },
  };
}
test('Directory usa consultas canónicas y rechaza proyecciones privadas, falta de facultades y paginación inválida', /** @returns {Promise<void>} Usa validadores y registro reales. */ async () => {
  const actor = identity(),
    calls     = [];
  let response = {
    statusCode : 200,
    data       : page('a', 1),
  };
  const topics = producerWindows(directoryWindows, {
    messaging : {
      /** @param {...*} args RPC y contexto. @returns {Promise<Object>} Productor simulado con recibo técnico real. */
      async request(...args) {
        calls.push(args);
        return response;
      },
    },
  });
  const spec = topics['directory.role'].page,
    query    = {
      page    : 1,
      perPage : 1,
    };
  assert.deepEqual(spec.validate(query), query);
  for (const invalid of [
    {
      page    : 0,
      perPage : 1,
    },
    {
      page    : 1,
      perPage : 0,
    },
    {
      page    : 1,
      perPage : 101,
    },
    {
      ...query,
      condition : '{}',
    },
    {
      ...query,
      fields : 'secrets',
    },
    {
      ...query,
      workspace : 'foreign',
    },
  ]) {
    assert.throws(/** @returns {Object} Consulta no autorizada. */ () => spec.validate(invalid), {
      statusCode : 400,
    });
  }
  assert.deepEqual((await spec.read(query, actor)).data, response.data);
  assert.deepEqual(calls[0], [
    'service-directory',
    'role-get-list',
    query,
    { context: actor.context },
  ]);
  await assert.rejects(
    spec.read(query, {
      ...actor,
      context : {
        ...actor.context,
        policies : [],
      },
    }),
    { statusCode: 403 },
  );
  response = {
    statusCode : 200,
    data       : {
      ...response.data,
      secret : 'PRIVATE',
    },
  };
  await assert.rejects(spec.read(query, actor), { statusCode: 503 });
  response = { statusCode: 403 };
  await assert.rejects(spec.read(query, actor), { statusCode: 403 });
  const change = {
    resource          : 'role',
    code              : 'a',
    workspace         : 'foreign',
    previousWorkspace : 'w',
    revision          : 2,
    erased            : false,
  };
  assert.equal(spec.affects(query, change, actor), true);
  assert.equal(
    spec.affects(
      query,
      {
        ...change,
        previousWorkspace : null,
      },
      actor,
    ),
    false,
  );
  assert.equal(
    topics['directory.contact'].page.affects(
      query,
      {
        ...change,
        workspace : 'w',
      },
      actor,
    ),
    true,
  );
  assert.equal(topics['directory.role'].detail.affects({ code: 'other' }, change, actor), false);
  assert.equal(
    spec.affects(
      query,
      {
        control : {
          kind : 'group',
          code : 'g',
        },
      },
      actor,
    ),
    true,
  );
  assert.deepEqual(dtos['directory.resource.changed'].input(change), change);
  assert.throws(
    /** @returns {Object} El hecho no distribuye registros. */ () =>
      dtos['directory.resource.changed'].input({
        ...change,
        item : { secret: 'PRIVATE' },
      }),
    { statusCode: 400 },
  );
});
test('la carrera HTTP conserva altas, retiradas y totales, sin snapshots por socket ni nuevas lecturas del cliente', /** @returns {Promise<void>} Verifica recibos y diferencias reales ante eventos duplicados y pérdida de permisos. */ async () => {
  const actor = identity(),
    sent      = [];
  let data = page('a', 1),
    reads  = 0;
  const spec   = producerWindows(directoryWindows, {
    messaging : {
      /** @returns {Promise<Object>} Consulta canónica por cambio real. */
      async request() {
        reads++;
        return {
          statusCode : 200,
          data       : structuredClone(data),
        };
      },
    },
  })['directory.role'].page;
  const query  = {
    page    : 1,
    perPage : 1,
  };
  const window = new ReadWindow({
    subscriptionId : 'roles',
    watchVersion   : 1,
    topic          : 'directory.role',
    section        : 'page',
    collections    : spec.collections,
    /** @returns {Promise<void>} El transporte revalida el principal. */ async authorize() {},
    /** @returns {Promise<Object>} El propietario calcula únicamente esta ventana. */ read() {
      return spec.read(query, actor);
    },
    /** @param {Object} value Diferencia. @returns {void} Captura la entrega. */ emit(value) {
      sent.push(value);
    },
  });
  assert.equal(window.ready().statusCode, 200);
  assert.equal(reads, 0);
  const snapshot = await spec.read(query, actor);
  data = page('b', 2);
  await window.change();
  assert.equal(sent.length, 0);
  const { contentCursor } = require('../server/library/realtime/window-delta');
  await window.bind({
    subscriptionId : 'roles',
    watchVersion   : 1,
    data           : snapshot.data,
    query,
    cursor         : snapshot.cursor,
    projectionHash : contentCursor(snapshot.data),
  });
  assert.equal(sent.length, 1);
  assert.equal(sent[0].type, 'patch');
  assert(
    sent[0].data.operations.some(
      /** @param {Object} op Diferencia. @returns {boolean} Alta en la ventana. */ (op) =>
        op.op === 'add',
    ),
  );
  assert(
    sent[0].data.operations.some(
      /** @param {Object} op Diferencia. @returns {boolean} Retirada de la ventana. */ (op) =>
        op.op === 'remove',
    ),
  );
  assert(
    sent[0].data.operations.some(
      /** @param {Object} op Diferencia. @returns {boolean} Total autoritativo. */ (op) =>
        op.path === '/pagination/total' && op.value === 2,
    ),
  );
  await window.change();
  assert.equal(sent.length, 1);
  actor.context.policies = [];
  await window.change();
  assert.equal(sent.at(-1).type, 'revoked');
  window.close();
});

test('las etiquetas de roles seleccionados conservan la consulta autorizada y sólo reaccionan a sus códigos', /** @returns {Promise<void>} La selección no define autoridad ni una base del cliente. */ async () => {
  const calls = [],
    actor     = identity();
  const spec  = producerWindows(directoryWindows, {
    messaging : {
      /** @param {...Object} args Solicitud. @returns {Promise<Object>} Página autorizada del productor. */
      async request(...args) {
        calls.push(args);
        return {
          statusCode : 200,
          data       : page('r', 1),
        };
      },
    },
  })['directory.role'].references;
  const query = spec.validate({ codes: ['r'] });
  await spec.read(query, actor);
  assert.deepEqual(calls[0], [
    'service-directory',
    'role-get-list',
    {
      page      : 1,
      perPage   : 100,
      sort      : 'roleCode',
      order     : 'asc',
      condition : { roleCode: { $in: ['r'] } },
    },
    { context: actor.context },
  ]);
  assert.equal(
    spec.affects(
      query,
      {
        resource  : 'role',
        code      : 'r',
        workspace : 'w',
      },
      actor,
    ),
    true,
  );
  assert.equal(
    spec.affects(
      query,
      {
        resource  : 'role',
        code      : 'r',
        workspace : 'foreign',
      },
      actor,
    ),
    false,
  );
  assert.equal(
    spec.affects(
      query,
      {
        resource  : 'role',
        code      : 'other',
        workspace : 'w',
      },
      actor,
    ),
    false,
  );
  assert.throws(
    /** @returns {Object} Rechaza una selección que pretende ampliar el ámbito. */ () =>
      spec.validate({
        codes : ['r'],
        scope : 'global',
      }),
    { statusCode: 400 },
  );
  assert.throws(
    /** @returns {Object} Rechaza identidades duplicadas. */ () =>
      spec.validate({ codes: ['r', 'r'] }),
    { statusCode: 400 },
  );
});

test('perfiles seleccionados usan una ventana acotada y siguen etiquetas de sus referencias', /** @returns {Promise<void>} Protege la consulta, el ámbito y las dependencias de las filas autorizadas. */ async () => {
  const calls = [],
    actor     = identity();
  const data  = {
    items      : [
      {
        contactCode : 'c',
        name        : { firstName: 'Persona' },
        jobCode     : 'j',
        workspace   : 'w',
        revision    : 1,
      },
    ],
    pagination : {
      page    : 1,
      perPage : 100,
      total   : 1,
    },
  };
  const spec  = producerWindows(directoryWindows, {
    messaging : {
      /** @param {...*} args Consulta. @returns {Promise<Object>} Base autorizada. */
      async request(...args) {
        calls.push(args);
        return {
          statusCode : 200,
          data,
        };
      },
    },
  })['directory.contact'].references;
  const query = spec.validate({ codes: ['c'] });
  assert.deepEqual((await spec.read(query, actor)).data, data);
  assert.deepEqual(calls[0][2], {
    page      : 1,
    perPage   : 100,
    sort      : 'contactCode',
    order     : 'asc',
    condition : { contactCode: { $in: ['c'] } },
  });
  for (const query of [
    { codes: [] },
    { codes: ['c', 'c'] },
    {
      codes     : ['c'],
      condition : {},
    },
  ])
    assert.throws(/** @returns {Object} Consulta inválida. */ () => spec.validate(query), {
      statusCode : 400,
    });
  assert.equal(
    spec.affects(
      query,
      {
        resource  : 'contact',
        code      : 'other',
        workspace : 'w',
      },
      actor,
      data,
    ),
    false,
  );
  assert.equal(
    spec.affects(
      query,
      {
        resource  : 'contact',
        code      : 'c',
        workspace : 'w',
      },
      actor,
      data,
    ),
    true,
  );
  assert.equal(
    spec.affects(
      query,
      {
        resource  : 'job',
        code      : 'j',
        workspace : 'foreign',
      },
      actor,
      data,
    ),
    true,
  );
  assert.equal(
    spec.affects(
      query,
      {
        resource  : 'job',
        code      : 'other',
        workspace : 'w',
      },
      actor,
      data,
    ),
    false,
  );
});
