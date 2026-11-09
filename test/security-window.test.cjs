/** @file Ventanas Security: permisos, contratos, referencias y concurrencia HTTP/realtime. */
'use strict';
const test                = require('node:test');
const assert              = require('node:assert/strict');
const { producerWindows } = require('./helpers/producer-window.cjs');
const { securityWindows } = require('../server/modules/realtime/services/security-window.service');
const { ReadWindow }      = require('../server/library/realtime/read-window');
/** @returns {Object} Principal sintético. */
function identity() {
  return {
    userCode    : 'u',
    sessionCode : 's',
    workspace   : 'w',
    context     : {
      user        : { userCode: 'u' },
      sessionCode : 's',
      workspace   : 'w',
      policies    : ['security.group.view.workspace'],
    },
  };
}
/** @param {string} code Identidad. @param {number} total Cantidad autorizada. @returns {Object} Página canónica mínima. */
function page(code, total) {
  return {
    items      : [
      {
        groupCode : code,
        name      : code,
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
test('Security declara veintiuna ventanas y rechaza consultas de autoridad o proyecciones privadas' /** @returns {Promise<void>} Usa contratos y adaptador productivos. */, async () => {
  let data       = page('g', 1);
  let statusCode = 200;
  const calls    = [];
  const topics   = producerWindows(securityWindows, {
    messaging : {
      /** @param {...*} args Solicitud acreditada. @returns {Promise<Object>} Productor controlado. */
      async request(...args) {
        calls.push(args);
        return {
          statusCode,
          data,
        };
      },
    },
  });
  assert.equal(
    Object.values(topics).reduce(
      /** @param {number} count Total. @param {Object} sections Secciones. @returns {number} Cantidad acumulada. */
      (count, sections) => count + Object.keys(sections).length,
      0,
    ),
    21,
  );
  const spec  = topics['security.group'].page;
  const query = {
    page    : 1,
    perPage : 1,
  };
  for (const invalid of [
    {
      ...query,
      fields : 'password',
    },
    {
      ...query,
      condition : {},
    },
    {
      ...query,
      workspace : 'other',
    },
    {
      ...query,
      scope : 'guest',
    },
    {
      ...query,
      perPage : 0,
    },
    {
      ...query,
      page : 0,
    },
    {
      ...query,
      perPage : 101,
    },
  ])
    assert.throws(
      /** @returns {Object} Entrada no autorizada. */
      () => spec.validate(invalid),
      { statusCode: 400 },
    );
  assert.deepEqual(
    spec.validate({
      ...query,
      scope : 'owner',
    }),
    {
      ...query,
      scope : 'owner',
    },
  );
  await spec.read(query, identity());
  assert.equal(calls[0][0], 'service-security');
  assert.equal(calls[0][1], 'group-get-list');
  await assert.rejects(
    spec.read(query, {
      ...identity(),
      context : { policies: [] },
    }),
    { statusCode: 403 },
  );
  data = {
    ...data,
    secret : 'PRIVATE',
  };
  await assert.rejects(spec.read(query, identity()), { statusCode: 503 });
  data = null;
  await assert.rejects(topics['security.group'].detail.read({ code: 'g' }, identity()), {
    statusCode : 403,
  });
  statusCode = 204;
  await assert.rejects(topics['security.group'].detail.read({ code: 'g' }, identity()), {
    statusCode : 403,
  });
  statusCode = 200;
  data = [
    {
      parameter : 'user-setings',
      workspace : 'allWorkspaces',
      revision  : 2,
      data      : {
        tokenNotificationExpiration : 86400,
        defaultGroupsByRole         : [
          {
            roleCode   : '100-customer',
            groupCodes : ['customer-group'],
          },
        ],
      },
    },
  ];
  const settingsIdentity = {
    ...identity(),
    context : {
      ...identity().context,
      policies : ['security.settings.view.global'],
    },
  };
  await topics['security.settings'].collection.read({ scope: 'global' }, settingsIdentity);
  data = data[0];
  await topics['security.settings'].detail.read({ parameter: 'user-setings' }, settingsIdentity);
  assert.equal(
    topics['security.settings'].collection.validate({ scope: 'global' }).scope,
    'global',
  );
  assert.throws(
    /** @returns {Object} La colección completa tiene capacidad explícita, no perPage cero. */
    () => topics['security.settings'].collection.validate({ perPage: 0 }),
    { statusCode: 400 },
  );
});
test('las referencias recalculan pertenencia de búsquedas y etiquetas sólo cuando corresponde' /** @returns {void} Selección causal sobre la ventana autorizada por el backend. */, () => {
  const topics = securityWindows({ messaging: {} });
  const role   = {
    source   : 'directory',
    resource : 'role',
    code     : 'r',
  };
  const user   = topics['security.user'].page;
  assert.equal(user.affects({}, role, identity(), { items: [{ roleCode: 'other' }] }), false);
  assert.equal(user.affects({ search: 'texto' }, role, identity(), { items: [] }), true);
  assert.equal(user.affects({}, role, identity(), { items: [{ roleCode: 'r' }] }), true);
  assert.equal(
    topics['security.profile'].detail.affects(
      {},
      {
        resource : 'user',
        code     : 'other',
      },
      identity(),
    ),
    false,
  );
  assert.equal(
    topics['security.profile'].detail.affects(
      {},
      {
        resource : 'session',
        code     : 's',
      },
      identity(),
    ),
    true,
  );
  assert.equal(
    topics['security.workspace'].detail.affects(
      { code: 'w' },
      {
        resource : 'workspace',
        code     : 'other',
      },
      identity(),
    ),
    false,
  );
  const dto  = require('../server/contracts/registry')['security.resource.changed'];
  const fact = {
    resource : 'settings',
    code     : 'session-setings',
    revision : 2,
  };
  assert.deepEqual(dto.input(fact), fact);
  assert.throws(
    /** @returns {Object} El evento no distribuye configuración privada. */
    () =>
      dto.input({
        ...fact,
        data : { secret: 'PRIVATE' },
      }),
    { statusCode: 400 },
  );
});
test('la carrera HTTP conserva altas, retiradas y totales, sin snapshots por socket ni nuevas lecturas del cliente', /** @returns {Promise<void>} Verifica recibos y diferencias reales ante eventos duplicados y pérdida de permisos. */ async () => {
  const actor = identity(),
    sent      = [];
  let data = page('a', 1),
    reads  = 0;
  const spec   = producerWindows(securityWindows, {
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
  })['security.group'].page;
  const query  = {
    page    : 1,
    perPage : 1,
  };
  const window = new ReadWindow({
    subscriptionId : 'groups',
    watchVersion   : 1,
    topic          : 'security.group',
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
    subscriptionId : 'groups',
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

test('acceso propio no exige permiso administrativo, usa el productor y rechaza selectores de otro actor' /** @returns {Promise<void>} El control comparte contrato con las otras ventanas y nunca expone credenciales. */, async () => {
  const calls     = [];
  const value     = {
    sessionCode : 's',
    userCode    : 'u',
    workspace   : 'w',
    expiration  : 9000000000,
    groups      : ['g'],
    policies    : [],
    workspaces  : [
      {
        workspaceCode : 'w',
        name          : 'Workspace',
      },
    ],
  };
  const topics    = producerWindows(securityWindows, {
    messaging : {
      /** @param {string} service Destino. @param {string} action Capacidad. @param {Object} query Consulta. @param {Object} options Identidad acreditada. @returns {Promise<Object>} Productor controlado. */
      async request(service, action, query, options) {
        calls.push({
          service,
          action,
          query,
          options,
        });
        return {
          statusCode : 200,
          data       : value,
        };
      },
    },
  });
  const spec      = topics['security.access'].detail;
  const principal = identity();
  principal.context.policies = [];
  principal.context.groups = ['g'];
  assert.deepEqual(spec.validate({}), {});
  assert.throws(/** @returns Selector prohibido. */ () => spec.validate({ userCode: 'other' }), {
    statusCode : 400,
  });
  assert.deepEqual((await spec.read({}, principal)).data, value);
  assert.equal(calls[0].action, 'session-own-access-get');
  assert.equal(calls[0].options.context, principal.context);
  assert.equal(
    spec.affects(
      {},
      {
        resource : 'user',
        code     : 'u',
      },
      principal,
    ),
    true,
  );
  assert.equal(
    spec.affects(
      {},
      {
        resource : 'user',
        code     : 'other',
      },
      principal,
    ),
    false,
  );
  assert.equal(
    spec.affects(
      {},
      {
        resource : 'group',
        code     : 'g',
      },
      principal,
    ),
    true,
  );
  assert.equal(
    spec.affects(
      {},
      {
        source   : 'directory',
        resource : 'contact',
        code     : 'c',
      },
      principal,
    ),
    false,
  );
  value.authorization = 'forbidden';
  await assert.rejects(spec.read({}, principal), { statusCode: 503 });
});

/** @param {string} resource Recurso. @returns {Object} Página canónica mínima de referencias. */
function referencePage(resource) {
  return {
    items      : [
      {
        [resource + 'Code'] : 'one',
        ...(resource === 'policy'
          ? { identifier: 'security.user.view.workspace' }
          : { name: 'Visible' }),
        revision            : 1,
      },
    ],
    pagination : {
      page    : 1,
      perPage : 100,
      total   : 1,
    },
  };
}
test('las referencias y servicios se leen por capacidades canónicas con ámbito acreditado y filtros cerrados', /** @returns {Promise<void>} No admite identidades, autoridad ni condiciones arbitrarias en la selección. */ async () => {
  const calls = [];
  let data;
  const topics = producerWindows(securityWindows, {
    messaging : {
      /** @param {...Object} args Solicitud. @returns {Promise<Object>} Proyección del productor. */
      async request(...args) {
        calls.push(args);
        return {
          statusCode : 200,
          data,
        };
      },
    },
  });
  for (const resource of ['group', 'policy', 'workspace']) {
    const spec  = topics['security.' + resource].references;
    const query = { codes: ['one', 'missing'] };
    const actor = identity();
    actor.context.policies = [
      'security.' + (resource === 'workspace' ? 'workspaces' : resource) + '.view.workspace',
    ];
    assert.deepEqual(spec.validate(query), query);
    for (const invalid of [
      { codes: ['one', 'one'] },
      {
        codes     : ['one'],
        workspace : 'foreign',
      },
      {
        codes     : ['one'],
        condition : {},
      },
      {
        codes : Array.from(
          { length: 101 },
          /** @param {Object} unused Sin uso. @param {number} index Posición. @returns {string} Código distinto. */ (
            _,
            index,
          ) => String(index),
        ),
      },
    ])
      assert.throws(/** @returns {Object} Consulta inválida. */ () => spec.validate(invalid), {
        statusCode : 400,
      });
    data = referencePage(resource);
    assert.deepEqual((await spec.read(query, actor)).data, data);
    assert.deepEqual(calls.at(-1), [
      'service-security',
      (resource === 'workspace' ? 'workspaces' : resource) + '-get-list',
      {
        page      : 1,
        perPage   : 100,
        sort      : resource + 'Code',
        order     : 'asc',
        condition : { [resource + 'Code']: { $in: query.codes } },
      },
      { context: actor.context },
    ]);
    assert.equal(
      spec.affects(
        query,
        {
          resource,
          code : 'one',
        },
        actor,
      ),
      true,
    );
    assert.equal(
      spec.affects(
        query,
        {
          resource,
          code : 'other',
        },
        actor,
      ),
      false,
    );
    await assert.rejects(
      spec.read(query, {
        ...actor,
        context : { policies: [] },
      }),
      { statusCode: 403 },
    );
  }
  const spec = topics['security.policy'].services,
    actor    = identity();
  actor.context.policies = ['security.policy.view.workspace'];
  data = { items: ['security', 'directory'] };
  assert.deepEqual((await spec.read(spec.validate({}), actor)).data, data);
  assert.equal(calls.at(-1)[1], 'policy-get-index');
  assert.throws(
    /** @returns {Object} No acepta paginación ficticia. */ () => spec.validate({ perPage: 0 }),
    { statusCode: 400 },
  );
});

test('conexiones personales usan la lectura canónica y sólo reciben concesiones de su titular', /** @returns {Promise<void>} No amplía administración OAuth ni depende de permisos administrativos. */ async () => {
  const calls  = [];
  const topics = producerWindows(securityWindows, {
    messaging : {
      /** @param {...*} args Consulta acreditada. @returns {Promise<Object>} Página canónica vacía. */
      async request(...args) {
        calls.push(args);
        return {
          statusCode : 200,
          data       : { items: [] },
        };
      },
    },
  });
  const spec   = topics['security.connection'].page;
  await spec.read(spec.validate({ page: 2 }), identity());
  assert.deepEqual(calls[0].slice(0, 3), ['service-security', 'oauth-grant-get-list', { page: 2 }]);
  assert.deepEqual(spec.collections, { '/items': 'code' });
  assert.throws(
    /** @returns {Object} La cuenta nunca la elige el solicitante. */ () =>
      spec.validate({
        page     : 1,
        userCode : 'other',
      }),
    { statusCode: 400 },
  );
  assert.equal(
    spec.affects(
      {},
      {
        resource : 'grant',
        code     : 'g',
        userCode : 'u',
      },
      identity(),
    ),
    true,
  );
  assert.equal(
    spec.affects(
      {},
      {
        resource : 'grant',
        code     : 'g',
        userCode : 'other',
      },
      identity(),
    ),
    false,
  );
  assert.equal(
    spec.affects(
      {},
      {
        resource : 'user',
        code     : 'u',
      },
      identity(),
    ),
    false,
  );
});

test('directorio público conserva páginas y selecciones, valida límites y delega la autorización al propietario', /** @returns {Promise<void>} El selector no exige administración de usuarios ni acepta filtros privados. */ async () => {
  const calls  = [];
  const data   = {
    items      : [
      {
        userCode  : 'u2',
        name      : 'Otra persona',
        email     : '',
        actorType : 'human',
      },
    ],
    pagination : {
      page    : 2,
      perPage : 20,
      total   : 21,
    },
  };
  const topics = producerWindows(securityWindows, {
    messaging : {
      /** @param {...*} args Consulta. @returns {Promise<Object>} Proyección canónica sin credenciales. */
      async request(...args) {
        calls.push(args);
        return {
          statusCode : 200,
          data,
        };
      },
    },
  });
  const actor  = identity();
  actor.context.policies = [];
  const page   = topics['security.directory'].page,
    references = topics['security.directory'].references;
  const query = {
    page      : 2,
    search    : 'Otra',
    workspace : 'w',
  };
  assert.deepEqual((await page.read(page.validate(query), actor)).data, data);
  assert.deepEqual(calls[0], [
    'service-security',
    'user-directory-get-list',
    query,
    { context: actor.context },
  ]);
  assert.deepEqual(page.collections, { '/items': 'userCode' });
  await references.read(
    references.validate({
      codes     : ['u2'],
      workspace : 'w',
    }),
    actor,
  );
  assert.deepEqual(calls[1][2], {
    codes     : ['u2'],
    workspace : 'w',
  });
  for (const query of [
    { codes: [] },
    { codes: ['u', 'u'] },
    {
      codes  : ['u'],
      search : 'secret',
    },
    {
      codes : Array.from(
        { length: 101 },
        /** @param {*} value Sin uso. @param {number} index Posición. @returns {string} Identidad única. */ (
          value,
          index,
        ) => 'u' + index,
      ),
    },
  ])
    assert.throws(/** @returns {Object} Consulta inválida. */ () => references.validate(query), {
      statusCode : 400,
    });
  assert.throws(
    /** @returns {Object} No permite condiciones arbitrarias. */ () =>
      page.validate({
        page      : 1,
        condition : {},
      }),
    { statusCode: 400 },
  );
  assert.equal(
    references.affects(
      { codes: ['u2'] },
      {
        resource : 'user',
        code     : 'u3',
      },
      actor,
    ),
    false,
  );
  assert.equal(
    references.affects(
      { codes: ['u2'] },
      {
        resource : 'user',
        code     : 'u2',
      },
      actor,
    ),
    true,
  );
  assert.equal(
    page.affects(
      query,
      {
        resource : 'contact',
        code     : 'c',
        source   : 'directory',
      },
      actor,
    ),
    true,
  );
  assert.equal(
    page.affects(
      query,
      {
        resource : 'grant',
        code     : 'g',
      },
      actor,
    ),
    false,
  );
});
