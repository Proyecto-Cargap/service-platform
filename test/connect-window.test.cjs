/** @file Cuenta Connect: autoridad del productor, ausencia y revocación sin consultas a proveedores. */
'use strict';
const { producerWindows } = require('./helpers/producer-window.cjs');
const test                = require('node:test');
const assert              = require('node:assert/strict');
const { connectWindows }  = require('../server/modules/realtime/services/connect-window.service');
const dtos                = require('../server/contracts/registry');
test('retirar un participante de Chat revoca su ventana Meta sin consultar nuevamente desde el cliente', /** @returns {Promise<void>} La membresía vigente precede cualquier entrega de Connect, incluso con estado ausente. */ async () => {
  const { ReadWindow }    = require('../server/library/realtime/read-window');
  const { contentCursor } = require('../server/library/realtime/window-delta');
  const identity          = {
    userCode : 'u',
    context  : { policies: ['connect.meta.view.global'] },
  };
  let allowed = true,
    reads     = 0;
  const spec = producerWindows(connectWindows, {
    messaging : {
      /** @param {string} service Propietario. @param {string} action Lectura. @returns {Promise<Object>} Acredita la membresía antes de Connect. */
      async request(service, action) {
        if (service === 'service-communication') {
          assert.equal(action, 'chat-get-one');
          return { statusCode: allowed ? 200 : 404 };
        }
        reads++;
        return {
          statusCode : 200,
          data       : null,
        };
      },
    },
  })['connect.meta-conversation'].detail;
  const query = { conversationCode: 'chat' },
    frames    = [];
  const change = {
    source           : 'communication',
    conversationCode : 'chat',
    users            : ['u'],
    revision         : 3,
  };
  assert.equal(
    spec.affects(
      query,
      {
        ...change,
        conversationCode : 'other',
      },
      identity,
    ),
    false,
  );
  assert.equal(
    spec.affects(
      query,
      {
        ...change,
        users : ['other'],
      },
      identity,
    ),
    false,
  );
  assert.equal(spec.affects(query, change, identity, null), true);
  const window   = new ReadWindow({
    subscriptionId                                                                                          : 'meta',
    watchVersion                                                                                            : 1,
    topic                                                                                                   : 'connect.meta-conversation',
    section                                                                                                 : 'detail',
    collections                                                                                             : spec.collections,
    /** @returns {Promise<void>} Principal autenticado. */ async authorize() {},
    /** @returns {Promise<Object>} Consulta autorizada del productor. */ read                               : () =>
      spec.read(query, identity),
    /** @param {Object} frame Entrega estándar. @returns {number} Captura sin lecturas del cliente. */ emit : (
      frame,
    ) => frames.push(frame),
  });
  const snapshot = await spec.read(query, identity);
  await window.bind({
    subscriptionId : 'meta',
    watchVersion   : 1,
    query,
    ...snapshot,
    projectionHash : contentCursor(snapshot.data),
  });
  const before = reads;
  allowed = false;
  await window.change(change);
  assert.equal(reads, before);
  assert.equal(frames.at(-1).type, 'revoked');
  window.close();
});
test('las cuentas sólo consultan Connect con el contexto vigente y descartan hechos ajenos' /** @returns {Promise<void>} Comprueba selector, proyección, errores y alcance personal. */, async () => {
  const identity = {
    userCode  : 'u',
    workspace : 'w',
    context   : {
      user      : { userCode: 'u' },
      workspace : 'w',
      groups    : ['g'],
    },
  };
  let response   = {
    statusCode : 200,
    data       : null,
  };
  const spec     = producerWindows(connectWindows, {
    messaging : {
      /** @param {string} service Productor. @param {string} action Lectura. @param {Object} query Selector. @param {Object} options Contexto. @returns {Promise<Object>} Respuesta del productor. */
      async request(service, action, query, options) {
        assert.equal(service, 'service-connect');
        assert.equal(action, 'mail-account-get');
        assert.deepEqual(query, {});
        assert.equal(options.context, identity.context);
        return response;
      },
    },
  })['connect.mail-account'].detail;
  assert.deepEqual(spec.validate({}), {});
  assert.throws(
    /** @returns {Object} Rechaza autoridad del cuerpo. */ () =>
      spec.validate({ workspace: 'other' }),
    { statusCode: 400 },
  );
  assert.equal((await spec.read({}, identity)).data, null);
  response = {
    statusCode : 200,
    data       : {
      userCode : 'u',
      secrets  : 'PRIVATE',
    },
  };
  await assert.rejects(spec.read({}, identity), { statusCode: 503 });
  response = {
    statusCode      : 403,
    descriptionCode : 'FORBIDDEN',
  };
  await assert.rejects(spec.read({}, identity), { statusCode: 403 });
  assert.equal(spec.affects({}, { userCode: 'other' }, identity), false);
  assert.equal(spec.affects({}, { userCode: 'u' }, identity), true);
  assert.equal(spec.affects({ userCode: 'other' }, { userCode: 'other' }, identity), true);
  assert.equal(
    spec.affects(
      {},
      {
        control : {
          kind : 'user',
          code : 'u',
        },
      },
      identity,
    ),
    true,
  );
  assert.equal(
    spec.affects(
      {},
      {
        control : {
          kind : 'group',
          code : 'g',
        },
      },
      identity,
    ),
    true,
  );
  assert.equal(
    spec.affects(
      {},
      {
        parameter : 'mail-connection',
        workspace : 'allWorkspaces',
      },
      identity,
    ),
    false,
  );
  const page = producerWindows(connectWindows, { messaging: {} })['connect.mail-account'].page;
  const base = {
    items : [
      {
        type     : 'personal',
        userCode : 'visible',
      },
    ],
  };
  assert.equal(
    page.affects(
      {},
      {
        userCode   : 'other',
        listChange : 'update',
      },
      identity,
      base,
    ),
    false,
  );
  assert.equal(
    page.affects(
      {},
      {
        userCode   : 'visible',
        listChange : 'update',
      },
      identity,
      base,
    ),
    true,
  );
  assert.equal(
    page.affects(
      {},
      {
        userCode   : 'visible',
        listChange : 'none',
      },
      identity,
      base,
    ),
    false,
  );
  assert.equal(
    page.affects(
      {},
      {
        userCode   : 'other',
        listChange : 'membership',
      },
      identity,
      base,
    ),
    true,
  );
  assert.equal(page.affects({ page: 1 }, { userCode: 'other' }, identity), true);
  assert.equal(
    page.affects(
      { page: 1 },
      {
        parameter : 'mail-connection',
        workspace : 'allWorkspaces',
      },
      identity,
    ),
    true,
  );
  assert.equal(
    page.affects(
      { page: 1 },
      {
        parameter : 'another-setting',
        workspace : 'allWorkspaces',
      },
      identity,
    ),
    false,
  );
  assert.throws(
    /** @returns {Object} La consulta no acredita autoridad. */ () =>
      page.validate({
        page     : 1,
        userCode : 'other',
      }),
    { statusCode: 400 },
  );
  assert.throws(
    /** @returns {Object} No admite secretos en eventos. */ () =>
      dtos['connect.mail-account.changed'].input({
        userCode : 'u',
        secrets  : 'PRIVATE',
      }),
    { statusCode: 400 },
  );
});

test('SMTP global distingue ausencia, revocación y proyección inválida sin consultar proveedores' /** @returns {Promise<void>} Sólo el productor autorizado proporciona la ficha y sus revisiones. */, async () => {
  const query    = {
    parameter : 'mail-connection',
    scope     : 'global',
  };
  const identity = {
    userCode : 'u',
    context  : { groups: ['g'] },
  };
  let response   = {
    statusCode      : 400,
    descriptionCode : 'N0002-C000-E0002:Settings does not exist',
  };
  const spec     = producerWindows(connectWindows, {
    messaging : {
      /** @param {string} service Productor. @param {string} action Acción. @param {Object} input Selector. @param {Object} options Contexto. @returns {Promise<Object>} Respuesta pública. */
      async request(service, action, input, options) {
        assert.equal(service, 'service-connect');
        assert.equal(action, 'settings-get-one');
        assert.deepEqual(input, query);
        assert.equal(options.context, identity.context);
        return response;
      },
    },
  })['connect.settings'].detail;
  assert.deepEqual(spec.validate(query), query);
  assert.throws(
    /** @returns {Object} Rechaza autoridad suministrada por el cliente. */
    () =>
      spec.validate({
        ...query,
        workspace : 'another',
      }),
    { statusCode: 400 },
  );
  assert.equal((await spec.read(query, identity)).data, null);
  response = {
    statusCode      : 503,
    descriptionCode : 'UNAVAILABLE',
  };
  await assert.rejects(spec.read(query, identity), { statusCode: 503 });
  response = {
    statusCode      : 403,
    descriptionCode : 'FORBIDDEN',
  };
  await assert.rejects(spec.read(query, identity), { statusCode: 403 });
  response = {
    statusCode : 200,
    data       : {
      ...query,
      secrets : 'PRIVATE',
    },
  };
  await assert.rejects(spec.read(query, identity), { statusCode: 503 });
  assert.equal(
    spec.affects(
      query,
      {
        parameter : 'mail-connection',
        workspace : 'allWorkspaces',
      },
      identity,
    ),
    true,
  );
  assert.equal(
    spec.affects(
      query,
      {
        parameter : 'mail-connection',
        workspace : 'another',
      },
      identity,
    ),
    false,
  );
  assert.equal(
    spec.affects(
      query,
      {
        parameter : 'another',
        workspace : 'allWorkspaces',
      },
      identity,
    ),
    false,
  );
  assert.equal(spec.affects(query, { userCode: 'u' }, identity), false);
  assert.equal(
    spec.affects(
      query,
      {
        control : {
          kind : 'group',
          code : 'g',
        },
      },
      identity,
    ),
    true,
  );
});

test('plantillas: páginas y fichas reciben diferencias, conservan orden y total y revocan sin lecturas del cliente', /** @returns {Promise<void>} Coordina contratos reales y un productor canónico controlado. */ async () => {
  const { RealtimeWindows } = require('../server/library/realtime/windows');
  const { ReadWindow }      = require('../server/library/realtime/read-window');
  const { contentCursor }   = require('../server/library/realtime/window-delta');
  const identity            = {
    userCode : 'u',
    context  : {
      user      : { userCode: 'u' },
      workspace : 'w',
      policies  : ['connect.email-template.view.global'],
    },
  };
  let records               = [
    {
      templateCode : 'T1',
      identifier   : 'a',
      title        : 'Uno',
      content      : 'Hola',
      workspace    : 'allWorkspaces',
      userCreate   : 'u',
      userAssigned : '',
      creationDate : 1,
      updateDate   : 1,
    },
    {
      templateCode : 'T2',
      identifier   : 'b',
      title        : 'Dos',
      content      : 'Hola',
      workspace    : 'allWorkspaces',
      userCreate   : 'u',
      userAssigned : '',
      creationDate : 1,
      updateDate   : 1,
    },
  ];
  let reads   = 0,
    permitted = true;
  const topics = producerWindows(connectWindows, {
    messaging : {
      /** @param {string} service Propietario. @param {string} action Capacidad. @param {Object} request Consulta. @param {Object} options Contexto. @returns {Promise<Object>} Representación autorizada, con recibo del productor. */
      async request(service, action, request, options) {
        assert.equal(service, 'service-connect');
        assert.equal(action, 'emailtemplate-window-read');
        assert.equal(options.context, identity.context);
        reads++;
        if (!permitted)
          return {
            statusCode      : 403,
            descriptionCode : 'FORBIDDEN',
          };
        const { section, query } = request;
        const selected           = records
          .filter(
            /** @param {Object} row Fila. @returns {boolean} Filtro canónico controlado. */ (row) =>
              !query.search || row.title.includes(query.search),
          )
          .sort(
            /** @param {Object} a Fila. @param {Object} b Otra fila. @returns {number} Orden determinista. */ (
              a,
              b,
            ) =>
              a.identifier.localeCompare(b.identifier) ||
              a.templateCode.localeCompare(b.templateCode),
          );
        return {
          statusCode : 200,
          data       :
            section === 'detail'
              ? records.find(
                  /** @param {Object} row Fila. @returns {boolean} Identidad. */ (row) =>
                    row.templateCode === query.code,
                ) || null
              : {
                  items      : selected.slice(
                    (query.page - 1) * query.perPage,
                    query.page * query.perPage,
                  ),
                  pagination : {
                    page    : query.page,
                    perPage : query.perPage,
                    total   : selected.length,
                  },
                },
        };
      },
    },
  });
  const hub = new RealtimeWindows({
      topics,
      receipts : {},
    }),
    windows = new Map();
  hub.sessions.set('local', { windows });
  /** @param {string} section Sección. @param {Object} query Selector. @returns {Promise<Object>} Lectura explícita seguida de patches. */
  async function observe(section, query) {
    const spec       = topics['connect.template'][section],
      subscriptionId = section,
      deliveries     = [];
    spec.validate(query);
    const window = new ReadWindow({
      subscriptionId,
      watchVersion                                                                       : 1,
      topic                                                                              : 'connect.template',
      section,
      collections                                                                        : spec.collections,
      /** @returns {Promise<void>} Identidad acreditada. */ async authorize() {},
      /** @returns {Promise<Object>} Consulta canónica acotada. */ read                  : () =>
        spec.read(query, identity),
      /** @param {Object} value Diferencia. @returns {number} Registra sin HTTP. */ emit : (value) =>
        deliveries.push(value),
    });
    windows.set(subscriptionId, {
      window,
      query,
      identity,
    });
    assert.equal(Object.hasOwn(window.ready(), 'data'), false);
    const snapshot = await spec.read(query, identity);
    await window.bind({
      subscriptionId,
      watchVersion   : 1,
      query,
      ...snapshot,
      projectionHash : contentCursor(snapshot.data),
    });
    return {
      window,
      deliveries,
    };
  }
  try {
    const query = {
      page    : 1,
      perPage : 1,
      sort    : 'identifier',
      order   : 'asc',
      search  : '',
      scope   : 'global',
    };
    const page = await observe('page', query),
      detail   = await observe('detail', {
        code  : 'T1',
        scope : 'global',
      });
    const first = reads;
    await hub.changed('connect.template', {
      code      : 'foreign',
      workspace : 'another',
    });
    assert.equal(reads, first);
    for (let n = 1; n <= 100; n++) {
      records = records.map(
        /** @param {Object} row Fila. @returns {Object} Cambio real. */ (row) =>
          row.templateCode === 'T1'
            ? {
                ...row,
                title      : 'Cambio ' + n,
                updateDate : n + 1,
              }
            : row,
      );
      await hub.changed('connect.template', {
        code      : 'T1',
        workspace : 'allWorkspaces',
      });
    }
    assert.equal(page.window.base.items[0].title, 'Cambio 100');
    assert.equal(detail.window.base.title, 'Cambio 100');
    assert.equal(page.deliveries.length, 100);
    assert(
      page.deliveries.every(
        /** @param {Object} frame Entrega. @returns {boolean} Diferencias sin snapshots. */ (
          frame,
        ) => frame.type === 'patch',
      ),
    );
    records = records.map(
      /** @param {Object} row Fila. @returns {Object} Un cambio de orden hace entrar otra fila. */ (
        row,
      ) =>
        row.templateCode === 'T2'
          ? {
              ...row,
              identifier : '0',
            }
          : row,
    );
    const details = detail.deliveries.length;
    await hub.changed('connect.template', {
      code      : 'T2',
      workspace : 'allWorkspaces',
    });
    assert.equal(page.window.base.items[0].templateCode, 'T2');
    assert.equal(detail.deliveries.length, details);
    records = records.filter(
      /** @param {Object} row Fila. @returns {boolean} Retirada real. */ (row) =>
        row.templateCode !== 'T2',
    );
    await hub.changed('connect.template', {
      code      : 'T2',
      workspace : 'allWorkspaces',
    });
    assert.equal(page.window.base.items[0].templateCode, 'T1');
    assert.equal(page.window.base.pagination.total, 1);
    permitted = false;
    await hub.changed('connect.template', {
      control : {
        kind : 'user',
        code : 'u',
      },
    });
    assert.equal(page.deliveries.at(-1).type, 'revoked');
    assert.equal(detail.deliveries.at(-1).type, 'revoked');
    assert.throws(
      /** @returns {Object} No admite eludir límites. */ () =>
        topics['connect.template'].page.validate({
          ...query,
          perPage : 0,
        }),
      { statusCode: 400 },
    );
  } finally {
    hub.close();
  }
});

test('Meta: configuración, conexiones e historial sólo reciben cambios propios y el productor renueva permisos', /** @returns {Promise<void>} Valida registros y propagación de ajustes globales sobre las conexiones. */ async () => {
  const { ReadWindow }    = require('../server/library/realtime/read-window');
  const { contentCursor } = require('../server/library/realtime/window-delta');
  const identity          = {
    userCode : 'u',
    context  : { policies: ['connect.meta.view.global'] },
  };
  let settings            = {
    configurationCode     : 'M1',
    revision              : 1,
    enabled               : true,
    appId                 : 'app',
    graphVersion          : 'v25.0',
    publicBaseUrl         : 'https://example.invalid',
    workspaceCode         : 'W',
    contactRoleCode       : 'R',
    externalGroupCodes    : [],
    webhookUrl            : 'https://example.invalid/connect/meta/webhooks/M1',
    appSecretConfigured   : true,
    verifyTokenConfigured : true,
  };
  let record              = {
    connectionCode        : 'C1',
    revision              : 1,
    name                  : 'Canal',
    channel               : 'whatsapp',
    assetId               : 'asset',
    businessId            : 'business',
    instagramAuthMode     : 'unified',
    pageId                : '',
    active                : true,
    automation            : {
      enabled   : false,
      agentCode : '',
      teamCode  : '',
    },
    status                : 'ready',
    lastVerifiedAt        : 1,
    lastError             : null,
    accessTokenConfigured : true,
    credentialExpiresAt   : null,
    credentialExpiryKnown : false,
  };
  let permitted = true,
    privateData = false;
  const topics = producerWindows(connectWindows, {
    messaging : {
      /** @param {string} service Propietario. @param {string} action Capacidad. @param {Object} input Consulta. @param {Object} options Identidad. @returns {Promise<Object>} Datos públicos cerrados. */
      async request(service, action, input, options) {
        assert.equal(service, 'service-connect');
        assert.equal(action, 'meta-window-read');
        assert.equal(options.context, identity.context);
        if (!permitted) return { statusCode: 403 };
        const data =
          input.section === 'settings'
            ? settings
            : input.section === 'connection'
              ? record
              : {
                  items      : input.section === 'connections' ? [record] : [],
                  pagination : {
                    page    : input.query.page,
                    perPage : input.query.perPage,
                    total   : input.section === 'connections' ? 1 : 0,
                  },
                };
        return {
          statusCode : 200,
          data       : privateData
            ? {
                ...data,
                secrets : 'PRIVATE',
              }
            : data,
        };
      },
    },
  })['connect.meta'];
  const page   = {
    page    : 1,
    perPage : 20,
  };
  assert.deepEqual(topics.connections.collections, { '/items': 'connectionCode' });
  assert.deepEqual(topics.conversations.collections, { '/items': 'conversationCode' });
  assert.equal((await topics.settings.read({}, identity)).data.revision, 1);
  assert.equal((await topics.connections.read(page, identity)).data.items[0].connectionCode, 'C1');
  assert.deepEqual((await topics.conversations.read(page, identity)).data.items, []);
  assert.equal(
    topics.settings.affects(
      {},
      {
        resource : 'connection',
        code     : 'C1',
      },
      identity,
    ),
    false,
  );
  assert.equal(
    topics.connections.affects(
      page,
      {
        resource : 'settings',
        code     : 'global',
      },
      identity,
    ),
    true,
  );
  assert.equal(
    topics.connection.affects(
      { connectionCode: 'C1' },
      {
        resource : 'settings',
        code     : 'global',
      },
      identity,
    ),
    true,
  );
  assert.equal(
    topics.connection.affects(
      { connectionCode: 'C1' },
      {
        resource : 'connection',
        code     : 'other',
      },
      identity,
    ),
    false,
  );
  assert.equal(
    topics.conversations.affects(
      {
        ...page,
        connectionCode : 'C1',
      },
      {
        resource       : 'conversation',
        code           : 'V',
        connectionCode : 'other',
      },
      identity,
    ),
    false,
  );
  assert.equal(
    topics.conversations.affects(
      {
        ...page,
        connectionCode : 'C1',
      },
      {
        resource       : 'conversation',
        code           : 'V',
        connectionCode : 'C1',
      },
      identity,
    ),
    true,
  );
  const deliveries = [];
  const spec = topics.connection,
    query    = { connectionCode: 'C1' };
  const window   = new ReadWindow({
    subscriptionId                                                                         : 'meta',
    watchVersion                                                                           : 1,
    topic                                                                                  : 'connect.meta',
    section                                                                                : 'connection',
    collections                                                                            : spec.collections,
    /** @returns {Promise<void>} Identidad acreditada. */ async authorize() {},
    /** @returns {Promise<Object>} Consulta pública canónica. */ read                      : () =>
      spec.read(query, identity),
    /** @param {Object} frame Diferencia. @returns {number} Sólo entrega realtime. */ emit : (
      frame,
    ) => deliveries.push(frame),
  });
  const snapshot = await spec.read(query, identity);
  await window.bind({
    subscriptionId : 'meta',
    watchVersion   : 1,
    query,
    ...snapshot,
    projectionHash : contentCursor(snapshot.data),
  });
  for (let n = 2; n <= 101; n++) {
    record = {
      ...record,
      revision : n,
      name     : 'Remoto ' + n,
    };
    await window.change();
  }
  assert.equal(deliveries.length, 100);
  assert(
    deliveries.every(
      /** @param {Object} frame Entrega. @returns {boolean} El canal no entrega listas completas. */ (
        frame,
      ) => frame.type === 'patch',
    ),
  );
  assert.equal(window.base.name, 'Remoto 101');
  privateData = true;
  await assert.rejects(topics.settings.read({}, identity), { statusCode: 503 });
  privateData = false;
  permitted = false;
  await window.change();
  assert.equal(deliveries.at(-1).type, 'revoked');
  assert.equal(window.closed, true);
  assert.throws(
    /** @returns {Object} El cuerpo no puede acreditar permisos. */ () =>
      topics.settings.validate({ policies: ['connect.meta.view.global'] }),
    { statusCode: 400 },
  );
  assert.throws(
    /** @returns {Object} Sin atajos para quitar la paginación. */ () =>
      topics.connections.validate({
        page    : 1,
        perPage : 0,
      }),
    { statusCode: 400 },
  );
});
