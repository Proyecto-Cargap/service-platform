/** @file Contrato de páginas y aislamiento de suscripciones documentales OAuth. */
'use strict';
const { producerWindows }         = require('./helpers/producer-window.cjs');
const { reader, readIdentity }    = require('./helpers/repository-read.cjs');
const test                        = require('node:test');
const assert                      = require('node:assert/strict');
const { createDocumentsRealtime } = require('../server/adapters/socket/documents-realtime');
const { RealtimeWindows }         = require('../server/library/realtime/windows');
const {
  documentsWindows,
} = require('../server/modules/realtime/services/documents-window.service');
const {
  documentsResourceWindows,
} = require('../server/modules/realtime/services/documents-resource-window.service');
const { registerSocketEvents } = require('../server/adapters/socket/socket');
const A = 'aaaaaaaaaaaaaaaaaaaaaaaa',
  F     = 'ffffffffffffffffffffffff';
/** @returns {Object} Socket con salas y callbacks inspeccionables. */
function socket() {
  const handlers = new Map(),
    messages     = [];
  return {
    id        : 'native',
    data      : {},
    rooms     : new Set(['native']),
    handshake : { auth: {} },
    messages,
    handlers,
    /** @param {string} name Evento. @param {Function} listener Callback. @returns {void} Registra. */ on(
      name,
      listener,
    ) {
      handlers.set(name, listener);
    },
    /** @param {string} channel Canal. @param {Object} message Entrega. @returns {void} Captura. */ emit(
      channel,
      message,
    ) {
      messages.push({
        channel,
        ...message,
      });
    },
    /** @param {string} name Sala. @returns {Promise<void>} Incorpora. */ async join(name) {
      this.rooms.add(name);
    },
    /** @param {string} name Sala. @returns {Promise<void>} Retira. */ async leave(name) {
      this.rooms.delete(name);
    },
    /** @param {string} name Operación. @param {Object} data Entrada. @returns {Promise<Object>} Acuse. */ async request(
      name,
      data,
    ) {
      let result;
      await handlers.get(name)(
        data,
        /** @param {Object} value Acuse. @returns {void} Conserva. */ (value) => {
          result = value;
        },
      );
      return result;
    },
  };
}
/** @param {Object} messaging Transporte del productor. @returns {Object} Ventanas reales con recibos efímeros de prueba. */
function hub(messaging) {
  const saved = new Map();
  return new RealtimeWindows({
    topics   : {
      'documents.view' : producerWindows(documentsWindows, { messaging }),
      ...producerWindows(documentsResourceWindows, { messaging }),
    },
    receipts : {
      /** @param {Object} value Reserva previa al HTTP. @returns {Promise<void>} Persiste una reserva. */
      async put(value) {
        saved.set(value.receiptId, value);
      },
      /** @param {string} id Recibo. @param {string} owner Principal. @returns {Promise<Object|null>} Base privada. */
      async get(id, owner) {
        const value = saved.get(id);
        return value?.owner === owner ? value : null;
      },
      /** @param {Object} value Resultado HTTP. @returns {Promise<boolean>} Reclama la reserva una sola vez. */
      async complete(value) {
        if (saved.get(value.receiptId)?.state !== 'registered') return false;
        saved.set(value.receiptId, {
          ...value,
          state : 'ready',
        });
        return true;
      },
    },
  });
}
test('web conserva página de 20 y removedIds; repositorio recibe identidad sin interpretar expulsión de página como borrado', /** @returns {Promise<void>} Distingue retiradas de una página y bajas del repositorio. */ async () => {
  const root = {
      id             : A,
      kind           : 'workspace',
      name           : 'Prueba',
      ownerCode      : '',
      workspace      : 'W1',
      usedSpace      : 0,
      availableSpace : 1000,
      sizeFileMax    : 1000,
      revision       : 1,
      capabilities   : {
        view   : true,
        create : true,
        update : true,
        delete : true,
      },
    },
    identity = {
      userCode        : 'U1',
      sessionCode     : 'S1',
      expiration      : Date.now() / 1000 + 600,
      activeWorkspace : 'W1',
      policies        : [],
      workspaces      : ['W1'],
    };
  let items       = Array.from(
    { length: 20 },
    /** @param {*} unused Vacío. @param {number} n Consecutivo. @returns {Object} Fila. */ (
      _,
      n,
    ) => ({
      id           : n.toString(16).padStart(24, '0'),
      repositoryId : A,
      parentId     : null,
      name         : `archivo-${n}`,
      type         : 'file',
      size         : 1,
      mimeType     : 'text/plain',
      modified     : 1,
      creationDate : 1,
      revision     : 1,
      sha256       : null,
      itemsCount   : 0,
      isFavorite   : false,
    }),
  );
  const messaging = {
    /** @param {string} service Destino. @param {string} action Acción. @returns {Promise<Object>} Vista canónica acotada. */ async request(
      service,
      action,
    ) {
      if (action === 'documents-entry-get-one')
        return {
          statusCode : 200,
          data       : {
            item : {
              ...items.find(
                /** @param {Object} item Entrada. @returns {boolean} Ítem cambiado. */
                (item) => item.id === F,
              ),
              repositoryId : A,
              revision     : 1,
            },
          },
        };
      return {
        statusCode : 200,
        data       :
          action === 'documents-repositories-get-list'
            ? { listItem: [root] }
            : {
                repository  : root,
                parentId    : null,
                breadcrumbs : [],
                selected    : null,
                listItem    : structuredClone(items),
                page        : 1,
                totalCount  : 100,
              },
      };
    },
  };
  const reads     = reader();
  const windows   = hub(messaging);
  const documents = createDocumentsRealtime({
    messaging,
    /** @returns {Object} Recibos correlacionados. */ reads          : () => reads,
    /** @returns {Object} Ventanas del proceso de prueba. */ windows : () => windows,
  });
  const client    = socket();
  client.data.identity = identity;
  documents.bind(client);
  documents.authenticate(
    client,
    /** @returns {Promise<Object>} Identidad web. */ async () => identity,
  );
  const view                       = {
    watchVersion : 1,
    favorites    : false,
    repositoryId : A,
    parentId     : null,
    selectedId   : null,
    search       : '',
    type         : '',
    sort         : 'name',
    order        : 'asc',
    page         : 1,
    perPage      : 20,
  };
  const { watchVersion, ...query } = view;
  const reference                  = {
    topic          : 'documents.view',
    section        : 'page',
    subscriptionId : 'page',
    watchVersion,
  };
  const ack                        = await client.request('realtime', {
    ...reference,
    type      : 'subscribe',
    requestId : 'subscribe-page',
    query,
  });
  assert.equal(ack.statusCode, 200);
  assert.equal(client.messages.length, 0, 'Subscribe nunca devuelve la lista');
  const response = await windows.read(
    {
      ...reference,
      query,
      receiptId : ack.receiptId,
    },
    {
      userCode    : 'U1',
      sessionCode : 'S1',
      workspace   : 'W1',
      context     : {},
    },
  );
  assert.equal(response.data.listItem.length, 20);
  await client.request('realtime', {
    ...reference,
    type      : 'bind',
    requestId : 'bind-page',
    receiptId : response.control.receiptId,
  });
  const repositoryReady   = await client.request('realtime', {
    type           : 'subscribe',
    requestId      : 'read-1',
    topic          : 'documents.entries',
    section        : 'repository',
    subscriptionId : 'repo',
    watchVersion   : 1,
    query          : { repositoryId: A },
  });
  const repositoryRequest = {
    subscriptionId : 'repo',
    watchVersion   : 1,
    receiptId      : repositoryReady.receiptId,
    query          : { repositoryId: A },
  };
  await reads.read(repositoryRequest, readIdentity(identity));
  await client.request('realtime', {
    ...repositoryRequest,
    topic     : 'documents.entries',
    section   : 'repository',
    type      : 'bind',
    requestId : 'bind-repository',
  });
  const removed = items[0].id;
  items = [
    {
      ...items[0],
      id   : F,
      name : 'adelante',
    },
    ...items.slice(1),
  ];
  await documents.changed(
    {
      repositoryId : A,
      entryId      : F,
      parentId     : null,
      operation    : 'upload',
      changeCode   : '1',
      revision     : 1,
    },
    'service-documents',
  );
  const patch = client.messages.find(
    /** @param {Object} value Evento. @returns {boolean} Parche web. */ (value) =>
      value.type === 'patch' && value.topic === 'documents.view',
  );
  assert.ok(
    patch.data.operations.some(
      /** @param {Object} operation Diferencia. @returns {boolean} Retirada de la ventana, sin borrar la entrada del repositorio. */
      (operation) => operation.op === 'remove' && operation.path === '/listItem/0',
    ),
  );
  assert.equal(response.data.listItem[0].id, removed);
  const invalidation = client.messages.find(
    /** @param {Object} value Evento. @returns {boolean} Repositorio. */ (value) =>
      value.type === 'patch' && value.topic === 'documents.entries',
  );
  assert.equal(invalidation.data.entryId, F);
  assert.deepEqual(invalidation.data.removedIds, []);
  documents.close();
});
test('ticket documental accede sólo a secciones documentales delegadas y renueva sin perder sus ventanas', /** @returns {Promise<void>} Frontera real de autenticación y suscripciones con RPC inyectado. */ async () => {
  let connection,
    otherAuthentications = 0;
  const principal    = {
    userCode   : 'U1',
    workspace  : 'W1',
    workspaces : ['W1'],
    policies   : [],
    grantCode  : 'G1',
    clientId   : 'C1',
    scopes     : ['documents:read'],
    channel    : 'realtime',
    expiresAt  : Date.now() + 600000,
  };
  const root         = {
    id           : A,
    kind         : 'workspace',
    name         : 'Equipo',
    workspace    : 'W1',
    revision     : 1,
    capabilities : {
      view   : true,
      create : true,
      update : true,
      delete : true,
    },
  };
  const messaging    = {
    /** @param {string} service Destino. @param {string} action Acción. @returns {Promise<Object>} Contratos de Security y Documents. */ async request(
      service,
      action,
    ) {
      if (action === 'documents-entry-get-one')
        return {
          statusCode : 200,
          data       : {
            item : {
              id           : F,
              repositoryId : A,
              revision     : 1,
            },
          },
        };
      return {
        statusCode : 200,
        data       :
          action === 'documents-repositories-get-list'
            ? {
                listItem   : [structuredClone(root)],
                totalCount : 1,
              }
            : action === 'session-authenticate'
              ? {
                  userCode                : 'U1',
                  activeWorkspace         : 'W1',
                  workspaces              : ['W1'],
                  policies                : [],
                  sessionCode             : 'web',
                  expiration              : Date.now() / 1000 + 600,
                  currentPoliciesVerified : true,
                }
              : principal,
      };
    },
  };
  const reads        = reader();
  const windows      = hub(messaging);
  const authenticate = windows.authenticate.bind(windows);
  /** @param {...*} args Conexión y verificación. @returns {void} Cuenta el coordinador estándar, con capacidades explícitas para delegación. */
  windows.authenticate = (...args) => {
    otherAuthentications++;
    authenticate(...args);
  };
  messaging.documentsRealtime = createDocumentsRealtime({
    messaging,
    /** @returns {Object} Recibos correlacionados. */ reads      : () => reads,
    /** @returns {Object} Ventanas estándar aisladas. */ windows : () => windows,
  });
  registerSocketEvents({
    io : {
      /** @param {string} name Evento. @param {Function} listener Callback. @returns {void} Captura conexión. */ on(
        name,
        listener,
      ) {
        connection = listener;
      },
    },
    messaging,
  });
  const client = socket();
  connection(client);
  for (const name of ['pipeline-watch', 'pipeline-window', 'pipeline-unwatch'])
    assert.equal(client.handlers.has(name), false);
  assert.equal((await client.request('start-session', { ticket: 'one-use' })).statusCode, 200);
  assert.equal(client.data.identity.actorType, 'documents-delegated');
  assert.equal(client.rooms.has('platform'), false);
  assert.equal(otherAuthentications, 1);
  const roots    = {
    topic          : 'documents.repository',
    section        : 'collection',
    subscriptionId : 'roots',
    watchVersion   : 1,
  };
  const rootsAck = await client.request('realtime', {
    ...roots,
    type      : 'subscribe',
    requestId : 'roots-sub',
    query     : {},
  });
  assert.equal(rootsAck.statusCode, 200);
  assert.equal(client.messages.length, 0);
  const rootRequest     = {
    ...roots,
    query     : {},
    receiptId : rootsAck.receiptId,
  };
  const catalogIdentity = {
    userCode  : 'U1',
    workspace : 'W1',
    context   : {
      delegation : {
        grantCode : 'G1',
        clientId  : 'C1',
      },
    },
  };
  await assert.rejects(
    windows.read(rootRequest, {
      ...catalogIdentity,
      context : {
        delegation : {
          grantCode : 'G2',
          clientId  : 'C1',
        },
      },
    }),
    { statusCode: 409 },
  );
  const initialRoots = await windows.read(rootRequest, catalogIdentity);
  assert.equal(initialRoots.data.listItem[0].name, 'Equipo');
  await client.request('realtime', {
    ...roots,
    type      : 'bind',
    requestId : 'roots-bind',
    receiptId : rootsAck.receiptId,
  });
  const activeRoots = windows.sessions.get(client.id).windows.get('roots');
  const registered  = await client.request('realtime', {
    type           : 'subscribe',
    requestId      : 'read-1',
    topic          : 'documents.entries',
    section        : 'repository',
    subscriptionId : 'a',
    watchVersion   : 1,
    query          : { repositoryId: A },
  });
  assert.equal(registered.statusCode, 200);
  const repositoryRequest = {
    subscriptionId : 'a',
    watchVersion   : 1,
    receiptId      : registered.receiptId,
    query          : { repositoryId: A },
  };
  await reads.read(repositoryRequest, readIdentity(client.data.identity));
  await client.request('realtime', {
    ...repositoryRequest,
    topic     : 'documents.entries',
    section   : 'repository',
    type      : 'bind',
    requestId : 'bind-repository',
  });
  assert.equal(
    (
      await client.request('realtime', {
        type           : 'subscribe',
        requestId      : 'forbidden',
        topic          : 'documents.view',
        section        : 'page',
        subscriptionId : 'bad',
        watchVersion   : 2,
        query          : {},
      })
    ).statusCode,
    403,
  );
  await client.request('start-session', { ticket: 'renewed' });
  assert.equal(windows.sessions.get(client.id).windows.get('roots'), activeRoots);
  root.name = 'Nombre actualizado';
  await messaging.documentsRealtime.changed(
    {
      repositoryId : A,
      entryId      : F,
      changeCode   : 'event',
      operation    : 'replace',
    },
    'service-documents',
  );
  assert.equal(client.messages.at(-1).delivery, 1);
  const rootPatch = client.messages.find(
    /** @param {Object} value Entrega. @returns {boolean} Catálogo delegado. */ (value) =>
      value.topic === 'documents.repository',
  );
  assert.equal(rootPatch.type, 'patch');
  assert.deepEqual(rootPatch.data.operations, [
    {
      op    : 'replace',
      path  : '/listItem/0/name',
      value : 'Nombre actualizado',
    },
  ]);
  principal.policies = ['documents.workspace.view.workspace'];
  assert.equal(
    (await client.request('start-session', { ticket: 'changed-policies' })).statusCode,
    200,
  );
  assert.equal(activeRoots.window.closed, true);
  assert.equal(windows.sessions.get(client.id).windows.size, 0);
  const renewed    = {
    ...roots,
    watchVersion : 2,
  };
  const renewedAck = await client.request('realtime', {
    ...renewed,
    type      : 'subscribe',
    requestId : 'renewed-subscribe',
    query     : {},
  });
  assert.equal(renewedAck.statusCode, 200);
  const renewedRead = await windows.read(
    {
      ...renewed,
      query     : {},
      receiptId : renewedAck.receiptId,
    },
    catalogIdentity,
  );
  assert.equal(
    (
      await client.request('realtime', {
        ...renewed,
        type      : 'bind',
        requestId : 'renewed-bind',
        receiptId : renewedRead.control.receiptId,
      })
    ).statusCode,
    200,
  );
  principal.scopes = [];
  await messaging.documentsRealtime.accessChanged({
    resource : 'grant',
    code     : 'G1',
  });
  assert.ok(
    client.messages.some(
      /** @param {Object} value Control. @returns {boolean} Retira datos de la concesión revocada. */
      (value) => value.topic === 'documents.repository' && value.type === 'revoked',
    ),
  );
  principal.scopes = ['documents:read'];
  assert.equal((await client.request('start-session', { token: 'web-session' })).statusCode, 200);
  assert.equal(otherAuthentications, 4);
  assert.ok(client.rooms.has('platform'));
  await client.request('disconnect');
  messaging.documentsRealtime.close();
});

test('los cambios de permisos del recurso no recalculan una página personal ajena', /** @returns {void} Sólo las ventanas de recurso o favoritos pueden depender de esas capacidades. */ () => {
  const spec = documentsWindows({ messaging: {} }).page;
  assert.equal(
    spec.affects(
      { favorites: false },
      { resourceAccess: true },
      {},
      { repository: { kind: 'workspace' } },
    ),
    false,
  );
  assert.equal(
    spec.affects(
      { favorites: false },
      { resourceAccess: true },
      {},
      { repository: { kind: 'resource' } },
    ),
    true,
  );
  assert.equal(spec.affects({ favorites: true }, { resourceAccess: true }, {}, {}), true);
});
