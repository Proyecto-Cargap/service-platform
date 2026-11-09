/** @file Correlación HTTP/repositorio: páginas autorizadas, carreras de lectura y buffers anteriores al bind. */
'use strict';
const test                     = require('node:test');
const assert                   = require('node:assert/strict');
const { reader, readIdentity } = require('./helpers/repository-read.cjs');
const { repositoryWatches }    = require('../server/adapters/socket/documents-repositories-realtime');
const A = 'aaaaaaaaaaaaaaaaaaaaaaaa',
  B     = 'bbbbbbbbbbbbbbbbbbbbbbbb';
const principal = {
  userCode        : 'U1',
  activeWorkspace : 'W1',
  delegation      : {
    grantCode : 'G1',
    clientId  : 'C1',
  },
};
test('las páginas y el bind exigen la misma concesión, consulta y generación; el socket sólo entrega cambios posteriores', /** @returns {Promise<void>} Productor y verificación controlados, protocolo y recibos reales. */ async () => {
  const messages = [],
    calls        = [];
  let revision  = 7;
  const reads   = reader({
    /** @param {Object} query Consulta autorizada por la reserva. @returns {Promise<Object>} Dos páginas emitidas por el productor. */
    async manifest(query) {
      calls.push(query);
      return {
        statusCode : 200,
        data       : {
          repository  : { id: A },
          revision,
          items       : query.cursor
            ? [
                {
                  id       : B,
                  revision : 1,
                },
              ]
            : [],
          nextCursor  : query.cursor ? null : 'next-7',
          notModified : false,
        },
      };
    },
  });
  const watches = repositoryWatches({
    /** @returns {Object} Misma persistencia para HTTP y socket. */ reads : () => reads,
    messaging                                                             : {
      /** @param {string} service Destino. @param {string} action Autorización o detalle. @returns {Promise<Object>} Proyecciones públicas. */
      async request(service, action) {
        return {
          statusCode : 200,
          data       :
            action === 'documents-entry-get-one'
              ? {
                  item : {
                    id           : B,
                    repositoryId : A,
                    revision     : 2,
                  },
                }
              : {
                  listItem : [
                    {
                      id           : A,
                      revision,
                      capabilities : { view: true },
                    },
                  ],
                },
        };
      },
    },
  });
  const socket  = {
    id : 'socket',
    /** @param {string} channel Canal. @param {Object} value Entrega. @returns {void} Captura exclusivamente realtime. */
    emit(channel, value) {
      assert.equal(channel, 'realtime');
      messages.push(value);
    },
  };
  watches.authenticate(
    socket,
    /** @returns {Promise<Object>} Principal vigente. */ async () => principal,
  );
  const ready = await watches.watch(socket, {
    subscriptionId : 'repo',
    repositoryId   : A,
    watchVersion   : 1,
  });
  assert.equal(calls.length, 0);
  assert.equal(ready.data, undefined);
  const request  = {
    subscriptionId : 'repo',
    watchVersion   : 1,
    receiptId      : ready.receiptId,
    query          : { repositoryId: A },
  };
  const identity = readIdentity(principal);
  await assert.rejects(watches.bind(socket, request), { statusCode: 409 });
  for (const context of [
    {
      ...identity.context,
      workspace : 'W2',
    },
    {
      ...identity.context,
      user : { userCode: 'U2' },
    },
    {
      ...identity.context,
      delegation : {
        grantCode : 'G2',
        clientId  : 'C1',
      },
    },
    {
      ...identity.context,
      delegation : {
        grantCode : 'G1',
        clientId  : 'C2',
      },
    },
  ])
    await assert.rejects(reads.read(request, { context }), { statusCode: 409 });
  await assert.rejects(
    reads.read(
      {
        ...request,
        query : { repositoryId: B },
      },
      identity,
    ),
    { statusCode: 409 },
  );
  await assert.rejects(
    reads.read(
      {
        ...request,
        watchVersion : 2,
      },
      identity,
    ),
    { statusCode: 409 },
  );
  const first = await reads.read(request, identity);
  assert.equal(first.data.nextCursor, 'next-7');
  assert.equal(first.control.queryFingerprint, ready.queryFingerprint);
  await assert.rejects(reads.read(request, identity), { statusCode: 409 });
  await assert.rejects(watches.bind(socket, request), { statusCode: 409 });
  await watches.changed({
    repositoryId       : A,
    entryId            : B,
    operation          : 'replace',
    revision           : 1,
    repositoryRevision : 7,
    changeCode         : 'included-in-read',
  });
  assert.equal(messages.length, 0);
  const final = await reads.read(
    {
      ...request,
      pageCursor : 'next-7',
    },
    identity,
  );
  revision = 8;
  await watches.changed({
    repositoryId       : A,
    entryId            : B,
    operation          : 'replace',
    revision           : 2,
    repositoryRevision : 8,
    changeCode         : 'after-read',
  });
  assert.equal(messages.length, 0);
  const bound = await watches.bind(socket, request);
  assert.equal(bound.cursor, final.control.cursor);
  assert.equal(messages.length, 1);
  assert.equal(messages[0].data.changeCode, 'after-read');
  assert.equal(messages[0].delivery, 1);
  assert.equal(messages[0].queryFingerprint, ready.queryFingerprint);
  await assert.rejects(watches.bind(socket, request), { statusCode: 409 });
  assert.equal(calls.length, 2);
  watches.close();
});

test('lecturas simultáneas no avanzan dos veces; otra revisión entre páginas invalida el manifiesto completo', /** @returns {Promise<void>} Simula dos instancias HTTP compartiendo el recibo. */ async () => {
  let release;
  const gate   = new Promise(
    /** @param {Function} resolve Barrera. @returns {void} Retiene ambas lecturas. */ (resolve) => {
      release = resolve;
    },
  );
  let revision = 4;
  const reads  = reader({
    /** @param {Object} query Página. @returns {Promise<Object>} Productor retenido durante la carrera. */
    async manifest(query) {
      await gate;
      return {
        statusCode : 200,
        data       : {
          repository  : { id: A },
          items       : [{ id: B }],
          revision,
          nextCursor  : query.cursor ? null : 'next',
          notModified : false,
        },
      };
    },
  });
  const identity = readIdentity(principal),
    registration = {
      subscriptionId : 'repo',
      watchVersion   : 1,
      repositoryId   : A,
    };
  const ready   = await reads.register(registration, identity);
  const request = {
    ...registration,
    receiptId : ready.receiptId,
    query     : { repositoryId: A },
  };
  const jobs    = [reads.read(request, identity), reads.read(request, identity)];
  release();
  const results = await Promise.allSettled(jobs);
  assert.equal(
    results.filter(
      /** @param {Object} result Resultado. @returns {boolean} Sólo un ganador. */ (result) =>
        result.status === 'fulfilled',
    ).length,
    1,
  );
  revision++;
  await assert.rejects(
    reads.read(
      {
        ...request,
        pageCursor : 'next',
      },
      identity,
    ),
    { statusCode: 409 },
  );
  await assert.rejects(reads.bind(request, identity), { statusCode: 409 });
});
