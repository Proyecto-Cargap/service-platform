/** @file Ventanas de buzón: sólo persistencia, aislamiento personal y hechos acotados por carpeta. */
'use strict';
const { producerWindows } = require('./helpers/producer-window.cjs');
const test                = require('node:test');
const assert              = require('node:assert/strict');
const { mailboxWindows }  = require('../server/modules/realtime/services/mailbox-window.service');
const dtos                = require('../server/contracts/registry');

test('las ventanas del buzón no abren búsquedas IMAP ni admiten autoridad del cliente' /** @returns {Promise<void>} Mantiene permisos, filtros y errores canónicos sin simular ausencia ante un fallo. */, async () => {
  const calls    = [];
  let response   = {
    statusCode : 200,
    data       : {
      items   : [],
      total   : 0,
      page    : 1,
      perPage : 20,
    },
  };
  const identity = {
    userCode : 'u',
    context  : { groups: ['g'] },
  };
  const sections = producerWindows(mailboxWindows, {
    messaging : {
      /** @param {string} service Productor. @param {string} action Consulta. @param {Object} query Filtros. @param {Object} options Contexto. @returns {Promise<Object>} Respuesta del productor. */
      async request(service, action, query, options) {
        calls.push({
          service,
          action,
          query,
        });
        assert.equal(options.context, identity.context);
        return response;
      },
    },
  })['connect.mailbox'];
  const query    = {
    folder  : 'INBOX',
    page    : 1,
    perPage : 20,
  };
  sections.messages.validate(query);
  assert.throws(
    /** @returns {Object} Una búsqueda remota exige su capacidad explícita. */
    () =>
      sections.messages.validate({
        ...query,
        search : 'body text',
      }),
    { statusCode: 400 },
  );
  assert.throws(
    /** @returns {Object} El propietario procede del contexto verificado. */
    () =>
      sections.messages.validate({
        ...query,
        userCode : 'another',
      }),
    { statusCode: 400 },
  );
  assert.deepEqual((await sections.messages.read(query, identity)).data, response.data);
  assert.deepEqual(calls[0], {
    service : 'service-connect',
    action  : 'mailbox-messages-list',
    query,
  });
  const change = {
    userCode  : 'u',
    mailboxId : 'box',
    section   : 'messages',
    folder    : 'INBOX',
    id        : 'm',
    kind      : 'update',
  };
  dtos['connect.mailbox.changed'].input(change);
  assert.equal(sections.messages.affects(query, change, identity, { items: [{ id: 'm' }] }), true);
  assert.equal(
    sections.messages.affects(query, change, identity, { items: [{ id: 'outside' }] }),
    false,
  );
  assert.equal(
    sections.messages.affects(
      query,
      {
        ...change,
        kind : 'membership',
      },
      identity,
      { items: [] },
    ),
    true,
  );
  assert.equal(sections.messages.affects(query, change, identity), true);
  assert.equal(
    sections.messages.affects(
      query,
      {
        ...change,
        folder : 'Trash',
      },
      identity,
    ),
    false,
  );
  assert.equal(
    sections.messages.affects(
      query,
      {
        ...change,
        userCode : 'another',
      },
      identity,
    ),
    false,
  );
  assert.equal(sections.operations.affects({ page: 1 }, change, identity), false);
  assert.equal(sections.status.affects({}, change, identity), false);
  assert.equal(
    sections.status.affects(
      {},
      {
        userCode   : 'u',
        listChange : 'none',
      },
      identity,
    ),
    true,
  );
  assert.equal(
    sections.operation.affects(
      { id: 'op' },
      {
        ...change,
        section : 'operations',
        id      : 'op',
      },
      identity,
    ),
    true,
  );
  assert.equal(
    sections.operation.affects(
      { id: 'other' },
      {
        ...change,
        section : 'operations',
        id      : 'op',
      },
      identity,
    ),
    false,
  );
  assert.equal(
    sections.messages.affects(
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
  for (const statusCode of [403, 404, 503]) {
    response = {
      statusCode,
      descriptionCode : 'CONTROL',
    };
    await assert.rejects(sections.messages.read(query, identity), { statusCode });
  }
  response = {
    statusCode : 200,
    data       : {
      ...response.data,
      secrets : 'PRIVATE',
    },
  };
  await assert.rejects(sections.messages.read(query, identity), { statusCode: 503 });
});

test('la búsqueda usa su reserva del servidor y sus patches leen sólo la materialización' /** @returns {Promise<void>} Distingue consulta HTTP, heartbeat, lectura local y vencimiento. */, async () => {
  const calls    = [];
  const page     = {
    items   : [],
    total   : 0,
    page    : 1,
    perPage : 20,
  };
  let expired    = false;
  const identity = {
    userCode : 'u',
    context  : {},
  };
  const spec     = producerWindows(mailboxWindows, {
    messaging : {
      /** @param {string} service Propietario. @param {string} action Capacidad. @param {Object} query Consulta. @returns {Promise<Object>} Control técnico o página ya materializada. */
      async request(service, action, query) {
        calls.push({
          action,
          query,
        });
        if (expired)
          return {
            statusCode      : 409,
            descriptionCode : 'MAILBOX_SEARCH_EXPIRED',
          };
        const flags = {
          'mailbox-search-open'  : 'registered',
          'mailbox-search-renew' : 'renewed',
          'mailbox-search-close' : 'released',
        };
        return {
          statusCode : 200,
          data       : flags[action] ? { [flags[action]]: true } : page,
        };
      },
    },
  })['connect.mailbox'].search;
  const query    = {
    folder  : 'INBOX',
    search  : 'body query',
    page    : 1,
    perPage : 20,
  };
  spec.validate(query);
  await spec.open(query, identity, 'server-receipt');
  await spec.renew('server-receipt', identity);
  assert.deepEqual(
    calls.map(
      /** @param {Object} call Operación. @returns {string} Control sin búsqueda externa. */
      (call) => call.action,
    ),
    ['mailbox-search-open', 'mailbox-search-renew'],
  );
  assert.deepEqual((await spec.read(query, identity, { receiptId: 'server-receipt' })).data, page);
  assert.equal(calls.at(-1).action, 'mailbox-search-execute');
  assert.deepEqual(calls.at(-1).query, { watchId: 'server-receipt' });
  assert.deepEqual(
    (await spec.readWindow(query, identity, page, [], { receiptId: 'server-receipt' })).data,
    page,
  );
  assert.equal(calls.at(-1).action, 'mailbox-search-get');
  const change = {
    userCode : 'u',
    section  : 'search',
    id       : 'server-receipt',
  };
  assert.equal(spec.affects(query, change, identity, page, { receiptId: 'server-receipt' }), true);
  assert.equal(spec.affects(query, change, identity, page, { receiptId: 'another' }), false);
  expired = true;
  await assert.rejects(
    spec.readWindow(query, identity, page, [], { receiptId: 'server-receipt' }),
    { resyncRequired: true },
  );
});

test('mensaje seleccionado mantiene su ventana aunque salga de la página y recibe su retirada', /** @returns {Promise<void>} No solicita MIME y discrimina cambios por identidad o carpeta reconciliada. */ async () => {
  const calls    = [];
  const identity = {
    userCode : 'u',
    context  : {},
  };
  const spec     = producerWindows(mailboxWindows, {
    messaging : {
      /** @param {string} service Productor. @param {string} action Lectura local. @param {Object} query Identidad. @returns {Promise<Object>} Ausencia canónica. */
      async request(service, action, query) {
        calls.push({
          service,
          action,
          query,
        });
        return {
          statusCode : 200,
          data       : null,
        };
      },
    },
  })['connect.mailbox'].message;
  const query    = spec.validate({ id: 'm' });
  assert.equal((await spec.read(query, identity)).data, null);
  assert.equal((await spec.readWindow(query, identity, {}, [])).data, null);
  assert(
    calls.every(
      /** @param {Object} call Lectura. @returns {boolean} Sólo persistencia, nunca IMAP. */
      (call) => call.action === 'mailbox-message-get',
    ),
  );
  const change = {
    userCode : 'u',
    section  : 'messages',
    folder   : 'INBOX',
    kind     : 'membership',
  };
  assert.equal(
    spec.affects(
      query,
      {
        ...change,
        id : 'm',
      },
      identity,
    ),
    true,
  );
  assert.equal(
    spec.affects(
      query,
      {
        ...change,
        id : 'other',
      },
      identity,
    ),
    false,
  );
  assert.equal(spec.affects(query, change, identity, { folder: 'INBOX' }), true);
  assert.equal(spec.affects(query, change, identity, { folder: 'Sent' }), false);
  assert.equal(
    spec.affects(
      query,
      {
        ...change,
        userCode : 'other',
      },
      identity,
    ),
    false,
  );
});
