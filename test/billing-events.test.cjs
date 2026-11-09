/** @file Ventanas Billing: autorización del productor, carrera HTTP/evento y diferencias sin recargas del cliente. */
'use strict';
const { producerWindows } = require('./helpers/producer-window.cjs');
const test                = require('node:test');
const assert              = require('node:assert/strict');
const { RealtimeWindows } = require('../server/library/realtime/windows');
const { billingWindows }  = require('../server/modules/realtime/services/billing-window.service');
const {
  billingEvents,
} = require('../server/modules/notifications/services/billing-events.service');
const dtos = require('../server/modules/notifications/dtos/notifications.actions.dto');
const item = {
  code         : 'OP1',
  workspace    : 'W1',
  revision     : 1,
  userCreate   : 'U1',
  userAssigned : 'U1',
};
/** @returns {Object} Productor acreditado y recibos compartidos; utiliza el hub y los esquemas productivos. */
function fixture() {
  const receipts   = new Map();
  const deliveries = [];
  let allowed      = true;
  let reads        = 0;
  let record       = {
    ...item,
    creationDate   : 1,
    updateDate     : 1,
    kind           : 'issue',
    targetCode     : 'D1',
    status         : 'queued',
    error          : '',
    providerCode   : '',
    idempotencyKey : 'K1',
  };
  const identity   = {
    userCode    : 'U1',
    sessionCode : 'S1',
    workspace   : 'W1',
    context     : {
      user      : { userCode: 'U1' },
      workspace : 'W1',
      groups    : [],
    },
  };
  const hub        = new RealtimeWindows({
    topics   : producerWindows(billingWindows, {
      messaging : {
        /** @param {string} service Propietario. @param {string} action Consulta. @param {Object} query Filtros. @param {Object} options Contexto acreditado. @returns {Promise<Object>} Respuesta canónica y autorizada. */
        async request(service, action, query, options) {
          reads++;
          assert.equal(service, 'service-billing');
          assert.equal(options.context, identity.context);
          if (!allowed)
            return {
              statusCode      : 403,
              descriptionCode : 'FORBIDDEN',
            };
          return {
            statusCode : 200,
            data       : action.endsWith('-get-one')
              ? structuredClone(record)
              : {
                  items      : [structuredClone(record)],
                  pagination : {
                    page    : query.page,
                    perPage : query.perPage,
                    total   : 1,
                  },
                },
          };
        },
      },
    }),
    receipts : {
      /** @param {Object} value Reserva. @returns {Promise<void>} Persiste el recibo antes del ACK. */
      async put(value) {
        receipts.set(value.receiptId, value);
      },
      /** @param {string} id Recibo. @param {string} owner Principal. @returns {Promise<Object|undefined>} Reserva del mismo principal. */
      async get(id, owner) {
        const value = receipts.get(id);
        return value?.owner === owner ? value : undefined;
      },
      /** @param {Object} value Resultado HTTP. @returns {Promise<boolean>} Instala una única base sobre la reserva original. */
      async complete(value) {
        const before = receipts.get(value.receiptId);
        if (before?.state !== 'registered') return false;
        receipts.set(value.receiptId, {
          ...value,
          state : 'ready',
        });
        return true;
      },
    },
  });
  const socket     = {
    id   : 'SOCKET1',
    data : {},
    /** @returns {void} El test controla el hub sin simular el protocolo de red. */
    on() {},
    /** @param {string} channel Canal. @param {Object} value Entrega. @returns {void} Captura sólo el protocolo estándar. */
    emit(channel, value) {
      assert.equal(channel, 'realtime');
      deliveries.push(value);
    },
  };
  hub.authenticate(
    socket,
    /** @returns {Promise<Object>} Sesión vigente. */
    async () => identity,
  );
  return {
    hub,
    identity,
    deliveries,
    service : billingEvents({ windows: hub }),
    /** @returns {number} Consultas causadas por lecturas explícitas o cambios reales. */
    reads   : () => reads,
    /** @returns {void} Transición del productor durante la lectura. */
    advance() {
      record = {
        ...record,
        revision : 2,
        status   : 'running',
      };
    },
    /** @returns {void} Revoca el permiso del productor. */
    revoke() {
      allowed = false;
    },
    /** @param {Object} request Control técnico. @returns {Promise<Object>} ACK del hub real. */
    control(request) {
      return hub.control(hub.sessions.get(socket.id), request);
    },
  };
}
test('subscribe no lee listas; el evento concurrente produce patch y la revocación retira acceso', /** @returns {Promise<void>} Verifica recibo, autorización, cursor y ausencia de snapshots por socket. */ async () => {
  const f       = fixture();
  const request = {
    type           : 'subscribe',
    topic          : 'billing.operation',
    section        : 'page',
    subscriptionId : 'view',
    watchVersion   : 1,
    query          : {
      page    : 1,
      perPage : 20,
    },
  };
  const ack     = await f.control(request);
  assert.equal(f.reads(), 0);
  assert.equal(ack.data, undefined);
  const base = await f.hub.read(
    {
      ...request,
      receiptId : ack.receiptId,
    },
    f.identity,
  );
  assert.equal(base.data.items[0].status, 'queued');
  f.advance();
  await f.service.changed(
    {
      originService : 'service-billing',
      data          : {
        item : {
          ...item,
          revision : 2,
        },
      },
    },
    'operation',
  );
  await f.control({
    ...request,
    type      : 'bind',
    receiptId : ack.receiptId,
  });
  assert.equal(f.deliveries.at(-1).type, 'patch');
  assert.ok(JSON.stringify(f.deliveries.at(-1)).includes('running'));
  assert.equal(
    f.deliveries.some(
      /** @param {Object} delivery Entrega. @returns {boolean} Los sockets no transportan bases completas. */
      (delivery) => delivery.type === 'snapshot',
    ),
    false,
  );
  const before = f.reads();
  await f.service.changed(
    {
      originService : 'service-billing',
      data          : {
        item : {
          ...item,
          workspace : 'W2',
        },
      },
    },
    'operation',
  );
  assert.equal(f.reads(), before);
  f.revoke();
  await f.hub.changed('billing.operation', {
    control : {
      kind : 'user',
      code : 'U1',
    },
  });
  assert.equal(f.deliveries.at(-1).type, 'revoked');
  f.hub.close();
});
test('El contrato cerrado rechaza autoridad del cliente, datos privados y orígenes ajenos', /** @returns {Promise<void>} No admite consultas arbitrarias ni avisos con información fiscal. */ async () => {
  const f = fixture();
  for (const resource of ['document', 'operation', 'issuer', 'expense', 'pac']) {
    const dto = dtos['billing.' + resource + '.changed'];
    assert.deepEqual(dto.input({ item }), { item });
    assert.throws(
      /** @returns {Object} Aviso inválido deliberado. */
      () =>
        dto.input({
          item : {
            ...item,
            total : '100.00',
          },
        }),
    );
  }
  assert.throws(
    /** @returns {Promise<void>} El origen no lo acredita el cuerpo. */
    () =>
      f.service.changed({
        originService : 'service-connect',
        data          : { item },
      }),
    /FORBIDDEN/,
  );
  await assert.rejects(
    f.control({
      type           : 'subscribe',
      topic          : 'billing.operation',
      section        : 'page',
      subscriptionId : 'invalid',
      watchVersion   : 1,
      query          : { workspace: 'W2' },
    }),
    /INVALID_BILLING_QUERY/,
  );
  f.hub.close();
});
