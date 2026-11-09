/** @file Los cursores se emiten con el DTO del productor y se comprueban después de acreditar la respuesta RPC. */
'use strict';
const test = require('node:test'),
  assert   = require('node:assert/strict');
const protocol = require('../server/library/messaging/protocol');
const rpc      = require('../server/library/messaging/rpc');
const {
  issueReadReceipt,
  confirmReadReceipt,
  readSnapshot,
} = require('../server/library/messaging/read-receipt');
test('el cursor representa JSON canónico y no añade datos al contrato público' /** @returns {void} Conserva listas ordenadas, ausencia y representaciones JSON de fechas. */, () => {
  const response = {
    statusCode      : 200,
    descriptionCode : 'OK',
    data            : {
      items : [
        {
          code : 'a',
          at   : new Date(0),
        },
      ],
      total : 1,
    },
  };
  const cursor = issueReadReceipt(response),
    wire       = JSON.parse(JSON.stringify(response));
  assert.equal(
    issueReadReceipt({
      data            : wire.data,
      descriptionCode : 'OK',
      statusCode      : 200,
    }),
    cursor,
  );
  confirmReadReceipt(wire, cursor);
  assert.deepEqual(readSnapshot(wire), {
    data : wire.data,
    cursor,
  });
  assert.equal(JSON.stringify(wire), JSON.stringify(response));
  assert.deepEqual(Object.getOwnPropertySymbols(wire), []);
  const missing = {
    statusCode      : 404,
    descriptionCode : 'NOT_FOUND',
  };
  confirmReadReceipt(missing, issueReadReceipt(missing));
  assert.equal(readSnapshot(missing, null).data, null);
  assert.throws(
    /** @returns {Object} Nunca emite un recibo en el consumidor. */ () =>
      readSnapshot({ ...wire }),
    { code: 'READ_RECEIPT_REQUIRED' },
  );
  wire.data.total = 2;
  assert.throws(
    /** @returns {Object} Una proyección modificada no conserva la prueba del productor. */ () =>
      readSnapshot(wire),
    { code: 'READ_RECEIPT_CHANGED' },
  );
  assert.notEqual(issueReadReceipt([1, 2]), issueReadReceipt([2, 1]));
});
test('RPC conserva el recibo del productor y rechaza alteraciones, origen ajeno y recibos ausentes' /** @returns {void} Usa serialización, validación y recepción reales del runtime. */, () => {
  const request  = protocol.createEnvelope({
    telemetry  : null,
    kind       : 'request',
    name       : 'records-get-list',
    source     : 'service-platform',
    target     : 'service-owner',
    deadlineAt : Date.now() + 10000,
    payload    : { page: 1 },
  });
  const owner    = {
    service  : 'service-owner',
    observer : {
      /** @returns {null} Sin trazas sintéticas. */ captureCarrier() {
        return null;
      },
    },
  };
  const response = rpc._responseEnvelope.call(owner, request, {
    ok    : true,
    value : {
      statusCode      : 200,
      descriptionCode : 'OK',
      data            : {
        items : [{ code: 'one' }],
        total : 1,
      },
    },
  });
  const results  = [];
  const caller   = {
    service         : 'service-platform',
    pendingRequests : new Map([
      [
        request.correlationId,
        {
          envelope : request,
          /** @param {Error} error Rechazo. @param {Object} value Resultado. @returns {void} Captura una recepción correlacionada. */ finish(
            error,
            value,
          ) {
            results.push({
              error,
              value,
            });
          },
        },
      ],
    ]),
  };
  /** @param {Object} envelope Respuesta de prueba. @returns {void} Pasa por la frontera JSON real. */
  function receive(envelope) {
    rpc._receiveReply.call(caller, {
      properties : { correlationId: request.correlationId },
      content    : protocol.encode(envelope),
    });
  }
  receive(response);
  assert.equal(results[0].error, null);
  assert.equal(readSnapshot(results[0].value).cursor, response.readCursor);
  receive({
    ...response,
    payload : {
      ...response.payload,
      data : {
        items : [],
        total : 0,
      },
    },
  });
  assert.equal(results.at(-1).error.code, 'INVALID_REPLY');
  receive({
    ...response,
    source : 'service-foreign',
  });
  assert.equal(results.at(-1).error.code, 'INVALID_REPLY');
  const absent = { ...response };
  delete absent.readCursor;
  receive(absent);
  assert.equal(results.at(-1).error.code, 'INVALID_REPLY');
  assert.throws(
    /** @returns {Object} Los cursores no se aceptan en eventos de negocio. */ () =>
      protocol.createEnvelope({
        ...request,
        kind       : 'event',
        readCursor : response.readCursor,
      }),
    { code: 'INVALID_READ_RECEIPT' },
  );
});
