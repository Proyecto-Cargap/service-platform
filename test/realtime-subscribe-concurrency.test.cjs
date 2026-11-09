/** @file Aperturas concurrentes comparten sólo la acreditación pendiente, sin listas ni permisos cacheados. */
'use strict';
const test                = require('node:test');
const assert              = require('node:assert/strict');
const { RealtimeWindows } = require('../server/library/realtime/windows');

test('aperturas simultáneas no multiplican Security y el siguiente grupo verifica de nuevo', /** @returns {Promise<void>} Acredita aislamiento, ausencia de datos y revocación entre aperturas. */ async () => {
  let calls = 0,
    release;
  const identity = {
    userCode    : 'u',
    workspace   : 'w',
    sessionCode : 's',
  };
  const hub      = new RealtimeWindows({
    topics   : {
      topic : {
        page : {
          /** @param {Object} query Selector. @returns {Object} Consulta. */ validate : (query) =>
            query,
          /** @returns {Promise<Object>} Abrir no consulta datos. */ async read() {
            throw new Error('UNEXPECTED_READ');
          },
        },
      },
    },
    receipts : {
      /** @returns {Promise<void>} Sólo conserva metadatos de control. */ async put() {},
    },
  });
  const socket   = {
    id : 'one',
    /** @returns {void} Transporte mínimo. */ on() {},
  };
  hub.authenticate(
    socket,
    /** @returns {Promise<Object>} Security pendiente. */ () => {
      calls++;
      return new Promise(
        /** @param {Function} resolve Continuación. @returns {*} Control del fixture. */ (
          resolve,
        ) => {
          release = resolve;
        },
      );
    },
  );
  const session = hub.sessions.get(socket.id);
  const opening = Promise.all(
    Array.from(
      { length: 6 },
      /** @param {*} unused Valor sin uso. @param {number} i Ventana. @returns {Promise<Object>} Apertura concurrente. */ (
        _,
        i,
      ) =>
        hub.control(session, {
          type           : 'subscribe',
          topic          : 'topic',
          section        : 'page',
          subscriptionId : String(i),
          watchVersion   : 1,
          query          : {},
        }),
    ),
  );
  await new Promise(
    /** @param {Function} resolve Continuación. @returns {*} Control del fixture. */ (resolve) =>
      setImmediate(resolve),
  );
  assert.equal(calls, 1);
  release(identity);
  const acks = await opening;
  assert.ok(
    acks.every(
      /** @param {Object} ack Confirmación. @returns {boolean} Sin datos. */ (ack) =>
        ack.statusCode === 200 && !Object.hasOwn(ack, 'data'),
    ),
  );
  /** @returns {Promise<Object>} Rechazo fresco después de terminar la acreditación anterior. */
  session.verify = async () => {
    calls++;
    throw Object.assign(new Error('REVOKED'), { statusCode: 401 });
  };
  await assert.rejects(
    hub.control(session, {
      type           : 'subscribe',
      topic          : 'topic',
      section        : 'page',
      subscriptionId : 'later',
      watchVersion   : 1,
      query          : {},
    }),
    { statusCode: 401 },
  );
  assert.equal(calls, 2);
  /** @returns {Promise<Object>} Nuevo resultado después del rechazo anterior. */
  session.verify = async () => identity;
  assert.equal(
    (
      await hub.control(session, {
        type           : 'subscribe',
        topic          : 'topic',
        section        : 'page',
        subscriptionId : 'retry',
        watchVersion   : 1,
        query          : {},
      })
    ).statusCode,
    200,
  );
  hub.close();
});
