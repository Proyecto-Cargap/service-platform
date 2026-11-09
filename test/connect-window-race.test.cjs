/** @file Un ítem que entra durante una consulta no pierde una edición posterior por filtrar contra la base anterior. */
'use strict';
const { producerWindows } = require('./helpers/producer-window.cjs');
const test                = require('node:test');
const assert              = require('node:assert/strict');
const { RealtimeWindows } = require('../server/library/realtime/windows');
const { ReadWindow }      = require('../server/library/realtime/read-window');
const { contentCursor }   = require('../server/library/realtime/window-delta');
const { connectWindows }  = require('../server/modules/realtime/services/connect-window.service');
/** @param {string} code Identidad. @param {string} description Texto público. @param {number} total Total de la consulta. @returns {Object} Página de una fila con el contrato canónico. */
function page(code, description, total) {
  return {
    items            : [
      {
        id        : 'personal:' + code,
        type      : 'personal',
        userCode  : code,
        email     : code + '@example.test',
        description,
        host      : 'smtp.example.test',
        port      : 587,
        active    : true,
        receiving : false,
      },
    ],
    pagination       : {
      page    : 1,
      perPage : 1,
      total,
    },
    globalConfigured : false,
  };
}
test('una edición concurrente de una fila entrante provoca una segunda proyección acotada' /** @returns {Promise<void>} Combina el registro de Connect y los coordinadores reales; no sustituye el selector de ventanas. */, async () => {
  let finish, entered;
  const hold    = new Promise(
    /** @param {Function} resolve Continuación. @returns {void} Retiene la primera consulta después de capturar su snapshot. */ (
      resolve,
    ) => {
      finish = resolve;
    },
  );
  const started = new Promise(
    /** @param {Function} resolve Continuación. @returns {void} Confirma que la primera lectura ya tomó datos. */ (
      resolve,
    ) => {
      entered = resolve;
    },
  );
  let data = page('a', 'Original', 1),
    reads  = 0;
  const topics   = producerWindows(connectWindows, {
    messaging : {
      /** @returns {Promise<Object>} Productor controlado cuya primera respuesta tarda mientras llegan nuevos hechos. */
      async request() {
        const snapshot = structuredClone(data);
        if (++reads === 1) {
          entered();
          await hold;
        }
        return {
          statusCode : 200,
          data       : snapshot,
        };
      },
    },
  });
  const registry = new RealtimeWindows({
    topics,
    receipts : {},
  });
  const identity = {
    userCode : 'admin',
    context  : {},
  };
  const query    = {
    page    : 1,
    perPage : 1,
  };
  const window   = new ReadWindow({
    subscriptionId : 'page',
    watchVersion   : 1,
    topic          : 'connect.mail-account',
    section        : 'page',
    collections    : { '/items': 'id' },
    /** @returns {Promise<void>} Identidad acreditada del arnés. */ async authorize() {},
    /** @returns {Promise<Object>} Usa el productor canónico del registro real. */ read() {
      return topics['connect.mail-account'].page.read(query, identity);
    },
    /** @returns {void} La prueba comprueba la última base; los patches se ejercitan en navegador. */ emit() {},
  });
  registry.sessions.set('local', {
    windows : new Map([
      [
        'page',
        {
          window,
          query,
          identity,
        },
      ],
    ]),
  });
  await window.bind({
    subscriptionId : 'page',
    watchVersion   : 1,
    data,
    query,
    cursor         : contentCursor(data),
    projectionHash : contentCursor(data),
  });
  data = page('b', 'Al entrar', 2);
  const first = registry.changed('connect.mail-account', {
    userCode   : 'b',
    listChange : 'membership',
  });
  await started;
  data = page('b', 'Edición concurrente', 2);
  const second = registry.changed('connect.mail-account', {
    userCode   : 'b',
    listChange : 'update',
  });
  finish();
  await Promise.all([first, second]);
  assert.equal(reads, 2);
  assert.equal(window.base.items[0].description, 'Edición concurrente');
  await registry.changed('connect.mail-account', {
    userCode   : 'outside',
    listChange : 'update',
  });
  assert.equal(reads, 2);
  registry.close();
});
