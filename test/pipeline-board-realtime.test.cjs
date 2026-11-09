/** @file Dos ventanas estándar de tablero: bases HTTP, cambios por ítem y revocación aislada sin canales históricos. */
'use strict';
const test                = require('node:test');
const assert              = require('node:assert/strict');
const { ReadWindow }      = require('../server/library/realtime/read-window');
const { produceSnapshot } = require('../server/library/realtime/snapshot');
const { contentCursor }   = require('../server/library/realtime/window-delta');
const { customerWindows } = require('../server/modules/realtime/services/customer-window.service');
const { manifest }        = require('../server/contracts');
test('dos clientes reciben diferencias de columna, duplicados y revocación por realtime estándar', /** @returns {Promise<void>} Las suscripciones no leen; sólo los hechos activan proyecciones del productor y cada base procede de HTTP. */ async () => {
  const spec     = customerWindows({ messaging: {} })['customer.pipeline'].board;
  const messages = [[], []];
  const allowed  = [true, true];
  let httpReads   = 0,
    producerReads = 0;
  let data      = {
    windows : [
      {
        stageCode : 'open',
        limit     : 20,
        items     : [
          {
            opportunityCode : 'o',
            title           : 'Original',
            revision        : 1,
          },
        ],
      },
    ],
    totals  : [
      {
        stageCode : 'open',
        currency  : 'MXN',
        amount    : '10',
        count     : 1,
      },
    ],
  };
  const windows = messages.map(
    /** @param {Array} sent Entregas. @param {number} index Sesión. @returns {Object} Coordinador estándar independiente. */
    (sent, index) =>
      new ReadWindow({
        subscriptionId : 'board-' + index,
        watchVersion   : 1,
        topic          : 'customer.pipeline',
        section        : 'board',
        collections    : spec.collections,
        /** @returns {Promise<void>} Revocación vigente, sin datos del cliente como autoridad. */
        async authorize() {
          if (!allowed[index]) throw Object.assign(new Error('FORBIDDEN'), { statusCode: 403 });
        },
        /** @returns {Promise<Object>} Proyección causada por cambios reales. */
        async read() {
          producerReads++;
          return produceSnapshot(structuredClone(data));
        },
        /** @param {Object} value Sobre común. @returns {void} Captura del canal realtime. */
        emit(value) {
          sent.push(value);
        },
      }),
  );
  try {
    for (const window of windows) {
      assert.deepEqual(Object.keys(window.ready()).sort(), [
        'statusCode',
        'subscriptionId',
        'watchVersion',
      ]);
      assert.equal(producerReads, 0);
      httpReads++;
      const snapshot = produceSnapshot(structuredClone(data));
      await window.bind({
        ...snapshot,
        projectionHash : contentCursor(snapshot.data),
        subscriptionId : window.subscriptionId,
        watchVersion   : 1,
        query          : {
          pipelineCode : 'p',
          windows      : [
            {
              stageCode : 'open',
              limit     : 20,
            },
          ],
        },
      });
    }
    assert.deepEqual(messages, [[], []]);
    for (let revision = 2; revision <= 101; revision++) {
      data.windows[0].items[0] = {
        opportunityCode : 'o',
        title           : 'Cambio ' + revision,
        revision,
      };
      for (const window of windows)
        await window.change({
          resource : 'opportunity',
          code     : 'o',
          revision,
        });
    }
    for (const sent of messages) {
      assert.equal(sent.length, 100);
      for (const event of sent) {
        assert.equal(event.type, 'patch');
        assert.equal(event.topic, 'customer.pipeline');
        assert.ok(
          event.data.operations.every(
            /** @param {Object} operation Diferencia. @returns {boolean} No entrega otra lista ni sustituye toda la columna. */
            (operation) => operation.path.startsWith('/windows/0/items/0/'),
          ),
        );
      }
    }
    for (const window of windows)
      await window.change({
        resource : 'opportunity',
        code     : 'o',
        revision : 101,
      });
    assert.equal(messages[0].length, 100);
    allowed[0] = false;
    for (const window of windows)
      await window.change({
        resource : 'customer',
        code     : 'c',
        revision : 2,
      });
    assert.equal(messages[0].at(-1).type, 'revoked');
    assert.equal(windows[0].closed, true);
    assert.equal(windows[1].closed, false);
    data = {
      windows : [
        {
          stageCode : 'open',
          limit     : 20,
          items     : [],
        },
      ],
      totals  : [],
    };
    for (const window of windows)
      await window.change({
        resource : 'opportunity',
        code     : 'o',
        revision : 102,
        erased   : true,
      });
    assert.equal(messages[0].at(-1).type, 'revoked');
    assert.ok(
      messages[1].at(-1).data.operations.some(
        /** @param {Object} operation Diferencia. @returns {boolean} Retira sólo la tarjeta afectada. */
        (operation) => operation.op === 'remove' && operation.path === '/windows/0/items/0',
      ),
    );
    assert.equal(httpReads, 2);
    assert.ok(producerReads > 0);
  } finally {
    for (const window of windows) window.close();
  }
});
test('CRM retira sus contratos de invalidación y los canales históricos', /** @returns {void} No ofrece aliases para clientes anteriores. */ () => {
  for (const resource of [
    'customer',
    'pipeline-type',
    'pipeline',
    'pipeline-board',
    'opportunity',
    'activity',
  ]) {
    assert.equal(
      manifest.endpoints.some(
        /** @param {Object} endpoint Contrato registrado. @returns {boolean} Nombre retirado. */
        (endpoint) => endpoint.name === 'customer.' + resource + '.changed',
      ),
      false,
    );
  }
});
