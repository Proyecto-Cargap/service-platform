/** @file Hechos Supplier por broker y outbox reales: cada proceso recibe referencias autorizadas y recupera continuidad. */
'use strict';
const test = require('node:test'),
  assert   = require('node:assert/strict');
const mongoose = require('mongoose'),
  amqp         = require('amqplib');
const { randomUUID }               = require('node:crypto');
const { startIsolatedMongo }       = require('./helpers/isolated-mongo.cjs');
const { startIsolatedAmqp, delay } = require('./helpers/isolated-amqp.cjs');
const { MessagingRuntime }         = require('../server/library/messaging/runtime');
const { createFacade }             = require('../server/library/messaging/facade');
const { createPlatformEvents }     = require('../server/adapters/messaging/platform-events');
/** @param {Function} condition Resultado esperado. @returns {Promise<void>} Espera acotada sólo del ensayo. */
async function until(condition) {
  for (let i = 0; i < 150; i++) {
    if (condition()) return;
    await delay(40);
  }
  assert.fail('SUPPLIER_EVENT_TIMEOUT');
}
test(
  'Supplier distribuye referencias a ambas instancias y propaga discontinuidad sin sondear datos',
  {
    skip    :
      !process.env.TEST_MONGOD_BINARY ||
      !process.env.TEST_RABBITMQ_SERVER_BINARY ||
      !process.env.TEST_EPMD_BINARY,
    timeout : 60000,
  },
  /** @returns {Promise<void>} Usa infraestructura efímera y no consulta datos productivos. */ async () => {
    let db, broker, runtime;
    const subscribers = [],
      connections     = [],
      facts           = [[], []],
      supportFacts    = [[], []],
      lost            = [0, 0];
    try {
      db = await startIsolatedMongo({
        binary : process.env.TEST_MONGOD_BINARY,
        mongoose,
      });
      broker = await startIsolatedAmqp({
        rabbitmqServer : process.env.TEST_RABBITMQ_SERVER_BINARY,
        epmd           : process.env.TEST_EPMD_BINARY,
      });
      const connection = db.connection,
        namespace      = 'supplier-' + randomUUID();
      for (const [name, indexes] of Object.entries(
        require('../server/library/messaging/indexes.json'),
      ))
        for (const { key, ...options } of indexes)
          await connection.db.collection(name).createIndex(key, options);
      runtime = new MessagingRuntime({
        service : 'service-supplier',
        namespace,
        url     : broker.url,
        config  : {
          startTimeoutMs    : 10000,
          rpcTimeoutMs      : 5000,
          shutdownTimeoutMs : 5000,
        },
      });
      const publisher = createFacade(runtime);
      await runtime.start({ connection });
      for (let i = 0; i < 2; i++) {
        const subscriber = createPlatformEvents({
          url       : broker.url,
          namespace,
          windows   : {
            /** @param {string} topic Ventana relacionada. @param {Object} change Hecho validado. @returns {Promise<void>} Registra la entrega de cada proceso. */ async changed(
              topic,
              change,
            ) {
              if (topic.startsWith('support.')) {
                supportFacts[i].push({
                  topic,
                  change,
                });
                return;
              }
              facts[i].push({
                topic,
                change,
              });
            },
          },
          documents : {
            /** @returns {void} Propaga la pérdida del broker al coordinador de ventanas. */ unavailable() {
              lost[i]++;
            },
            /** @returns {Promise<void>} Recuperación habilitada exclusivamente por control. */ async recover() {},
          },
          logger    : {
            /** @param {Object} value Diagnóstico. @returns {void} Los hechos propios no deben rechazarse. */ warn(
              value,
            ) {
              assert.fail(JSON.stringify(value));
            },
          },
          /** @param {string} url Broker efímero. @param {Object} options Opciones. @returns {Promise<Object>} Conexión de prueba para provocar pérdida de continuidad. */ async connect(
            url,
            options,
          ) {
            const result = await amqp.connect(url, options);
            connections[i] = result;
            return result;
          },
        });
        subscribers.push(subscriber);
        await subscriber.start();
      }
      const fact    = {
        item : {
          supplierCode : 's',
          workspace    : 'w',
          userCreate   : 'u',
          userAssigned : '',
          revision     : 1,
          erased       : false,
        },
      };
      const session = connection.getClient().startSession();
      try {
        await session.withTransaction(
          /** @returns {Promise<void>} Confirma estado sintético y outbox juntos. */ async () => {
            await connection.db
              .collection('SUPPLIER_FANOUT_TEST')
              .insertOne({ code: 'r' }, { session });
            await publisher.publishEvent('supplier.supplier.changed', fact, { session });
          },
        );
      } finally {
        await session.endSession();
      }
      await until(
        /** @returns {boolean} Ambas instancias reciben el hecho confirmado. */
        () =>
          facts.every(
            /** @param {Array} values Entregas. @returns {boolean} Una ventana del propietario. */
            (values) => values.length === 1,
          ),
      );
      for (const values of facts)
        assert.deepEqual(values, [
          {
            topic  : 'supplier.supplier',
            change : fact,
          },
        ]);
      await connections[0].close();
      await until(
        /** @returns {boolean} Sólo el proceso desconectado pierde continuidad. */ () =>
          lost[0] === 1,
      );
      assert.equal(lost[1], 0);
      await until(
        /** @returns {boolean} La nueva cola está enlazada antes de otro hecho. */ () =>
          subscribers[0].ready(),
      );
      const next = {
        item : {
          ...fact.item,
          revision : 2,
        },
      };
      await publisher.publishEvent('supplier.supplier.changed', next);
      await until(
        /** @returns {boolean} La instancia reconectada recibe cambios posteriores. */
        () =>
          facts.every(
            /** @param {Array} values Entregas. @returns {boolean} Segundo cambio confirmado. */
            (values) => values.length === 2,
          ),
      );
      for (const values of facts)
        assert.deepEqual(values[1], {
          topic  : 'supplier.supplier',
          change : next,
        });
      assert.deepEqual(supportFacts, [[], []]);
    } finally {
      for (const subscriber of subscribers) await subscriber.close();
      await runtime?.close();
      await broker?.stop();
      await db?.stop();
    }
  },
);
