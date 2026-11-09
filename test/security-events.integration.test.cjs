/** @file Hechos Security por broker y outbox reales: cada proceso recibe referencias autorizadas y recupera continuidad. */
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
  assert.fail('SECURITY_EVENT_TIMEOUT');
}
test(
  'Security distribuye referencias a ambas instancias y propaga discontinuidad sin sondear datos',
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
        namespace      = 'security-' + randomUUID();
      for (const [name, indexes] of Object.entries(
        require('../server/library/messaging/indexes.json'),
      ))
        for (const { key, ...options } of indexes)
          await connection.db.collection(name).createIndex(key, options);
      runtime = new MessagingRuntime({
        service : 'service-security',
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
              if (topic.startsWith('support.')) return;
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
        resource : 'user',
        code     : 'u',
        revision : 1,
      };
      const session = connection.getClient().startSession();
      try {
        await session.withTransaction(
          /** @returns {Promise<void>} Confirma estado sintético y outbox juntos. */ async () => {
            await connection.db
              .collection('SECURITY_FANOUT_TEST')
              .insertOne({ code: 'r' }, { session });
            await publisher.publishEvent('security.access.changed', fact, { session });
          },
        );
      } finally {
        await session.endSession();
      }
      await until(
        /** @returns {boolean} Ambas instancias reciben catálogo y contactos relacionados. */ () =>
          facts.every(
            /** @param {Array} values Entregas. @returns {boolean} Seis ventanas afectadas. */ (
              values,
            ) => values.length === 6,
          ),
      );
      for (const values of facts) {
        assert.deepEqual(
          values.map(
            /** @param {Object} value Entrega. @returns {string} Tópico. */ (value) => value.topic,
          ),
          [
            'security.user',
            'security.directory',
            'security.access',
            'security.session',
            'security.profile',
            'customer.pipeline',
          ],
        );
        assert.deepEqual(values[0].change, fact);
      }
      await publisher.publishEvent('security.access.changed', {
        resource : 'grant',
        code     : 'grant',
        userCode : 'u',
        revision : 1,
      });
      await until(
        /** @returns {boolean} Ambos procesos actualizan exclusivamente conexiones propias mediante el tópico canónico. */
        () =>
          facts.every(
            /** @param {Array} values Entregas. @returns {boolean} Incluye la referencia personal. */
            (values) =>
              values.length === 9 &&
              values.some(
                /** @param {Object} value Entrega. @returns {boolean} Concesión y titular acreditados. */
                (value) => value.topic === 'security.connection' && value.change.userCode === 'u',
              ),
          ),
      );
      for (const values of facts) values.splice(6);
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
      await publisher.publishEvent('security.resource.changed', {
        resource : 'settings',
        code     : 'session-setings',
        revision : 2,
      });
      await until(
        /** @returns {boolean} Configuración y visibilidad reciben el cambio en ambas instancias. */ () =>
          facts.every(
            /** @param {Array} values Entregas. @returns {boolean} Un hecho nuevo. */ (values) =>
              values.length === 7,
          ),
      );
      for (const values of facts)
        assert.deepEqual(
          values
            .slice(6)
            .map(
              /** @param {Object} value Entrega. @returns {string} Tópico. */ (value) =>
                value.topic,
            ),
          ['security.settings'],
        );
    } finally {
      for (const subscriber of subscribers) await subscriber.close();
      await runtime?.close();
      await broker?.stop();
      await db?.stop();
    }
  },
);
