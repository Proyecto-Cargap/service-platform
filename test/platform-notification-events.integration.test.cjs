/** @file Outbox de notificaciones y difusión a dos procesos Platform mediante MongoDB/RabbitMQ aislados. */
'use strict';
const test                         = require('node:test');
const assert                       = require('node:assert/strict');
const mongoose                     = require('mongoose');
const amqp                         = require('amqplib');
const { randomUUID }               = require('node:crypto');
const { startIsolatedMongo }       = require('./helpers/isolated-mongo.cjs');
const { startIsolatedAmqp, delay } = require('./helpers/isolated-amqp.cjs');
const { MessagingRuntime }         = require('../server/library/messaging/runtime');
const { createFacade }             = require('../server/library/messaging/facade');
const { createPlatformEvents }     = require('../server/adapters/messaging/platform-events');
/** @param {Function} condition Condición del ensayo. @returns {Promise<void>} Espera acotada de infraestructura de prueba. */
async function until(condition) {
  for (let i = 0; i < 200; i++) {
    if (condition()) return;
    await delay(40);
  }
  assert.fail('PLATFORM_EVENT_TIMEOUT');
}
test(
  'una entrega concurrente produce un aviso durable y ambas instancias reciben el hecho; una desconexión afecta sólo a su proceso',
  {
    skip    :
      !process.env.TEST_MONGOD_BINARY ||
      !process.env.TEST_RABBITMQ_SERVER_BINARY ||
      !process.env.TEST_EPMD_BINARY,
    timeout : 60000,
  },
  /** @returns {Promise<void>} Conecta repositorios reales con el outbox portable y el fanout de producción. */ async () => {
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
        namespace      = 'platform-notifications-' + randomUUID();
      for (const [name, indexes] of Object.entries(
        require('../server/library/messaging/indexes.json'),
      ))
        for (const { key, ...options } of indexes)
          await connection.db.collection(name).createIndex(key, options);
      runtime = new MessagingRuntime({
        service : 'service-platform',
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
      const original = require('../server/modules/notifications/models/notifications.model');
      const model    = connection.model(original.modelName, original.schema, original.collection.name);
      await model.createCollection();
      await model.createIndexes();
      const repository =
        require('../server/modules/notifications/repositories/notifications.repository')({
          model,
          messaging : publisher,
        });
      for (let i = 0; i < 2; i++) {
        const subscriber = createPlatformEvents({
          url       : broker.url,
          namespace,
          windows   : {
            /** @param {string} topic Dominio. @param {Object} change Referencia durable. @returns {Promise<void>} Cada proceso recibe su propia entrega. */ async changed(
              topic,
              change,
            ) {
              facts[i].push({
                topic,
                change,
              });
            },
          },
          documents : {
            /** @returns {void} Control de discontinuidad de este proceso. */ unavailable() {
              lost[i]++;
            },
            /** @returns {Promise<void>} Cola enlazada antes de nuevas lecturas. */ async recover() {},
          },
          logger    : {
            /** @param {Object} value Diagnóstico. @returns {void} No se admiten hechos inválidos. */ warn(
              value,
            ) {
              assert.fail(JSON.stringify(value));
            },
          },
          /** @param {string} url Broker efímero. @param {Object} options Opciones técnicas. @returns {Promise<Object>} Conexión propia para ensayar una interrupción. */ async connect(
            url,
            options,
          ) {
            const value = await amqp.connect(url, options);
            connections[i] = value;
            return value;
          },
        });
        subscribers.push(subscriber);
        await subscriber.start();
      }
      const input = {
        sourceEventKey   : 'e1',
        notificationCode : 'n1',
        workspace        : 'w',
        userCode         : 'alice',
        userAssigned     : 'alice',
        userCreate       : '',
        title            : 'Confidencial',
        content          : 'No aparece en el hecho',
        type             : 'info',
      };
      await Promise.all(
        Array.from(
          { length: 8 },
          /** @returns {Promise<Object>} Simula reentrega a instancias concurrentes. */ () =>
            repository.createEvent(input),
        ),
      );
      await until(
        /** @returns {boolean} Un hecho por proceso. */ () =>
          facts.every(
            /** @param {Array} rows Entregas. @returns {boolean} Recibió el alta. */ (rows) =>
              rows.length === 1,
          ),
      );
      assert.equal(await model.countDocuments(), 1);
      for (const rows of facts)
        assert.deepEqual(rows, [
          {
            topic  : 'platform.notification',
            change : {
              resource  : 'notification',
              code      : 'n1',
              userCode  : 'alice',
              workspace : 'w',
              created   : true,
              erased    : false,
            },
          },
        ]);
      await connections[0].close();
      await until(
        /** @returns {boolean} Sólo el proceso afectado pierde continuidad. */ () => lost[0] === 1,
      );
      assert.equal(lost[1], 0);
      await until(
        /** @returns {boolean} La cola nueva ya recibe hechos. */ () => subscribers[0].ready(),
      );
      await repository.createEvent({
        ...input,
        sourceEventKey   : 'e2',
        notificationCode : 'n2',
      });
      await until(
        /** @returns {boolean} Ambas instancias reciben el siguiente hecho. */ () =>
          facts.every(
            /** @param {Array} rows Entregas. @returns {boolean} Segunda alta presente. */ (rows) =>
              rows.length === 2,
          ),
      );
      assert.equal(JSON.stringify(facts).includes('Confidencial'), false);
      assert.equal(await model.countDocuments(), 2);
    } finally {
      for (const subscriber of subscribers) await subscriber.close();
      await runtime?.close();
      await broker?.stop();
      await db?.stop();
    }
  },
);
