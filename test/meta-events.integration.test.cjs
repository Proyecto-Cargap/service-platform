/** @file Outbox MongoDB y AMQP reales: distribución Meta por instancia y recuperación sin sondeo. */
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
const { metaState, metaWindow }    = require('./helpers/meta-window.cjs');
/** @param {Function} condition Predicado. @param {Function} diagnostic Diagnóstico sintético opcional. @returns {Promise<void>} Espera acotada de integración. */
async function until(condition, diagnostic) {
  for (let i = 0; i < 150; i++) {
    if (condition()) return;
    await delay(40);
  }
  assert.fail('EVENT_TIMEOUT: ' + JSON.stringify(diagnostic?.()));
}
test(
  'outbox entrega a dos instancias y recupera por HTTP tras pérdida AMQP',
  {
    skip    :
      !process.env.TEST_MONGOD_BINARY ||
      !process.env.TEST_RABBITMQ_SERVER_BINARY ||
      !process.env.TEST_EPMD_BINARY,
    timeout : 60000,
  },
  /** @returns {Promise<void>} Infraestructura efímera sin credenciales reales. */ async () => {
    let db, broker, runtime;
    const subscribers = [],
      channels        = [],
      projections     = [],
      deliveries      = [[], []],
      facts           = [[], []];
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
        namespace      = 'meta-' + randomUUID();
      for (const [name, indexes] of Object.entries(
        require('../server/library/messaging/indexes.json'),
      ))
        for (const { key, ...options } of indexes)
          await connection.db.collection(name).createIndex(key, options);
      runtime = new MessagingRuntime({
        service : 'service-connect',
        namespace,
        url     : broker.url,
        config  : {
          startTimeoutMs    : 10000,
          rpcTimeoutMs      : 5000,
          shutdownTimeoutMs : 5000,
        },
        logger  : { /** @returns {void} Omite cargas. */ error() {} },
      });
      const publisher = createFacade(runtime);
      await runtime.start({ connection });
      let state = metaState();
      for (let i = 0; i < 2; i++) {
        const projection = metaWindow(
          /** @returns {Object} Fuente canónica controlada; broker y outbox reales. */ () =>
            structuredClone(state),
          'socket' + i,
        );
        projections.push(projection.windows);
        deliveries[i] = projection.deliveries;
        const subscriber = createPlatformEvents({
          url       : broker.url,
          namespace,
          windows   : {
            /** @param {string} topic Sección. @param {Object} change Hecho validado. @returns {Promise<void>} Captura fanout real antes de actualizar la ventana. */
            async changed(topic, change) {
              facts[i].push({
                topic,
                change,
              });
              await projection.windows.changed(topic, change);
            },
          },
          documents : {
            /** @returns {void} Sin vista documental. */ unavailable() {
              projection.windows.unavailable();
            },
            /** @returns {Promise<void>} Sin vista documental. */ async recover() {
              projection.windows.recover();
            },
          },
          logger    : {
            /** @param {Object} diagnostic Código técnico. @returns {void} Un evento propio nunca debe rechazarse por contrato. */ warn(
              diagnostic,
            ) {
              assert.fail(JSON.stringify(diagnostic));
            },
          },
          /** @param {string} url Conexión efímera. @param {Object} options Opciones. @returns {Promise<Object>} Guarda sólo la conexión propia para forzar una pérdida. */ async connect(
            url,
            options,
          ) {
            const value = await amqp.connect(url, options);
            channels[i] = value;
            return value;
          },
        });
        subscribers.push(subscriber);
        await subscriber.start();
        await projection.read(await projection.subscribe());
        /** @returns {Promise<Object>} Recuperación puntual representada por el Gateway del arnés. */
        projection.windows.readBase = async () =>
          projection.read(await projection.subscribe('resync'));
      }
      assert(
        deliveries.every(
          /** @param {Array} values Entregas. @returns {boolean} HTTP no genera snapshots por socket. */ (
            values,
          ) => values.length === 0,
        ),
      );
      state = {
        ...state,
        stateRevision : 1,
        deliveryState : 'read',
      };
      const session = connection.getClient().startSession();
      try {
        await session.withTransaction(
          /** @returns {Promise<void>} Estado y aviso durable atómicos. */ async () => {
            await connection.db.collection('META_TEST_STATE').insertOne({ ...state }, { session });
            await publisher.publishEvent(
              'connect.meta-conversation.changed',
              {
                chatConversationCode : 'CHAT',
                stateRevision        : 1,
              },
              {
                session,
                context : {},
              },
            );
          },
        );
      } finally {
        await session.endSession();
      }
      await until(
        /** @returns {boolean} Fanout por instancia. */ () =>
          deliveries.every(
            /** @param {Array} values Entregas. @returns {boolean} Recibió revisión. */ (values) =>
              values.at(-1)?.type === 'patch',
          ),
        /** @returns {Object} Diagnóstico sintético acotado de fanout y entregas. */ () => ({
          facts,
          deliveries,
        }),
      );
      await channels[0].close();
      state = {
        ...state,
        stateRevision : 2,
        mode          : 'human',
      };
      await until(
        /** @returns {boolean} Recuperación sin evento adicional. */ () =>
          deliveries[0].at(-1)?.type === 'resync-required',
      );
      await until(
        /** @returns {boolean} Broker disponible antes de la lectura de recuperación. */ () =>
          subscribers[0].ready(),
      );
      await projections[0].readBase();
      assert.equal(
        projections[0].sessions.get('socket0').windows.get('meta').window.base.stateRevision,
        2,
      );
      assert.equal(
        projections[1].sessions.get('socket1').windows.get('meta').window.base.stateRevision,
        1,
      );
    } finally {
      for (const subscriber of subscribers) await subscriber.close();
      for (const projection of projections) projection.close();
      await runtime?.close();
      await broker?.stop();
      await db?.stop();
    }
  },
);
