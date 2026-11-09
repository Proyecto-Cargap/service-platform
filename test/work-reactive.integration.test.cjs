/** @file Comprueba con MongoDB real vencimientos, reposo, reinicios y exclusión de trabajadores reactivos. */
'use strict';
const test                          = require('node:test');
const assert                        = require('node:assert/strict');
const mongoose                      = require('mongoose');
const { randomUUID }                = require('node:crypto');
const { setTimeout: delay }         = require('node:timers/promises');
const { startIsolatedMongo }        = require('./helpers/isolated-mongo.cjs');
const { createReactiveWorker }      = require('../server/library/work/reactive-worker');
const { createMongoChanges }        = require('../server/library/work/mongo-changes');
const { createPushModels }          = require('../server/modules/push/models/push.model');
const { createPushRepository }      = require('../server/modules/push/repositories/push.repository');
const { createPushDelivery }        = require('../server/modules/push/services/push-delivery.service');
const createNotificationsRepository = require('../server/modules/notifications/repositories/notifications-center.repository');
const {
  createNotificationsWorker,
} = require('../server/modules/notifications/workers/notifications-worker');

/** @param {Function} condition Aserción observable. @returns {Promise<void>} Espera acotada exclusiva del arnés de pruebas. */
async function until(condition) {
  const deadline = Date.now() + 15000;
  while (!(await condition())) {
    if (Date.now() >= deadline) assert.fail('El efecto esperado no se produjo.');
    await delay(20);
  }
}
const idleMs = Number(process.env.TEST_WORK_IDLE_MS || 500);

test(
  'push: dos instancias reaccionan a cambios y deadlines, sin lecturas durante el reposo',
  {
    skip    : !process.env.TEST_MONGOD_BINARY,
    timeout : idleMs + 60000,
  },
  /** @returns {Promise<void>} No usa cuentas ni proveedores reales. */ async () => {
    const db = await startIsolatedMongo({
      binary : process.env.TEST_MONGOD_BINARY,
      mongoose,
    });
    const workers = [],
      failures    = [],
      sent        = [],
      streams     = [];
    const connection = {
      db : {
        /** @param {Array} pipeline Filtro del trabajador. @param {Object} options Opciones del driver. @returns {Object} Cursor real cuya discontinuidad puede controlarse. */
        watch(pipeline, options) {
          const stream = db.connection.db.watch(pipeline, options);
          streams.push(stream);
          return stream;
        },
      },
    };
    const logger     = {
      /** @param {Object} error Categoría segura. @returns {void} Conserva evidencia. */ error(
        error,
      ) {
        failures.push(error);
      },
    };
    let queries      = 0;
    try {
      const models = createPushModels({
        Schema : mongoose.Schema,
        models : db.connection.models,
        model  : db.connection.model.bind(db.connection),
      });
      for (const model of Object.values(models)) {
        await model.createCollection();
        await model.createIndexes();
      }
      const repository = createPushRepository({
        ...models,
        newId                                                  : randomUUID,
        /** @returns {Date} Reloj real del vencimiento. */ now : () => new Date(),
      });
      const delivery   = createPushDelivery({
        repository,
        /** @returns {Date} Reloj del emisor. */ now                                 : () => new Date(),
        /** @returns {Promise<boolean>} Dispositivo ficticio acreditado. */ eligible : async () =>
          true,
        sender                                                                       : {
          /** @param {Object} device Instalación. @param {Object} job Entrega cercada. @returns {Promise<void>} Efecto inequívoco contabilizado. */ async send(
            device,
            job,
          ) {
            sent.push(job.eventId);
          },
        },
      });
      /** @returns {Object} Arranca una instancia independiente sobre la misma cola durable. */
      function start() {
        const worker = createReactiveWorker({
          source    : createMongoChanges({
            connection,
            collections : ['PLATFORM_PUSH_DELIVERY'],
            logger,
            component   : 'push-test',
          }),
          /** @returns {Promise<number|undefined>} Cuenta toda consulta de actualización del trabajador. */ async nextAt() {
            queries++;
            return repository.nextAt();
          },
          tick      : delivery.tick,
          logger,
          component : 'push-test',
        });
        workers.push(worker);
        worker.start();
        return worker;
      }
      await repository.register({
        installationId : 'D1',
        bindingId      : 'B1',
        generation     : 1,
        fid            : 'fixture-installation',
        platform       : 'android',
        userCode       : 'U1',
        sessionCode    : 'S1',
        workspace      : 'W1',
        updatedAt      : new Date(),
      });
      /** @param {string} code Evento único. @returns {Promise<void>} Aceptación durable, sin llamar directamente al worker. */
      async function enqueue(code) {
        await repository.enqueue({
          messageCode        : code,
          conversationCode   : 'C1',
          sequence           : 1,
          workspace          : 'W1',
          recipientUserCodes : ['U1'],
          creationDate       : Date.now(),
        });
      }
      await enqueue('M1');
      const due = Date.now() + 2200;
      await models.deliveries.updateOne({ _id: 'M1:D1' }, { $set: { nextAt: new Date(due) } });
      start();
      start();
      await until(
        /** @returns {boolean} Ambas instancias abrieron el stream antes de consultar. */ () =>
          queries >= 2,
      );
      assert.equal(sent.length, 0);
      await until(
        /** @returns {boolean} El temporizador corresponde al registro durable. */ () =>
          sent.length === 1,
      );
      assert.ok(Date.now() >= due);
      await delay(1100);
      const before = queries;
      await delay(idleMs);
      assert.equal(queries, before, 'Una cola en reposo no produce consultas periódicas.');
      await enqueue('M2');
      await until(
        /** @returns {boolean} El stream activa la segunda entrega. */ () => sent.length === 2,
      );
      await Promise.all(
        workers.map(
          /** @param {Object} worker Instancia. @returns {Promise<void>} Cierra antes del reinicio. */ (
            worker,
          ) => worker.close(),
        ),
      );
      await enqueue('M3');
      // Una comprobación interrumpida puede recuperarse; una operación que llegó a sending no.
      await models.deliveries.updateOne(
        { _id: 'M3:D1' },
        {
          $set : {
            state      : 'checking',
            leaseUntil : new Date(Date.now() + 1200),
          },
        },
      );
      await enqueue('M4');
      await models.deliveries.updateOne({ _id: 'M4:D1' }, { $set: { state: 'sending' } });
      start();
      await until(
        /** @returns {boolean} El reinicio recupera el lease vencido una sola vez. */ () =>
          sent.length === 3,
      );
      assert.deepEqual(sent, ['M1', 'M2', 'M3']);
      assert.deepEqual(failures, []);
      await streams.at(-1).close();
      await until(
        /** @returns {boolean} Se detecta la discontinuidad real del cursor. */ () =>
          failures.length === 1,
      );
      await enqueue('M5');
      await until(
        /** @returns {boolean} El cursor reconstruido recupera el trabajo creado durante la desconexión. */ () =>
          sent.length === 4,
      );
      assert.deepEqual(sent, ['M1', 'M2', 'M3', 'M5']);
      assert.equal(failures[0].event, 'work_change_stream_unavailable');
    } finally {
      await Promise.all(
        workers.map(
          /** @param {Object} worker Instancia propia. @returns {Promise<void>} Libera cursores. */ (
            worker,
          ) => worker.close(),
        ),
      );
      await db.stop();
    }
  },
);

test(
  'notificaciones: un destinatario aplazado conserva deadline durable y no provoca reclamos vacíos',
  {
    skip    : !process.env.TEST_MONGOD_BINARY,
    timeout : 60000,
  },
  /** @returns {Promise<void>} Verifica recuperación transaccional con los modelos reales. */ async () => {
    const db = await startIsolatedMongo({
      binary : process.env.TEST_MONGOD_BINARY,
      mongoose,
    });
    const workers = [],
      errors      = [];
    try {
      const models = {};
      for (const [key, file] of Object.entries({
        notifications : 'notifications',
        broadcasts    : 'notifications-broadcast',
        recipients    : 'notifications-recipient',
      })) {
        const original = require(`../server/modules/notifications/models/${file}.model`);
        models[key] = db.connection.model(
          original.modelName,
          original.schema,
          original.collection.name,
        );
        await models[key].createCollection();
        await models[key].createIndexes();
      }
      await db.connection.db.createCollection('TEST_NOTIFICATION_OUTBOX');
      const messaging  = {
        /** @param {string} name Hecho. @param {Object} data Referencia mínima. @param {Object} options Transacción. @returns {Promise<Object>} Outbox del ensayo, indivisible del ledger. */
        publishEvent                                                                                 : (name, data, options) =>
          db.connection.db.collection('TEST_NOTIFICATION_OUTBOX').insertOne(
            {
              name,
              data,
            },
            { session: options.session },
          ),
        /** @returns {Object} Contexto no utilizado para escrituras internas cercadas. */ getContext :
          () => ({}),
        /** @returns {Promise<Object>} Membresía acreditada por el proveedor ficticio. */ async request() {
          return {
            statusCode : 200,
            data       : { items: [{ userCode: 'U1' }] },
          };
        },
      };
      const logger     = {
        /** @param {Object} error Código técnico. @returns {void} Conserva fallos inesperados. */ error(
          error,
        ) {
          errors.push(error);
        },
      };
      const repository = createNotificationsRepository({
        ...models,
        messaging,
      });
      await models.broadcasts.create({
        broadcastCode  : 'B1',
        workspace      : 'W1',
        userCreate     : 'ADMIN',
        title          : 'Prueba',
        content        : 'Contenido ficticio',
        audienceHash   : 'fixture',
        state          : 'queued',
        pendingCount   : 1,
        recipientCount : 1,
      });
      const due = Date.now() + 2500;
      await models.recipients.create({
        broadcastCode    : 'B1',
        workspace        : 'W1',
        userCreate       : 'ADMIN',
        userAssigned     : 'U1',
        userCode         : 'U1',
        notificationCode : 'N1',
        availableAt      : due,
      });
      let claims = 0;
      for (let i = 0; i < 2; i++) {
        const worker = createNotificationsWorker({
          repository : {
            ...repository,
            /** @returns {Promise<Object|null>} Cuenta reclamaciones de cada proceso. */ async claim() {
              claims++;
              return repository.claim();
            },
          },
          messaging,
          logger,
          realtime   : { /** @returns {void} El test no expone un socket. */ publish() {} },
          changes    : createMongoChanges({
            connection  : db.connection,
            collections : [models.broadcasts.collection.name, models.recipients.collection.name],
            logger,
            component   : 'notifications-test',
          }),
        });
        workers.push(worker);
        worker.start();
      }
      await until(
        /** @returns {Promise<boolean>} Una reclamación vacía agenda el destinatario futuro. */ async () =>
          (await models.broadcasts.findOne().lean()).availableAt === due,
      );
      const before = claims;
      await delay(300);
      assert.equal(claims, before);
      await until(
        /** @returns {Promise<boolean>} El vencimiento genera exactamente una notificación. */ async () =>
          (await models.notifications.countDocuments()) === 1,
      );
      assert.equal((await models.broadcasts.findOne().lean()).state, 'completed');
      assert.equal((await models.recipients.findOne().lean()).state, 'persisted');
      assert.deepEqual(errors, []);
    } finally {
      await Promise.all(
        workers.map(
          /** @param {Object} worker Instancia propia. @returns {Promise<void>} Cierra antes de MongoDB. */ (
            worker,
          ) => worker.close(),
        ),
      );
      await db.stop();
    }
  },
);
