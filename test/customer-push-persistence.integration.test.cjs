/** @file Integración transaccional aislada: rollback, reentregas concurrentes y recuperación de recordatorios. */
'use strict';
const test                     = require('node:test');
const assert                   = require('node:assert/strict');
const mongoose                 = require('mongoose');
const { randomUUID }           = require('node:crypto');
const { startIsolatedMongo }   = require('./helpers/isolated-mongo.cjs');
const { createPushModels }     = require('../server/modules/push/models/push.model');
const { createPushRepository } = require('../server/modules/push/repositories/push.repository');
const {
  createReminderPushRepository,
} = require('../server/modules/push/repositories/reminder-push.repository');
const notificationsModel = require('../server/modules/notifications/models/notifications.model');

test(
  'aviso y entregas se confirman juntos y no se duplican después de reiniciar',
  {
    skip    : !process.env.TEST_MONGOD_BINARY,
    timeout : 60000,
  },
  /** @returns {Promise<void>} Usa MongoDB efímero y ninguna credencial real. */ async () => {
    const database = await startIsolatedMongo({
      binary : process.env.TEST_MONGOD_BINARY,
      mongoose,
    });
    try {
      const models        = createPushModels({
        Schema : mongoose.Schema,
        models : database.connection.models,
        model  : database.connection.model.bind(database.connection),
      });
      const notifications = database.connection.model(
        'ReminderNotice',
        notificationsModel.schema.clone(),
        'CONNECT_NOTIFICATIONS',
      );
      for (const model of [...Object.values(models), notifications]) {
        await model.createCollection();
        await model.createIndexes();
      }
      const date                                   = new Date();
      /** @returns {Date} Reloj fijo. */ const now = () => date;
      const queue                                  = createPushRepository({
        ...models,
        now,
        newId : randomUUID,
      });
      for (const platform of ['ios', 'android'])
        await queue.register({
          installationId : randomUUID(),
          bindingId      : randomUUID(),
          generation     : 1,
          platform,
          fid            : `fictional-${platform}`,
          userCode       : 'U1',
          workspace      : 'W1',
          sessionCode    : 'S1',
          updatedAt      : date,
        });
      const input    = {
        notificationCode : 'N1',
        sourceEventKey   : 'customer.activity:A1:2',
        workspace        : 'W1',
        userAssigned     : 'U1',
        userCode         : 'U1',
        userCreate       : '',
        title            : 'Actividad pendiente',
        content          : 'Datos ficticios',
        type             : 'info',
        action           : 'open',
        idObject         : 'A1',
        from             : '',
        erased           : false,
        creationDate     : date.getTime(),
        updateDate       : date.getTime(),
        link             : {
          route      : '/app/customer/activity/detail/A1',
          parameters : [],
        },
      };
      const reminder = {
        activityCode     : 'A1',
        reminderRevision : 2,
        userCode         : 'U1',
        workspace        : 'W1',
      };
      await database.connection.db.createCollection('TEST_NOTIFICATION_OUTBOX');
      const messaging = {
        /** @param {string} name Hecho. @param {Object} data Referencia. @param {Object} options Sesión de la notificación y push. @returns {Promise<Object>} Escritura atómica del ensayo. */
        publishEvent : (name, data, options) =>
          database.connection.db.collection('TEST_NOTIFICATION_OUTBOX').insertOne(
            {
              name,
              data,
            },
            { session: options.session },
          ),
      };
      const failed    = createReminderPushRepository({
        messaging,
        ...models,
        notifications,
        now,
        ready      : queue.ready,
        deliveries : {
          /** @returns {Promise<never>} Simula falta de espacio antes de confirmar la transacción. */ async updateOne() {
            throw new Error('INJECTED_STORAGE_FAILURE');
          },
        },
      });
      await assert.rejects(failed.persist(input, reminder), /INJECTED_STORAGE_FAILURE/);
      assert.equal(await notifications.countDocuments(), 0);
      assert.equal(await models.deliveries.countDocuments(), 0);
      const repository = createReminderPushRepository({
        messaging,
        ...models,
        notifications,
        now,
        ready : queue.ready,
      });
      await Promise.all(
        Array.from(
          { length: 8 },
          /** @returns {Promise<Object>} Reentrega concurrente del mismo evento durable. */ () =>
            repository.persist(input, reminder),
        ),
      );
      assert.equal(await notifications.countDocuments(), 1);
      assert.equal(await models.deliveries.countDocuments(), 2);
      const jobs = await models.deliveries.find().lean();
      assert.ok(
        jobs.every(
          /** @param {Object} job Entrega persistida. @returns {boolean} Referencia canónica y generación conservadas. */ (
            job,
          ) =>
            job.kind === 'customer-activity' &&
            job.notificationCode === 'N1' &&
            job.reminderRevision === 2,
        ),
      );
      const restarted = createReminderPushRepository({
        messaging,
        ...models,
        notifications,
        now,
        ready : queue.ready,
      });
      await restarted.persist(
        {
          ...input,
          notificationCode : 'N2',
        },
        reminder,
      );
      assert.equal(await notifications.countDocuments(), 1);
      assert.equal(await models.deliveries.countDocuments(), 2);
      await restarted.persist(
        {
          ...input,
          notificationCode : 'N3',
          sourceEventKey   : 'customer.activity:A1:3',
        },
        {
          ...reminder,
          reminderRevision : 3,
        },
      );
      assert.equal(await notifications.countDocuments(), 2);
      assert.equal(await models.deliveries.countDocuments(), 4);
    } finally {
      await database.stop();
    }
  },
);
