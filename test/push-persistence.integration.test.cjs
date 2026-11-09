/** @file Integración Mongo aislada: generaciones, unicidad, caducidad y cercado de entregas push. */
'use strict';
const test                     = require('node:test');
const assert                   = require('node:assert/strict');
const mongoose                 = require('mongoose');
const { randomUUID }           = require('node:crypto');
const { startIsolatedMongo }   = require('./helpers/isolated-mongo.cjs');
const { createPushModels }     = require('../server/modules/push/models/push.model');
const { createPushRepository } = require('../server/modules/push/repositories/push.repository');
test(
  'registro y cola resisten concurrencia y cambio de contexto sin repetir un envío incierto',
  {
    skip    : !process.env.TEST_MONGOD_BINARY,
    timeout : 60000,
  },
  /** @returns {Promise<void>} Usa exclusivamente una base efímera y datos ficticios. */ async () => {
    const database = await startIsolatedMongo({
      binary : process.env.TEST_MONGOD_BINARY,
      mongoose,
    });
    try {
      const models = createPushModels({
        Schema : mongoose.Schema,
        models : database.connection.models,
        model  : database.connection.model.bind(database.connection),
      });
      for (const model of Object.values(models)) {
        await model.createCollection();
        await model.createIndexes();
      }
      let now          = new Date();
      const repository = createPushRepository({
        ...models,
        newId                                        : randomUUID,
        /** @returns {Date} Reloj controlado. */ now : () => now,
      });
      const device     = {
        installationId : randomUUID(),
        bindingId      : randomUUID(),
        generation     : 1,
        fid            : 'fictional-push-fid-0001',
        platform       : 'android',
        userCode       : 'U1',
        workspace      : 'W1',
        sessionCode    : 'S1',
        updatedAt      : now,
      };
      await repository.register(device);
      await repository.register(device);
      await repository.register({
        ...device,
        generation : 3,
      });
      await assert.rejects(
        repository.register({
          ...device,
          generation : 2,
        }),
        { statusCode: 409 },
      );
      await assert.rejects(
        repository.register({
          ...device,
          installationId : randomUUID(),
        }),
        { statusCode: 409 },
      );
      assert.equal(await models.devices.countDocuments(), 1);
      const event = {
        messageCode        : 'M1',
        conversationCode   : 'C1',
        sequence           : 1,
        workspace          : 'W1',
        recipientUserCodes : ['U1'],
        creationDate       : now.getTime(),
      };
      await Promise.all(
        Array.from(
          { length: 20 },
          /** @returns {Promise<void>} Reentrega concurrente del mismo evento. */ () =>
            repository.enqueue(event),
        ),
      );
      assert.equal(await models.deliveries.countDocuments(), 1);
      await repository.enqueue({
        ...event,
        messageCode  : 'EXPIRED',
        creationDate : now.getTime() - 3600001,
      });
      await repository.enqueue({
        ...event,
        messageCode : 'OTHER-WORKSPACE',
        workspace   : 'W2',
      });
      assert.equal(await models.deliveries.countDocuments(), 1);
      const first = await repository.claim();
      assert.ok(first);
      assert.equal(await repository.claim(), null);
      now = new Date(now.getTime() + 120001);
      const reclaimed = await repository.claim();
      assert.ok(reclaimed);
      assert.notEqual(reclaimed.lease, first.lease);
      assert.equal(await repository.beginSending(first), false);
      assert.equal(await repository.beginSending(reclaimed), true);
      now = new Date(now.getTime() + 120001);
      assert.equal(
        await repository.claim(),
        null,
        'sending no vuelve a reclamarse tras perder una confirmación',
      );
      await repository.finish(first, 'sent', 'STALE');
      assert.equal((await models.deliveries.findById(first._id).lean()).state, 'sending');
      await repository.finish(reclaimed, 'uncertain', 'DELIVERY_UNCERTAIN');
      await repository.register({
        ...device,
        generation : 4,
        bindingId  : randomUUID(),
        workspace  : 'W2',
        updatedAt  : now,
      });
      assert.equal(await repository.device(reclaimed), null);
      await repository.invalidate(reclaimed);
      assert.equal(
        await models.devices.countDocuments(),
        1,
        'invalidación tardía conserva la nueva vinculación',
      );
    } finally {
      await database.stop();
    }
  },
);
