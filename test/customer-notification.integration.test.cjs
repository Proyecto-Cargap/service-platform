/** @file Dedupe real de avisos Customer sobre MongoDB aislado con el índice único de Platform. */
'use strict';
const test                   = require('node:test');
const assert                 = require('node:assert/strict');
const mongoose               = require('mongoose');
const { startIsolatedMongo } = require('./helpers/isolated-mongo.cjs');
const createRepository       = require('../server/modules/notifications/repositories/notifications.repository');
test(
  'veinte reentregas concurrentes persisten un aviso por generación y destinatario',
  {
    skip    : !process.env.TEST_MONGOD_BINARY,
    timeout : 60000,
  },
  /** @returns {Promise<void>} Usa el modelo real, verifica unicidad y libera su base exclusiva. */
  async () => {
    const database = await startIsolatedMongo({
      binary : process.env.TEST_MONGOD_BINARY,
      mongoose,
    });
    try {
      const original = require('../server/modules/notifications/models/notifications.model');
      const model    = database.connection.model(
        original.modelName,
        original.schema,
        original.collection.name,
      );
      await model.createCollection();
      await model.createIndexes();
      await database.connection.db.createCollection('TEST_OUTBOX');
      const repository = createRepository({
        model,
        messaging : {
          /** @param {string} name Hecho. @param {Object} data Cambio. @param {Object} options Transacción del aviso. @returns {Promise<Object>} Outbox real sin broker para probar atomicidad y dedupe. */
          publishEvent : (name, data, options) =>
            database.connection.db.collection('TEST_OUTBOX').insertOne(
              {
                name,
                data,
              },
              { session: options.session },
            ),
        },
      });
      const input      = {
        notificationCode : 'notice',
        sourceEventKey   : 'customer.activity:activity-a:2',
        workspace        : 'workspace-a',
        userCreate       : '',
        userAssigned     : 'alice',
        userCode         : 'alice',
        title            : 'Pendiente',
        content          : 'Llamar',
        type             : 'info',
        erased           : false,
      };
      const deliveries = await Promise.all(
        Array.from(
          { length: 20 },
          /** @param {unknown} value Elemento inicial. @param {number} index Reentrega. @returns {Promise<Object>} Referencia canónica idempotente. */
          (value, index) =>
            repository.createEvent({
              ...input,
              notificationCode : 'notice-' + index,
            }),
        ),
      );
      assert.equal(await model.countDocuments(), 1);
      assert.equal(
        new Set(
          deliveries.map(
            /** @param {Object} item Referencia devuelta. @returns {string} Identidad persistida. */
            (item) => item.notificationCode,
          ),
        ).size,
        1,
      );
      await repository.createEvent({
        ...input,
        notificationCode : 'next',
        sourceEventKey   : 'customer.activity:activity-a:3',
      });
      await repository.createEvent({
        ...input,
        notificationCode : 'other-recipient',
        userAssigned     : 'bob',
        userCode         : 'bob',
      });
      assert.equal(await model.countDocuments(), 3);
      assert.equal(await database.connection.db.collection('TEST_OUTBOX').countDocuments(), 3);
    } finally {
      await database.stop();
    }
  },
);
