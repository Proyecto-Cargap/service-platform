/** @file Verifica señales sin sondeo y proyección real de cambios transaccionales de Messaging. */
'use strict';
const test                   = require('node:test');
const assert                 = require('node:assert/strict');
const { setTimeout: delay }  = require('node:timers/promises');
const mongoose               = require('mongoose');
const { WakeSignal }         = require('../server/library/messaging/wake-signal');
const { PersistenceFeed }    = require('../server/library/messaging/persistence-feed');
const { startIsolatedMongo } = require('./helpers/isolated-mongo.cjs');

/** @param {Function} predicate Condición observable. @returns {Promise<void>} Espera acotada exclusiva de la prueba. */
async function until(predicate) {
  const deadline = Date.now() + 10000;
  while (!predicate()) {
    assert.ok(Date.now() < deadline, 'La condición debe cumplirse dentro del plazo');
    await delay(10);
  }
}

test('la señal conserva cambios previos, vence y libera listeners al cerrar' /** @returns {Promise<void>} Comprueba las tres causas de despertar. */, async () => {
  const wake       = new WakeSignal();
  const controller = new AbortController();
  const version    = wake.version;
  wake.notify();
  await wake.wait(version, { signal: controller.signal });
  assert.equal(wake.waiters.size, 0);
  await wake.wait(wake.version, { deadlineAt: Date.now() + 20 });
  const pending = wake.wait(wake.version, { signal: controller.signal });
  assert.equal(wake.waiters.size, 1);
  controller.abort();
  await pending;
  assert.equal(wake.waiters.size, 0);
});

test(
  'Change Streams conserva el corte inicial, rollback, estados, vencimientos y ámbito',
  {
    skip    : !process.env.TEST_MONGOD_BINARY,
    timeout : 30000,
  },
  /** @returns {Promise<void>} Usa exclusivamente un replica set local temporal. */
  async () => {
    const database = await startIsolatedMongo({
      binary : process.env.TEST_MONGOD_BINARY,
      mongoose,
    });
    const scope    = {
      namespace : 'reactive-test',
      service   : 'service-platform',
    };
    const errors   = [];
    const feed     = new PersistenceFeed({
      connection : database.connection,
      scope,
      /** @returns {void} La prueba observa la proyección directamente. */
      onChange   : () => {},
      /** @param {Error} error Fallo del stream. @returns {number} Conserva evidencia. */
      onError    : (error) => errors.push(error),
    });
    try {
      const outbox = database.connection.db.collection('messaging_v1_outbox');
      for (const name of ['messaging_v1_inbox', 'messaging_v1_failures'])
        await database.connection.db.createCollection(name);
      const due = new Date(Date.now() + 5000);
      await outbox.insertMany([
        {
          _id         : 'pending',
          ...scope,
          status      : 'pending',
          availableAt : due,
          createdAt   : new Date(),
        },
        {
          _id       : 'published',
          ...scope,
          status    : 'published',
          createdAt : new Date(),
        },
        {
          _id         : 'foreign',
          ...scope,
          service     : 'service-other',
          status      : 'pending',
          availableAt : new Date(),
        },
      ]);
      void feed.start();
      await until(
        /** @returns {boolean} Espera el corte inicial. */
        () => feed.ready || errors.length > 0,
      );
      assert.deepEqual(errors, []);
      assert.equal(feed.snapshot().outbox.pending, 1);
      assert.equal(feed.snapshot().outbox.published, 1);
      assert.equal(feed.nextOutboxAt(), due.getTime());
      const session = await database.connection.startSession();
      try {
        session.startTransaction();
        await outbox.insertOne(
          {
            _id    : 'rollback',
            ...scope,
            status : 'pending',
          },
          { session },
        );
        await session.abortTransaction();
        await session.withTransaction(
          /** @returns {Promise<Object>} Confirma el cambio y su evento juntos. */
          () =>
            outbox.updateOne({ _id: 'pending' }, { $set: { status: 'published' } }, { session }),
        );
      } finally {
        await session.endSession();
      }
      await until(
        /** @returns {boolean} Espera la transición confirmada. */
        () => feed.snapshot().outbox.published === 2,
      );
      assert.equal(feed.snapshot().outbox.pending, 0);
      assert.equal(feed.nextOutboxAt(), undefined);
      assert.equal(feed.rows.has('outbox:rollback'), false);
      await outbox.deleteOne({ _id: 'published' });
      await until(
        /** @returns {boolean} Verifica decremento por eliminación, incluida la caducidad TTL. */
        () => feed.snapshot().outbox.published === 1,
      );
      const version = feed.signal.version;
      await delay(250);
      assert.equal(feed.signal.version, version, 'El reposo no produce nuevas muestras');
      assert.deepEqual(errors, []);
    } finally {
      await feed.close();
      await database.stop();
    }
  },
);
