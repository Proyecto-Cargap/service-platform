/** @file Acredita metadatos mínimos, planificación acotada y aislamiento del feed de mensajería. */
'use strict';
const test                   = require('node:test');
const assert                 = require('node:assert/strict');
const { setTimeout: delay }  = require('node:timers/promises');
const mongoose               = require('mongoose');
const { PersistenceFeed }    = require('../server/library/messaging/persistence-feed');
const { startIsolatedMongo } = require('./helpers/isolated-mongo.cjs');
const scope                  = {
  namespace : 'feed-performance',
  service   : 'service-platform',
};
/** @param {Function} predicate Condición. @returns {Promise<void>} Espera acotada únicamente en pruebas. */
async function until(predicate) {
  const deadline = Date.now() + 10000;
  while (!predicate()) {
    assert(Date.now() < deadline, 'No llegó el cambio esperado');
    await delay(10);
  }
}
test('buscar vencimientos no recorre el historial y un cambio de payload no despierta trabajo', /** @returns {void} Acredita planificación independiente del historial. */ () => {
  const changed = [];
  const feed    = new PersistenceFeed({
    connection : {},
    scope,
    /** @param {string} kind Colección afectada. @returns {number} Registra la notificación. */
    onChange   : (kind) => changed.push(kind),
    /** @param {Error} error Fallo inesperado. @returns {never} Hace fallar la prueba. */
    onError    : (error) => {
      throw error;
    },
  });
  for (let i = 0; i < 10000; i++)
    feed.put('inbox', {
      _id       : 'old-' + i,
      status    : 'completed',
      createdAt : new Date(1),
    });
  feed.put('outbox', {
    _id         : 'due',
    status      : 'pending',
    availableAt : new Date(10),
    createdAt   : new Date(1),
  });
  /** @returns {never} Detecta un recorrido indebido del historial. */
  feed.rows.values = () => {
    throw new Error('No recorrer historial');
  };
  assert.equal(feed.nextOutboxAt(), 10);
  assert.equal(feed.snapshot().oldestPendingAt.getTime(), 1);
  assert.equal(feed.snapshot().inbox.completed, 10000);
  feed.apply({
    ns                : { coll: 'messaging_v1_inbox' },
    operationType     : 'update',
    documentKey       : { _id: 'old-1' },
    updateDescription : { updatedFields: { resultWire: 'private' } },
  });
  assert.deepEqual(changed, []);
  feed.apply({
    ns                : { coll: 'messaging_v1_outbox' },
    operationType     : 'update',
    documentKey       : { _id: 'due' },
    updateDescription : { updatedFields: { status: 'published' } },
  });
  assert.deepEqual(changed, ['outbox']);
  assert.equal(feed.nextOutboxAt(), undefined);
  assert.equal(feed.pendingOutbox.size, 0);
});

test(
  'el stream real no entrega cuerpos ni hace updateLookup y conserva reemplazos y borrados',
  {
    skip    : !process.env.TEST_MONGOD_BINARY,
    timeout : 30000,
  },
  /** @returns {Promise<void>} Comprueba privacidad y transiciones con MongoDB real aislado. */ async () => {
    const database = await startIsolatedMongo({
      binary          : process.env.TEST_MONGOD_BINARY,
      mongoose,
      monitorCommands : true,
    });
    const changes = [],
      events      = [],
      commands    = [],
      errors      = [];
    const feed  = new PersistenceFeed({
      connection : database.connection,
      scope,
      /** @param {string} kind Colección afectada. @returns {number} Registra la notificación. */
      onChange   : (kind) => changes.push(kind),
      /** @param {Error} error Fallo de stream. @returns {number} Conserva la evidencia. */
      onError    : (error) => errors.push(error),
    });
    const apply = feed.apply.bind(feed);
    /** @param {Object} event Cambio recibido. @returns {void} Observa sólo lo que entregó MongoDB. */
    feed.apply = (event) => {
      events.push(event);
      apply(event);
    };
    database.connection.getClient().on(
      'commandStarted',
      /** @param {Object} event Comando del driver. @returns {void} Acredita las opciones del stream real. */ (
        event,
      ) => {
        if (event.commandName === 'aggregate' && event.command.pipeline?.[0]?.$changeStream)
          commands.push(event.command.pipeline[0].$changeStream);
      },
    );
    try {
      for (const name of ['outbox', 'inbox', 'failures'])
        await database.connection.db.createCollection('messaging_v1_' + name);
      void feed.start();
      await until(
        /** @returns {boolean} Espera la transición confirmada descrita por la condición. */ () =>
          feed.ready || errors.length,
      );
      assert.deepEqual(errors, []);
      const inbox  = database.connection.db.collection('messaging_v1_inbox');
      const outbox = database.connection.db.collection('messaging_v1_outbox');
      await inbox.insertOne({
        _id     : 'foreign',
        ...scope,
        service : 'another',
        status  : 'processing',
        wire    : 'PRIVATE_CANARY',
      });
      await inbox.insertOne({
        _id    : 'owned',
        ...scope,
        status : 'processing',
        wire   : 'PRIVATE_CANARY',
      });
      await until(
        /** @returns {boolean} Espera la transición confirmada descrita por la condición. */ () =>
          feed.snapshot().inbox.processing === 1,
      );
      assert(
        !events.some(
          /** @param {Object} event Cambio recibido. @returns {boolean} Detecta una inserción ajena. */ (
            event,
          ) => event.operationType === 'insert' && event.documentKey._id === 'foreign',
        ),
      );
      changes.length = 0;
      await inbox.updateOne(
        { _id: 'foreign' },
        {
          $set : {
            status     : 'completed',
            resultWire : 'PRIVATE_CANARY',
          },
        },
      );
      await inbox.updateOne(
        { _id: 'owned' },
        {
          $set : {
            status     : 'completed',
            resultWire : 'PRIVATE_CANARY',
          },
        },
      );
      await until(
        /** @returns {boolean} Espera la transición confirmada descrita por la condición. */ () =>
          feed.snapshot().inbox.completed === 1,
      );
      assert.deepEqual(changes, ['inbox']);
      assert(!JSON.stringify(events).includes('PRIVATE_CANARY'));
      assert(commands.length > 0);
      assert(
        commands.every(
          /** @param {Object} command Opciones enviadas a MongoDB. @returns {boolean} No solicita lookup del documento. */ (
            command,
          ) => command.fullDocument !== 'updateLookup',
        ),
      );
      assert(
        events
          .filter(
            /** @param {Object} event Cambio recibido. @returns {boolean} Es una actualización. */ (
              event,
            ) => event.operationType === 'update',
          )
          .every(
            /** @param {Object} event Actualización. @returns {boolean} Recibió únicamente el delta. */ (
              event,
            ) => !event.fullDocument,
          ),
      );
      await outbox.insertOne({
        _id         : 'work',
        ...scope,
        status      : 'pending',
        availableAt : new Date(10),
        wire        : 'PRIVATE_CANARY',
      });
      await until(
        /** @returns {boolean} Espera la transición confirmada descrita por la condición. */ () =>
          feed.nextOutboxAt() === 10,
      );
      await outbox.replaceOne(
        { _id: 'work' },
        {
          _id        : 'work',
          ...scope,
          status     : 'publishing',
          leaseUntil : new Date(20),
          wire       : 'PRIVATE_CANARY',
        },
      );
      await until(
        /** @returns {boolean} Espera la transición confirmada descrita por la condición. */ () =>
          feed.nextOutboxAt() === 20,
      );
      await outbox.deleteOne({ _id: 'work' });
      await inbox.deleteOne({ _id: 'owned' });
      await until(
        /** @returns {boolean} Espera la transición confirmada descrita por la condición. */ () =>
          feed.snapshot().inbox.completed === 0 && feed.pendingOutbox.size === 0,
      );
      assert.equal(feed.snapshot().outbox.publishing, 0);
      assert(!JSON.stringify(events).includes('PRIVATE_CANARY'));
      assert.deepEqual(errors, []);
    } finally {
      await feed.close();
      await database.stop();
    }
  },
);
