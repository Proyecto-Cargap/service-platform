/** @file Aceptación real de reposo, reinicio del broker y exclusión entre publicadores sin polling. */
'use strict';
const test                   = require('node:test');
const assert                 = require('node:assert/strict');
const { randomUUID }         = require('node:crypto');
const { setTimeout: delay }  = require('node:timers/promises');
const mongoose               = require('mongoose');
const { MessagingRuntime }   = require('../server/library/messaging/runtime');
const { startIsolatedMongo } = require('./helpers/isolated-mongo.cjs');
const { startIsolatedAmqp }  = require('./helpers/isolated-amqp.cjs');
/** @param {Function} predicate Estado esperado. @returns {Promise<void>} Espera acotada sólo del arnés de pruebas. */
async function until(predicate) {
  const deadline = Date.now() + 20000;
  while (!(await predicate())) {
    assert.ok(Date.now() < deadline, 'La operación no alcanzó su estado esperado');
    await delay(20);
  }
}
test(
  '600 segundos en reposo no consultan pendientes; broker reiniciado y dos instancias conservan idempotencia',
  {
    skip    :
      !process.env.TEST_REALTIME_IDLE ||
      !process.env.TEST_MONGOD_BINARY ||
      !process.env.TEST_RABBITMQ_SERVER_BINARY ||
      !process.env.TEST_EPMD_BINARY,
    timeout : 720000,
  },
  /** @param {Object} context Contexto de diagnóstico del arnés. @returns {Promise<void>} Usa exclusivamente infraestructura temporal de esta prueba. */
  async (context) => {
    let database, broker;
    const runtimes = [];
    try {
      database = await startIsolatedMongo({
        binary          : process.env.TEST_MONGOD_BINARY,
        mongoose,
        monitorCommands : true,
      });
      broker = await startIsolatedAmqp({
        rabbitmqServer : process.env.TEST_RABBITMQ_SERVER_BINARY,
        epmd           : process.env.TEST_EPMD_BINARY,
      });
      const connection = database.connection,
        client         = connection.getClient();
      for (const [name, indexes] of Object.entries(
        require('../server/library/messaging/indexes.json'),
      ))
        for (const { key, ...options } of indexes)
          await connection.db.collection(name).createIndex(key, options);
      const namespace = 'idle-' + randomUUID();
      const effects   = connection.db.collection('idle_test_effects');
      /** @param {string} service Identidad. @returns {Object} Runtime aislado con configuración idéntica a producción. */
      function create(service) {
        const runtime = new MessagingRuntime({
          service,
          namespace,
          url    : broker.url,
          config : {
            startTimeoutMs    : 20000,
            shutdownTimeoutMs : 5000,
          },
        });
        runtimes.push(runtime);
        return runtime;
      }
      const first = create('service-platform'),
        second    = create('service-platform'),
        sink      = create('service-sink');
      sink.subscribe({
        name      : 'idle.changed',
        execution : 'transactional',
        /** @param {Object} data Efecto esperado. @param {Object} unit Transacción del inbox. @returns {Promise<void>} Efecto durable que detecta ejecución duplicada. */
        async handler(data, unit) {
          await effects.updateOne(
            { _id: data.code },
            { $inc: { count: 1 } },
            {
              upsert  : true,
              session : unit.session,
            },
          );
        },
      });
      for (const runtime of runtimes) await runtime.start({ connection });
      await first.publishEvent('idle.changed', { code: 'warm' }, { publicationId: 'warm' });
      await until(
        /** @returns {Promise<boolean>} Confirmación durable del consumidor. */
        async () => (await effects.findOne({ _id: 'warm' }))?.count === 1,
      );
      await delay(500);
      const commands = [];
      /** @param {Object} event Comando del driver. @returns {void} Distingue Change Stream de consultas de negocio. */
      function observe(event) {
        if (
          ['find', 'findAndModify', 'count', 'distinct'].includes(event.commandName) ||
          (event.commandName === 'aggregate' && !event.command.pipeline?.[0]?.$changeStream)
        )
          commands.push(event.commandName);
      }
      client.on('commandStarted', observe);
      await effects.findOne({ _id: 'monitoring-control' });
      assert.deepEqual(commands, ['find'], 'El control positivo debe registrar una consulta real');
      commands.length = 0;
      const idleStarted = Date.now();
      context.diagnostic('Control positivo: una consulta find observada antes del reposo');
      await delay(600000);
      for (const runtime of runtimes) {
        runtime.getStats();
        assert.equal(runtime.persistenceFeed.ready, true);
      }
      client.off('commandStarted', observe);
      assert.deepEqual(
        commands,
        [],
        'Reposo sin lecturas periódicas ni reclamaciones de pendientes',
      );
      context.diagnostic(
        JSON.stringify({
          idleMs          : Date.now() - idleStarted,
          periodicQueries : commands.length,
        }),
      );
      await broker.restart();
      await until(
        /** @returns {boolean} Recuperación de todas las conexiones. */
        () =>
          runtimes.every(
            /** @param {Object} runtime Instancia. @returns {boolean} Broker y productor de cambios disponibles. */
            (runtime) => runtime.state === 'ready' && runtime.persistenceFeed.ready,
          ),
      );
      await Promise.all([
        first.publishEvent('idle.changed', { code: 'once' }, { publicationId: 'once' }),
        second.publishEvent('idle.changed', { code: 'once' }, { publicationId: 'once' }),
      ]);
      await until(
        /** @returns {Promise<boolean>} Una aplicación del efecto. */
        async () => (await effects.findOne({ _id: 'once' }))?.count === 1,
      );
      await delay(500);
      assert.equal((await effects.findOne({ _id: 'once' })).count, 1);
      context.diagnostic(
        'Broker reiniciado; dos publicadores produjeron exactamente un efecto durable',
      );
    } finally {
      for (const runtime of runtimes.reverse())
        await runtime.close().catch(
          /** @returns {void} Continúa cerrando infraestructura propia aunque un runtime ya haya terminado. */
          () => {},
        );
      await broker?.stop();
      await database?.stop();
    }
  },
);
