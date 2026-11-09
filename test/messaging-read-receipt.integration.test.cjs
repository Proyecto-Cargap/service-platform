/** @file RPC real con cursores emitidos por el productor sobre MongoDB y RabbitMQ efímeros. */
'use strict';
const { RealtimeWindows } = require('../server/library/realtime/windows');
const { contentCursor }   = require('../server/library/realtime/window-delta');
const test = require('node:test'),
  assert   = require('node:assert/strict'),
  mongoose = require('mongoose');
const { randomUUID }         = require('node:crypto');
const { MessagingRuntime }   = require('../server/library/messaging/runtime');
const { readSnapshot }       = require('../server/library/messaging/read-receipt');
const { startIsolatedMongo } = require('./helpers/isolated-mongo.cjs');
const { startIsolatedAmqp }  = require('./helpers/isolated-amqp.cjs');
test(
  'dos productores conservan el mismo cursor para el mismo estado y cambian con el resultado',
  {
    skip    :
      !process.env.TEST_MONGOD_BINARY ||
      !process.env.TEST_RABBITMQ_SERVER_BINARY ||
      !process.env.TEST_EPMD_BINARY,
    timeout : 60000,
  },
  /** @returns {Promise<void>} El cursor atraviesa la serialización y los dos canales RPC reales. */ async () => {
    let database, broker;
    const runtimes = [];
    try {
      database = await startIsolatedMongo({
        binary : process.env.TEST_MONGOD_BINARY,
        mongoose,
      });
      broker = await startIsolatedAmqp({
        rabbitmqServer : process.env.TEST_RABBITMQ_SERVER_BINARY,
        epmd           : process.env.TEST_EPMD_BINARY,
      });
      const connection = database.connection;
      for (const [name, indexes] of Object.entries(
        require('../server/library/messaging/indexes.json'),
      ))
        for (const { key, ...options } of indexes)
          await connection.db.collection(name).createIndex(key, options);
      const namespace = 'receipt-' + randomUUID();
      let count       = 1;
      for (const service of ['service-owner', 'service-owner', 'service-platform']) {
        const runtime = new MessagingRuntime({
          service,
          namespace,
          url    : broker.url,
          config : {
            startTimeoutMs    : 10000,
            rpcTimeoutMs      : 5000,
            shutdownTimeoutMs : 5000,
          },
        });
        if (service === 'service-owner')
          runtime.registerAction({
            name      : 'records-get-list',
            execution : 'nonTransactional',
            retrySafe : true,
            /** @returns {Object} DTO del propietario; no delega la creación del cursor al solicitante. */ handler() {
              return {
                statusCode      : 200,
                descriptionCode : 'OK',
                data            : {
                  items : [
                    {
                      code : 'one',
                      count,
                    },
                  ],
                  total : 1,
                },
              };
            },
          });
        runtimes.push(runtime);
        await runtime.start({ connection });
      }
      const caller = runtimes[2],
        first      = readSnapshot(await caller.request('service-owner', 'records-get-list', {}));
      const second = readSnapshot(await caller.request('service-owner', 'records-get-list', {}));
      assert.equal(first.cursor, second.cursor);
      assert.deepEqual(first.data, second.data);
      count = 2;
      const next = readSnapshot(await caller.request('service-owner', 'records-get-list', {}));
      assert.notEqual(first.cursor, next.cursor);
      assert.equal(next.data.items[0].count, 2);
      const receipts = new Map(),
        deliveries   = [];
      const identity = {
        userCode    : 'user',
        sessionCode : 'session',
        workspace   : 'workspace',
        context     : {},
      };
      const socket   = {
        id : 'socket',
        /** @returns {void} Controles directos en el arnés. */ on() {},
        /** @param {string} channel Canal. @param {Object} value Patch. @returns {void} Sólo captura entregas. */ emit(
          channel,
          value,
        ) {
          assert.equal(channel, 'realtime');
          deliveries.push(value);
        },
      };
      const windows  = new RealtimeWindows({
        topics   : {
          records : {
            page : {
              /** @param {Object} query Consulta. @returns {Object} Selector sintético. */ validate(
                query,
              ) {
                return query;
              },
              /** @returns {Promise<Object>} Lectura RPC real del productor. */ async read() {
                return readSnapshot(await caller.request('service-owner', 'records-get-list', {}));
              },
              /** @returns {boolean} Este hecho sintético afecta la única ventana. */ affects() {
                return true;
              },
            },
          },
        },
        receipts : {
          /** @param {Object} value Reserva. @returns {Promise<void>} Persiste intención. */ async put(
            value,
          ) {
            receipts.set(value.receiptId, value);
          },
          /** @param {string} id Recibo. @returns {Promise<Object>} Base del arnés aislado. */ async get(
            id,
          ) {
            return receipts.get(id);
          },
          /** @param {Object} value Base HTTP. @returns {Promise<boolean>} Instalación atómica. */ async complete(
            value,
          ) {
            receipts.set(value.receiptId, {
              ...value,
              state : 'ready',
            });
            return true;
          },
        },
      });
      try {
        windows.authenticate(
          socket,
          /** @returns {Promise<Object>} Identidad acreditada. */ async () => identity,
        );
        const session = windows.sessions.get('socket');
        const ack     = await windows.control(session, {
          type           : 'subscribe',
          topic          : 'records',
          section        : 'page',
          subscriptionId : 'page',
          watchVersion   : 1,
          query          : {},
        });
        const base    = await windows.read(
          {
            ...ack,
            topic   : 'records',
            section : 'page',
            query   : {},
          },
          identity,
        );
        assert.equal(base.control.cursor, next.cursor);
        assert.notEqual(base.control.cursor, contentCursor(base.data));
        await windows.control(session, {
          ...base.control,
          type : 'bind',
        });
        assert.equal(deliveries.length, 0);
        count = 3;
        await windows.changed('records', {});
        assert.equal(deliveries.length, 1);
        assert.equal(deliveries[0].type, 'patch');
        assert.equal(deliveries[0].data.baseCursor, next.cursor);
        assert.equal(
          deliveries[0].data.cursor,
          readSnapshot(await caller.request('service-owner', 'records-get-list', {})).cursor,
        );
      } finally {
        windows.close();
      }
    } finally {
      for (const runtime of runtimes.reverse()) await runtime.close();
      await broker?.stop();
      await database?.stop();
    }
  },
);
