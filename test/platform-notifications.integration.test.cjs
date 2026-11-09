/** @file Ventanas de avisos con productor Mongo real, permisos, contadores, carreras y outbox atómico. */
'use strict';
const test                   = require('node:test');
const assert                 = require('node:assert/strict');
const { AsyncLocalStorage }  = require('node:async_hooks');
const mongoose               = require('mongoose');
const { startIsolatedMongo } = require('./helpers/isolated-mongo.cjs');
const { platformWindows }    = require('../server/modules/realtime/services/platform-window.service');
const { RealtimeWindows }    = require('../server/library/realtime/windows');
const { ReadWindow }         = require('../server/library/realtime/read-window');
const { contentCursor }      = require('../server/library/realtime/window-delta');
const { validate }           = require('../server/contracts/action-dto');
const contract               = require('../server/contracts/service.json');
test(
  'avisos, contador y difusión comparten casos canónicos; los cambios conservan ventanas sin lecturas del cliente',
  {
    skip    : !process.env.TEST_MONGOD_BINARY,
    timeout : 60000,
  },
  /** @param {Object} t Prueba aislada. @returns {Promise<void>} Rollback, idempotencia, filtros, orden, totales y revocación reales. */ async (
    t,
  ) => {
    const db = await startIsolatedMongo({
      binary : process.env.TEST_MONGOD_BINARY,
      mongoose,
    });
    t.after(db.stop);
    const local   = new AsyncLocalStorage();
    const context = {
      user        : { userCode: 'alice' },
      sessionCode : 'session',
      workspace   : 'w',
      policies    : ['view', 'create', 'delete'].map(
        /** @param {string} action Permiso. @returns {string} Concesión exacta. */ (action) =>
          `platform.notifications.${action}.global`,
      ),
    };
    let permitted = true,
      failPublish = false;
    const messaging = {
      /** @returns {Object} Principal del contexto asíncrono. */ getContext                                                                                      : () =>
        local.getStore() || context,
      /** @param {Object} identity Principal. @param {Function} run Caso canónico. @returns {Promise<*>} Identidad sin autoridad del selector. */ runWithContext :
        (identity, run) => local.run(identity, run),
      /** @param {number} statusCode Resultado. @param {*} data DTO público. @returns {Object} Sobre estándar. */ successResponse                                :
        (statusCode, data) => ({
          statusCode,
          descriptionCode : 'OK',
          data,
        }),
      /** @param {string} target Propietario. @param {string} action Consulta. @param {Object} query Selector. @returns {Promise<Object>} Security vigente; no acepta RPC a Platform. */ async request(
        target,
        action,
        query,
      ) {
        assert.equal(target, 'service-security');
        if (!permitted) return { statusCode: 403 };
        const current = messaging.getContext();
        if (action === 'identity-get-current')
          return {
            statusCode : 200,
            data       : {
              userCode   : current.user.userCode,
              workspace  : current.workspace,
              workspaces : ['w', 'other'],
              policies   : current.policies,
            },
          };
        if (action === 'user-profile-get-list')
          return {
            statusCode : 200,
            data       : {
              items : query.codes.map(
                /** @param {string} userCode Referencia. @returns {Object} Perfil público. */ (
                  userCode,
                ) => ({
                  userCode,
                  name : userCode,
                }),
              ),
            },
          };
        if (action === 'workspace-users-get-list')
          return {
            statusCode : 200,
            data       : {
              items      : [{ userCode: 'alice' }, { userCode: 'bob' }],
              pagination : {
                total   : 2,
                page    : 1,
                perPage : 100,
              },
            },
          };
        assert.fail(action);
      },
      /** @param {string} name Hecho. @param {Object} data Referencia mínima. @param {Object} options Misma transacción. @returns {Promise<void>} Simula sólo el almacenamiento del outbox, usando Mongo real. */ async publishEvent(
        name,
        data,
        options,
      ) {
        assert.equal(name, 'platform.resource.changed');
        assert.ok(validate(data, contract.publishedEvents[name]));
        if (failPublish) throw new Error('OUTBOX_UNAVAILABLE');
        await db.connection.db
          .collection('TEST_OUTBOX')
          .insertOne({ data }, { session: options.session });
      },
    };
    const models    = {};
    for (const [key, file] of [
      ['notifications', 'notifications'],
      ['broadcasts', 'notifications-broadcast'],
      ['recipients', 'notifications-recipient'],
    ]) {
      const original = require('../server/modules/notifications/models/' + file + '.model');
      models[key] = db.connection.model(
        original.modelName,
        original.schema,
        original.collection.name,
      );
      await models[key].createCollection();
      await models[key].createIndexes();
    }
    await db.connection.db.createCollection('TEST_OUTBOX');
    const repository =
      require('../server/modules/notifications/repositories/notifications-center.repository')({
        ...models,
        messaging,
      });
    const creation   =
      require('../server/modules/notifications/repositories/notifications.repository')({
        model : models.notifications,
        messaging,
      });
    const controller =
      require('../server/modules/notifications/controllers/notifications-center.controller').notificationsCenterController(
        {
          repository,
          messaging,
        },
      );
    const topics     = platformWindows({
      messaging,
      /** @param {string} operation Consulta propia. @param {Object} data Selector. @returns {Promise<Object>} Mismo controlador que usa Gateway. */ read : (
        operation,
        data,
      ) => controller.execute(operation, { data }),
    });
    const hub        = new RealtimeWindows({
      topics,
      receipts : {},
    });
    t.after(/** @returns {void} Cierra sólo suscripciones del ensayo. */ () => hub.close());
    const identity = {
      userCode  : 'alice',
      workspace : 'w',
      context,
    };
    const windows = new Map(),
      events      = [];
    hub.sessions.set('local', { windows });
    /** @param {Object} input Aviso nuevo. @returns {Promise<Object>} Alta autorizada. */ async function create(
      input = {},
    ) {
      return creation.create(
        {
          notificationCode : 'n1',
          workspace        : 'w',
          userCode         : 'alice',
          userAssigned     : 'alice',
          title            : 'A',
          content          : '',
          type             : 'info',
          ...input,
        },
        'global',
      );
    }
    await create();
    const savedCount = await models.notifications.countDocuments();
    failPublish = true;
    await assert.rejects(create({ notificationCode: 'rollback' }), /OUTBOX_UNAVAILABLE/);
    assert.equal(await models.notifications.countDocuments(), savedCount);
    failPublish = false;
    const pending = await controller.execute('preview', {
      data : {
        workspace : 'w',
        title     : 'Difusión',
        content   : 'Contenido',
        type      : 'info',
        scope     : 'global',
      },
    });
    const job     = pending.data;
    assert.equal(job.state, 'prepared');
    let clientReads = 0;
    /** @param {string} topic Dominio. @param {string} section Sección. @param {Object} query Consulta. @returns {Promise<Object>} Base y parche de la misma sección. */ async function observe(
      topic,
      section,
      query,
    ) {
      const spec = topics[topic][section];
      spec.validate(query);
      assert.throws(
        /** @returns {Object} Rechaza una base o identidad del cliente. */ () =>
          spec.validate({
            ...query,
            records   : [],
            principal : 'bob',
          }),
        { statusCode: 400 },
      );
      const subscriptionId = topic + '/' + section;
      const deliveries     = [];
      const window         = new ReadWindow({
        subscriptionId,
        watchVersion                                                                            : 1,
        topic,
        section,
        collections                                                                             : spec.collections,
        /** @returns {Promise<void>} La lectura canónica también revalida. */ async authorize() {
          if (!permitted) throw Object.assign(new Error('FORBIDDEN'), { statusCode: 403 });
        },
        /** @returns {Promise<Object>} Reconstruye sólo una ventana afectada. */ read           : () =>
          spec.read(query, identity),
        /** @param {Object} value Entrega. @returns {void} Registra sin consultar HTTP. */ emit : (
          value,
        ) => deliveries.push(value),
      });
      windows.set(subscriptionId, {
        window,
        query,
        identity,
      });
      assert.deepEqual(Object.keys(window.ready()).sort(), [
        'statusCode',
        'subscriptionId',
        'watchVersion',
      ]);
      const snapshot = await spec.read(query, identity);
      clientReads++;
      await window.bind({
        subscriptionId,
        watchVersion   : 1,
        query,
        ...snapshot,
        projectionHash : contentCursor(snapshot.data),
      });
      return {
        window,
        deliveries,
      };
    }
    const personal = await observe('platform.notification', 'personal-page', {
      workspace : 'w',
      page      : 1,
      perPage   : 1,
      sort      : 'title',
      order     : 'asc',
    });
    const detail   = await observe('platform.notification', 'personal-detail', {
      workspace : 'w',
      code      : 'n1',
    });
    const count    = await observe('platform.notification', 'count', { workspace: 'w' });
    const admin    = await observe('platform.notification', 'admin-page', {
      workspace : 'w',
      scope     : 'global',
      page      : 1,
      perPage   : 1,
    });
    await observe('platform.notification', 'admin-detail', {
      workspace : 'w',
      scope     : 'global',
      code      : 'n1',
    });
    await observe('platform.broadcast', 'page', {
      workspace : 'w',
      scope     : 'global',
      page      : 1,
      perPage   : 1,
    });
    const broadcast = await observe('platform.broadcast', 'detail', {
      scope : 'global',
      code  : job.broadcastCode,
    });
    assert.equal(clientReads, 7);
    assert.equal(personal.window.base.items[0].title, 'A');
    /** @returns {Promise<void>} Emula sólo la entrega confirmada del broker; todas las lecturas y diferencias son reales. */ async function flush() {
      const facts = await db.connection.db.collection('TEST_OUTBOX').find().toArray();
      await db.connection.db.collection('TEST_OUTBOX').deleteMany({});
      for (const fact of facts) {
        events.push(fact.data);
        await hub.changed('platform.' + fact.data.resource, fact.data);
      }
    }
    await flush();
    await controller.execute('personalView', {
      data : {
        code      : 'n1',
        workspace : 'w',
      },
    });
    await flush();
    assert.equal(count.window.base.notifications, 0);
    assert.equal(detail.window.base.viewed, true);
    await create({
      notificationCode : 'n2',
      title            : '0',
    });
    await flush();
    assert.equal(personal.window.base.items[0].notificationCode, 'n2');
    assert.equal(personal.window.base.pagination.total, 2);
    assert.equal(count.window.base.notifications, 1);
    for (let i = 0; i < 100; i++) {
      await repository.update({ notificationCode: 'n2' }, { title: '0-' + i });
      await flush();
    }
    const beforeDuplicate = personal.deliveries.length;
    await hub.changed('platform.notification', events.at(-1));
    assert.equal(personal.deliveries.length, beforeDuplicate);
    await controller.execute('personalDelete', {
      data : {
        workspace : 'w',
        code      : 'n2',
      },
    });
    await flush();
    assert.equal(personal.window.base.items[0].notificationCode, 'n1');
    assert.equal(personal.window.base.pagination.total, 1);
    const beforeForeign = personal.deliveries.length;
    await create({
      notificationCode : 'foreign',
      title            : '0',
      userCode         : 'bob',
      userAssigned     : 'bob',
    });
    await flush();
    assert.equal(personal.deliveries.length, beforeForeign);
    assert.equal(admin.window.base.pagination.total, 2);
    await controller.execute('confirm', {
      data : {
        code              : job.broadcastCode,
        confirmationToken : job.confirmationToken,
        scope             : 'global',
      },
    });
    await flush();
    assert.equal(broadcast.window.base.state, 'queued');
    const claimed = await repository.claim();
    await repository.persistRecipient(claimed, { userCode: 'alice' }, true);
    await repository.persistRecipient(claimed, { userCode: 'bob' }, false);
    await flush();
    assert.equal(broadcast.window.base.state, 'completed');
    assert.equal(broadcast.window.base.persistedCount, 1);
    assert.equal(broadcast.window.base.skippedCount, 1);
    assert.equal(count.window.base.notifications, 1);
    assert.equal(clientReads, 7);
    // Un cambio posterior al snapshot HTTP se conserva hasta instalar esa base.
    const raceQuery = {
      workspace : 'w',
      page      : 1,
      perPage   : 20,
    };
    const raceSpec  = topics['platform.notification']['personal-page'];
    const race      = new ReadWindow({
      subscriptionId                                                                       : 'race',
      watchVersion                                                                         : 1,
      topic                                                                                : 'platform.notification',
      section                                                                              : 'personal-page',
      collections                                                                          : raceSpec.collections,
      /** @returns {Promise<void>} Principal vigente del ensayo. */ async authorize() {},
      /** @returns {Promise<Object>} Lectura del productor tras una mutación real. */ read : () =>
        raceSpec.read(raceQuery, identity),
      /** @returns {void} La prueba inspecciona la base final. */ emit() {},
    });
    windows.set('race', {
      window : race,
      query  : raceQuery,
      identity,
    });
    const old = await raceSpec.read(raceQuery, identity);
    await create({
      notificationCode : 'during-http',
      title            : 'Concurrente',
    });
    await flush();
    await race.bind({
      subscriptionId : 'race',
      watchVersion   : 1,
      query          : raceQuery,
      ...old,
      projectionHash : contentCursor(old.data),
    });
    assert.ok(
      race.base.items.some(
        /** @param {Object} item Fila vigente. @returns {boolean} El cambio ocurrido durante HTTP no se pierde. */ (
          item,
        ) => item.notificationCode === 'during-http',
      ),
    );
    await repository.snapshot(
      /** @returns {Promise<void>} Un commit paralelo no altera el total del snapshot ya abierto. */ async () => {
        const before = await repository.count({
          workspace : 'w',
          userCode  : 'alice',
        });
        await create({
          notificationCode : 'concurrent-total',
          title            : 'Total coherente',
        });
        assert.equal(
          await repository.count({
            workspace : 'w',
            userCode  : 'alice',
          }),
          before,
        );
      },
    );
    await flush();
    permitted = false;
    await hub.changed('platform.notification', {
      resource  : 'notification',
      workspace : 'w',
      userCode  : 'alice',
      code      : '',
    });
    assert.ok(
      personal.deliveries.some(
        /** @param {Object} value Control. @returns {boolean} Revocación explícita. */ (value) =>
          value.type === 'revoked',
      ),
    );
    assert.equal(clientReads, 7);
  },
);
