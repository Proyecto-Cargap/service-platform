/** @file Navegación, preferencias y versiones: escritura/outbox atómicos y ventanas canónicas sobre Mongo aislado. */
'use strict';
const test = require('node:test'),
  assert   = require('node:assert/strict'),
  mongoose = require('mongoose');
const { startIsolatedMongo } = require('./helpers/isolated-mongo.cjs');
const { platformWindows }    = require('../server/modules/realtime/services/platform-window.service');
const { RealtimeWindows }    = require('../server/library/realtime/windows');
const { ReadWindow }         = require('../server/library/realtime/read-window');
const { contentCursor }      = require('../server/library/realtime/window-delta');
const { configurationTag }   = require('../server/shared/configuration-tag');
const { validate }           = require('../server/contracts/action-dto');
const schema                 = require('../server/contracts/service.json').publishedEvents[
  'platform.resource.changed'
];
test(
  'Home publica diferencias de navegación y releases sin lecturas adicionales; rollback y permisos son efectivos',
  {
    skip    : !process.env.TEST_MONGOD_BINARY,
    timeout : 60000,
  },
  /** @param {Object} t Prueba aislada. @returns {Promise<void>} Repositorios y coordinador reales. */ async (
    t,
  ) => {
    const db = await startIsolatedMongo({
      binary : process.env.TEST_MONGOD_BINARY,
      mongoose,
    });
    t.after(db.stop);
    const context = {
      user        : { userCode: 'U1' },
      workspace   : 'W1',
      sessionCode : 'session',
      policies    : [
        'platform.configuration.update.global',
        ...['create', 'view', 'update', 'delete'].map(
          /** @param {string} action Acción. @returns {string} Permiso exacto. */ (action) =>
            'platform.release.' + action + '.global',
        ),
      ],
    };
    let failPublish = false,
      permitted     = true;
    const messaging = {
      /** @returns {Object} Principal de la prueba. */ getContext                                                                              : () => context,
      /** @param {Object} identity Principal acreditado. @param {Function} work Caso canónico. @returns {*} Resultado local. */ runWithContext :
        (identity, work) => work(),
      /** @param {number} statusCode Estado. @param {*} data Datos. @returns {Object} Sobre del productor. */ successResponse                  :
        (statusCode, data) => ({
          statusCode,
          data,
        }),
      /** @param {string} name Nombre. @param {Object} data Hecho. @param {Object} options Misma transacción. @returns {Promise<void>} Outbox de prueba durable. */ async publishEvent(
        name,
        data,
        options,
      ) {
        assert.equal(name, 'platform.resource.changed');
        assert(validate(data, schema));
        if (failPublish) throw new Error('OUTBOX_FAILED');
        await db.connection.db
          .collection('TEST_OUTBOX')
          .insertOne({ data }, { session: options.session });
      },
    };
    const models    = {};
    for (const module of ['configuration', 'release']) {
      const original = require('../server/modules/' + module + '/models/' + module + '.model');
      models[module] = db.connection.model(
        original.modelName,
        original.schema,
        original.collection.name,
      );
      await models[module].createCollection();
      await models[module].createIndexes();
    }
    await db.connection.db.createCollection('TEST_OUTBOX');
    await db.connection.db.collection('PLATFORM_RELEASE_CONTROL').insertOne({
      _id      : 'global',
      revision : 0,
    });
    const menus = {
      menus : {
        public : [],
        main   : [
          {
            key        : 'home',
            label      : 'Inicio',
            icon       : 'home',
            routerLink : '/app/home',
          },
        ],
      },
    };
    await models.configuration.create({
      _id          : 'global',
      workspace    : 'allWorkspaces',
      userCreate   : '',
      userAssigned : '',
      data         : menus,
    });
    const configurationRepository =
      require('../server/modules/configuration/repositories/configuration.repository')({
        model : models.configuration,
        messaging,
      });
    const configuration           =
      require('../server/modules/configuration/controllers/configuration.controller').configurationController(
        {
          repository : configurationRepository,
          messaging,
        },
      );
    const releases                = require('../server/modules/release/repositories/release.repository')({
      model : models.release,
      messaging,
    });
    let nextId                    = 0;
    const release                 =
      require('../server/modules/release/controllers/release.controller').releaseController({
        repository  : releases,
        messaging,
        identifiers : {
          /** @returns {Promise<string>} Identidad estable. */ async getObjectCode() {
            return 'R' + ++nextId;
          },
        },
        /** @returns {Promise<Object>} Artefacto sintético verificado. */ async verifyWebBuild() {
          return {
            id          : 'build',
            sourceSha   : 'a'.repeat(40),
            environment : 'test',
          };
        },
      });
    const topics                  = platformWindows({
      messaging,
      /** @param {string} operation Capacidad registrada. @param {Object} data Consulta. @returns {Promise<Object>} Caso canónico con autorización del productor. */ async read(
        operation,
        data,
      ) {
        if (!permitted) throw Object.assign(new Error('FORBIDDEN'), { statusCode: 403 });
        if (operation === 'configuration') return configuration.getOne();
        return release[operation.slice(8)]({ data });
      },
    });
    const hub = new RealtimeWindows({
        topics,
        receipts : {},
      }),
      windows = new Map();
    hub.sessions.set('local', { windows });
    t.after(/** @returns {void} Libera ventanas propias. */ () => hub.close());
    const identity  = {
      userCode  : 'U1',
      workspace : 'W1',
      context,
    };
    let clientReads = 0;
    /** @param {string} topic Dominio. @param {string} section Sección. @param {Object} query Selector. @returns {Promise<Object>} Base HTTP y entregas posteriores independientes. */
    async function observe(topic, section, query = {}) {
      const spec = topics[topic][section];
      spec.validate(query);
      const subscriptionId = topic + '/' + section,
        deliveries         = [];
      const window = new ReadWindow({
        subscriptionId,
        watchVersion                                                                        : 1,
        topic,
        section,
        collections                                                                         : spec.collections,
        /** @returns {Promise<void>} Acceso vigente. */ async authorize() {
          if (!permitted) throw Object.assign(new Error('FORBIDDEN'), { statusCode: 403 });
        },
        /** @returns {Promise<Object>} Sólo la ventana afectada. */ read                    : () =>
          spec.read(query, identity),
        /** @param {Object} delivery Diferencia. @returns {void} Registro sin HTTP. */ emit : (
          delivery,
        ) => deliveries.push(delivery),
      });
      windows.set(subscriptionId, {
        window,
        query,
        identity,
      });
      assert.equal(Object.hasOwn(window.ready(), 'data'), false);
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
    /** @returns {Promise<void>} Entrega sólo hechos confirmados; no hay temporizadores. */
    async function flush() {
      const events = await db.connection.db.collection('TEST_OUTBOX').find().toArray();
      await db.connection.db.collection('TEST_OUTBOX').deleteMany({});
      for (const event of events) await hub.changed('platform.' + event.data.resource, event.data);
    }
    const navigation = await observe('platform.configuration', 'navigation'),
      current        = await observe('platform.release', 'current'),
      history        = await observe('platform.release', 'history', {
        page    : 1,
        perPage : 1,
      }),
      page           = await observe('platform.release', 'page', {
        page    : 1,
        perPage : 1,
        scope   : 'global',
      }),
      detail         = await observe('platform.release', 'detail', {
        code  : 'R1',
        scope : 'global',
      });
    assert.equal(current.window.base, null);
    assert.equal(detail.window.base, null);
    assert.equal(clientReads, 5);
    failPublish = true;
    await assert.rejects(
      configuration.update({
        data : {
          configuration : {
            menus : {
              public : [],
              main   : [],
            },
          },
          scope         : 'global',
          expectedTag   : configurationTag(menus),
        },
      }),
      { statusCode: 503 },
    );
    assert.deepEqual((await configuration.getOne()).data, menus);
    failPublish = false;
    for (let i = 1; i <= 100; i++) {
      const data = {
        menus : {
          public : [],
          main   : [
            {
              ...menus.menus.main[0],
              label : 'Inicio ' + i,
            },
          ],
        },
      };
      await configuration.update({
        data : {
          configuration : data,
          scope         : 'global',
        },
      });
      await flush();
    }
    assert.equal(navigation.window.base.configuration.menus.main[0].label, 'Inicio 100');
    assert.equal(clientReads, 5);
    await release.create({
      data : {
        version     : '1.0.0',
        description : 'Primera',
        scope       : 'global',
      },
    });
    await flush();
    assert.equal(page.window.base.items[0].releaseCode, 'R1');
    assert.equal(detail.window.base.description, 'Primera');
    await release.published({
      data : {
        code            : 'R1',
        scope           : 'global',
        expectedBuildId : 'build',
      },
    });
    await flush();
    assert.equal(current.window.base.releaseCode, 'R1');
    await release.create({
      data : {
        version     : '2.0.0',
        description : 'Segunda',
        scope       : 'global',
      },
    });
    await release.published({
      data : {
        code            : 'R2',
        scope           : 'global',
        expectedBuildId : 'build',
      },
    });
    await flush();
    assert.equal(current.window.base.releaseCode, 'R2');
    assert.equal(history.window.base.items[0].releaseCode, 'R1');
    assert.equal(detail.window.base.state, '2-released');
    failPublish = true;
    await assert.rejects(
      release.update({
        data : {
          code        : 'R2',
          scope       : 'global',
          description : 'No confirmada',
        },
      }),
    );
    failPublish = false;
    assert.equal((await release.current()).data.description, 'Segunda');
    assert.equal(clientReads, 5);
    permitted = false;
    await hub.changed('platform.release', {
      resource  : 'release',
      code      : '',
      workspace : 'allWorkspaces',
    });
    assert.equal(current.deliveries.at(-1).type, 'revoked');
    assert.equal(clientReads, 5);
  },
);
