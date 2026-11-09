/** @file Cinco ventanas documentales con contratos y productor reales; no se aceptan bases ni autoridad del cliente. */
'use strict';
const test                = require('node:test');
const assert              = require('node:assert/strict');
const { producerWindows } = require('./helpers/producer-window.cjs');
const {
  documentsResourceWindows,
} = require('../server/modules/realtime/services/documents-resource-window.service');
const manifest    = require('../server/contracts/service.json');
const sourcePath  = require('node:path').resolve(
  __dirname,
  '../../service-documents/server/contracts/service.json',
);
const source      = require('node:fs').existsSync(sourcePath) ? require(sourcePath) : null;
const definitions = [
  ['documents.repository', 'collection', 'documents-repositories-get-list'],
  ['documents.repository', 'resource', 'documents-repository-resolve'],
  ['documents.entry', 'page', 'documents-entries-get-list'],
  ['documents.entry', 'detail', 'documents-entry-get-one'],
  ['documents.entry', 'references', 'documents-entry-references'],
  ['documents.settings', 'detail', 'documents-settings-get'],
];
test(
  'cada ventana usa el esquema del productor',
  { skip: !source && 'Requiere checkout conjunto de Documents' },
  /** @returns {void} Paridad de frontera, sin conversiones ni listas en ACK. */ () => {
    const read = manifest.endpoints.find(
      /** @param {Object} entry Contrato. @returns {boolean} Lectura estándar. */
      (entry) => entry.name === 'realtime-read',
    );
    for (const [topic, section, name] of definitions) {
      const window = read.successData.oneOf.find(
        /** @param {Object} value Sobre. @returns {boolean} Ventana correspondiente. */
        (value) =>
          value.properties.control.properties.topic.enum[0] === topic &&
          value.properties.control.properties.section.enum[0] === section,
      );
      assert.deepEqual(
        window.properties.data,
        source.endpoints.find(
          /** @param {Object} value Capacidad. @returns {boolean} Acción canónica. */
          (value) => value.name === name,
        ).successData,
      );
    }
  },
);
test('los hechos documentales mínimos rechazan contenido ajeno', /** @returns {void} Valida la frontera privada incluso en el checkout aislado de Platform. */ () => {
  const registry = require('../server/contracts/registry');
  for (const name of ['documents.repository.changed', 'documents.settings.changed']) {
    if (source) assert.deepEqual(registry[name].inputSchema, source.publishedEvents[name]);
    assert.throws(
      /** @returns {Object} Un aviso no puede transportar filas privadas o autoridad. */
      () =>
        registry[name].input({
          data     : ['PRIVATE'],
          userCode : 'forged',
        }),
      { statusCode: 400 },
    );
  }
});
test(
  'Gateway lee raíces, árbol, detalle y settings del propietario y los cambios seleccionan su ventana',
  {
    skip :
      (!source || !process.env.TEST_MONGOD_BINARY) && 'Requiere Documents y MongoDB de integración',
  },
  /** @param {Object} t Prueba aislada. @returns {Promise<void>} Productor Mongo real, proyección y control de permisos. */ async (
    t,
  ) => {
    const f = await require('../../service-documents/test/helpers/documents-fixture.cjs').fixture(
      t,
      { delegated: false },
    );
    t.after(f.close);
    const roots    = await f.call('documents-repositories-get-list');
    const root     = roots.listItem.find(
      /** @param {Object} item Raíz. @returns {boolean} Espacio activo. */
      (item) => item.kind === 'workspace',
    );
    const folder   = await f.call('documents-folder-create', {
      repositoryId : root.id,
      name         : 'Documentación',
    });
    const identity = {
      userCode  : f.actor.user.userCode,
      workspace : f.actor.workspace,
      context   : f.actor,
    };
    let expected,
      queries = 0,
      failure = false;
    const windows = producerWindows(documentsResourceWindows, {
      messaging : {
        /** @param {string} service Propietario. @param {string} action Capacidad. @param {Object} query Consulta. @param {Object} options Contexto. @returns {Promise<Object>} Datos reales y errores públicos. */
        async request(service, action, query, options) {
          assert.equal(service, 'service-documents');
          assert.equal(action, expected);
          assert.equal(options.context, identity.context);
          queries++;
          if (failure) return { statusCode: 503 };
          try {
            return {
              statusCode : 200,
              data       : await f.call(action, query),
            };
          } catch (error) {
            if (!error.statusCode) throw error;
            return {
              statusCode      : error.statusCode,
              descriptionCode : error.descriptionCode,
            };
          }
        },
      },
    });
    for (const [topic, section, action] of definitions.filter(
      /** @param {Array} entry Sección. @returns {boolean} La referencia de recurso tiene integración separada con Projects/Support. */
      (entry) => entry[1] !== 'resource',
    )) {
      expected = action;
      const spec = windows[topic][section],
        query    =
          section === 'page'
            ? {
                repositoryId : root.id,
                page         : 1,
                perPage      : 20,
              }
            : section === 'references'
              ? { entryIds: [folder.id] }
              : topic === 'documents.entry'
                ? { entryId: folder.id }
                : {};
      spec.validate(query);
      assert.throws(
        /** @returns {Object} El cliente no puede acreditar los registros visibles. */
        () =>
          spec.validate({
            ...query,
            records  : [folder],
            userCode : 'foreign',
          }),
        { statusCode: 400 },
      );
      const base   = (await spec.read(query, identity)).data;
      const change =
        topic === 'documents.settings'
          ? {
              parameter : 'documents-settings',
              revision  : 1,
            }
          : {
              repositoryId : root.id,
              entryId      : folder.id,
              workspace    : f.actor.workspace,
            };
      assert.equal(spec.affects(query, change, identity, base), true);
      assert.equal(
        spec.affects(
          query,
          {
            repositoryId : 'f'.repeat(24),
            entryId      : 'e'.repeat(24),
            workspace    : 'foreign',
          },
          identity,
          base,
        ),
        false,
      );
      assert.equal(
        spec.affects(
          query,
          {
            repositoryId : 'f'.repeat(24),
            workspace    : 'foreign',
            kind         : 'workspace',
          },
          identity,
          base,
        ),
        false,
      );
      const before = queries;
      assert.equal(
        spec.affects(
          query,
          {
            control : {
              kind : 'user',
              code : identity.userCode,
            },
          },
          identity,
          base,
        ),
        true,
      );
      assert.equal(queries, before);
      failure = true;
      await assert.rejects(spec.read(query, identity), { statusCode: 503 });
      failure = false;
    }
    f.actor.policies = [];
    expected = 'documents-entry-get-one';
    await assert.rejects(windows['documents.entry'].detail.read({ entryId: folder.id }, identity), {
      statusCode : 403,
    });
    expected = 'documents-settings-get';
    await assert.rejects(windows['documents.settings'].detail.read({}, identity), {
      statusCode : 403,
    });
  },
);
