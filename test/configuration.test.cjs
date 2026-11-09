/** @file Pruebas de comportamiento de configuration.test en service-platform. */
'use strict';
const test   = require('node:test');
const assert = require('node:assert/strict');
const {
  configurationService,
  validateConfiguration,
} = require('../server/modules/configuration/services/configuration.service');
const createRepository     = require('../server/modules/configuration/repositories/configuration.repository');
const { configurationTag } = require('../server/shared/configuration-tag');
/**
 * Ejecuta fixture dentro del flujo de configuration.test.
 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
const fixture              = () => ({
  menus : {
    public : [
      {
        key        : 'login',
        label      : 'Acceder',
        icon       : 'home',
        routerLink : '/login',
      },
    ],
    main   : [
      {
        key        : 'home',
        label      : 'Inicio',
        icon       : 'home',
        routerLink : '/app/home',
      },
    ],
  },
});
const context              = {
  user     : { userCode: 'test-user' },
  policies : ['platform.configuration.update.global'],
};

test('configuration exposes exclusively the current navigation contract' /**
 * Verifica el comportamiento esperado del caso de prueba.
 * @returns {void} Finaliza después de aplicar los efectos de la operación.
 */, () => {
  assert.equal(validateConfiguration(fixture()).menus.main[0].key, 'home');
  for (const key of [
    'pwa',
    'images',
    'uiLicense',
    'endpoints',
    'title',
    'publicUrl',
    'application',
    'logos',
    'theme',
  ])
    assert.throws(
      /**
       * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      () =>
        validateConfiguration({
          ...fixture(),
          [key] : {},
        }),
    );
  const invalid = fixture();
  invalid.menus.main[0].routerLink = '/login';
  assert.throws(
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    () => validateConfiguration(invalid),
  );
});
test('public read does not write and invalid persisted navigation fails closed' /**
 * Verifica el comportamiento esperado del caso de prueba.
 * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
 */, async () => {
  const repository = {
    /**
     * Obtiene el valor solicitado.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    get : async () => ({ data: fixture() }),
    /**
     * Ejecuta put dentro del flujo de configuration.test.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    put : () => assert.fail('unexpected write'),
  };
  assert.deepEqual(await configurationService({ repository }).get(), fixture());
  /**
   * Obtiene el valor solicitado.
   * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  repository.get = async () => ({ data: {} });
  await assert.rejects(
    configurationService({ repository }).get(),
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @param {*} e Valor de e utilizado por esta operación.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    (e) => e.statusCode === 503,
  );
});
test('update preserves authorization and rejects presentation fields before persistence' /**
 * Verifica el comportamiento esperado del caso de prueba.
 * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
 */, async () => {
  let writes    = 0;
  const service = configurationService({
    repository : {
      /**
       * Ejecuta put dentro del flujo de configuration.test.
       * @param {Object} data Datos de entrada de la operación.
       * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      put : async (data) => {
        writes++;
        return { data };
      },
    },
  });
  await assert.rejects(
    service.put(fixture(), {}, 'global'),
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @param {*} e Valor de e utilizado por esta operación.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    (e) => e.statusCode === 403,
  );
  await assert.rejects(
    service.put(
      {
        ...fixture(),
        title : 'changed',
      },
      context,
      'global',
    ),
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @param {*} e Valor de e utilizado por esta operación.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    (e) => e.statusCode === 400,
  );
  assert.equal(writes, 0);
  assert.deepEqual(await service.put(fixture(), context, 'global'), fixture());
  assert.equal(writes, 1);
});
test('repository projects menus and keeps ETag optimistic concurrency consistent' /**
 * Verifica el comportamiento esperado del caso de prueba.
 * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
 */, async () => {
  const data = fixture(),
    expected = configurationTag(data);
  let updated      = 0;
  const model      = {
    db : {
      /** @param {Function} work Unidad. @returns {Promise<*>} Sesión sintética para comprobar su propagación. */ async transaction(
        work,
      ) {
        return work({ id: 'transaction' });
      },
    },
    /**
     * Resuelve findOne aplicando los filtros y colaboradores del módulo.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {*} projection Valor de projection utilizado por esta operación.
     * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    findOne(filter, projection) {
      assert.deepEqual(projection, { 'data.menus': 1 });
      assert(filter);
      return {
        /** @param {Object} value Sesión. @returns {Object} Consulta dentro de la misma unidad. */ session(
          value,
        ) {
          assert.equal(value.id, 'transaction');
          return this;
        },
        /**
         * Ejecuta lean dentro del flujo de configuration.test.
         * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        lean : async () => ({ data }),
      };
    },
    /**
     * Resuelve findOneAndUpdate aplicando los filtros y colaboradores del módulo.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {Object} changes Valor de changes utilizado por esta operación.
     * @param {Object} options Opciones que delimitan el comportamiento de la operación.
     * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    findOneAndUpdate(filter, changes, options) {
      updated++;
      assert.deepEqual(filter.$and.at(-1), { 'data.menus': data.menus });
      assert.deepEqual(changes.$set['data.menus'], data.menus);
      assert.equal(Object.hasOwn(changes.$set, 'data'), false);
      assert.deepEqual(options.projection, { 'data.menus': 1 });
      assert.equal(options.runValidators, true);
      return {
        /**
         * Ejecuta lean dentro del flujo de configuration.test.
         * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        lean : async () => ({ data: { menus: changes.$set['data.menus'] } }),
      };
    },
  };
  const repository = createRepository({
    model,
    messaging : {
      /** @param {string} name Hecho. @param {Object} data Identidad. @param {Object} options Unidad. @returns {Promise<void>} Confirma la misma transacción de navegación. */ async publishEvent(
        name,
        data,
        options,
      ) {
        assert.equal(name, 'platform.resource.changed');
        assert.equal(data.resource, 'configuration');
        assert.equal(options.session.id, 'transaction');
      },
    },
  });
  assert.deepEqual(await repository.get(), { data });
  assert.deepEqual(await repository.put(data, context, 'global', expected), {
    data,
  });
  await assert.rejects(
    repository.put(data, context, 'global', '"cfg-' + '0'.repeat(64) + '"'),
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @param {*} e Valor de e utilizado por esta operación.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    (e) => e.statusCode === 412,
  );
  assert.equal(updated, 1);
});
