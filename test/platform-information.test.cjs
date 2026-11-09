/** @file Pruebas de comportamiento de platform-information.test en service-platform. */
'use strict';
const test   = require('node:test');
const assert = require('node:assert/strict');
const {
  platformInformation,
} = require('../server/modules/configuration/contracts/configuration-information');
const {
  configurationService,
} = require('../server/modules/configuration/services/configuration.service');
const {
  configurationController,
} = require('../server/modules/configuration/controllers/configuration.controller');
const createRepository     = require('../server/modules/configuration/repositories/configuration.repository');
const { configurationTag } = require('../server/shared/configuration-tag');
/**
 * Ejecuta information dentro del flujo de platform-information.test.
 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
const information          = () => ({
  title     : 'Example',
  publicUrl : 'https://example.invalid',
  logos     : { email: '/assets/branding/logoLight.svg' },
  theme     : { primary: { 900: '#78192b' } },
});
/**
 * Ejecuta menus dentro del flujo de platform-information.test.
 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
const menus                = () => ({
  public : [],
  main   : [],
});
const context              = {
  user     : { userCode: 'admin' },
  policies : ['platform.configuration.update.global'],
};

test('Platform normalizes the central information and exposes no navigation, UI license or connection data' /**
 * Verifica el comportamiento esperado del caso de prueba.
 * @returns {void} Finaliza después de aplicar los efectos de la operación.
 */, () => {
  const data   = {
    ...information(),
    menus     : menus(),
    pwa       : { name: 'private' },
    uiLicense : 'never-return',
    endpoints : { api: 'never-return' },
    images    : { loginDesktop: 'never-return' },
  };
  const before = structuredClone(data);
  const result = platformInformation(data);
  assert.deepEqual(result, {
    ...information(),
    logos : { email: 'https://example.invalid/assets/branding/logoLight.svg' },
  });
  assert.deepEqual(data, before);
  assert.deepEqual(Object.keys(result).sort(), ['logos', 'publicUrl', 'theme', 'title']);
  assert.deepEqual(
    platformInformation({
      ...information(),
      logos : { email: '' },
    }).logos,
    {
      email : '',
    },
  );
  assert.equal(
    platformInformation({
      ...information(),
      publicUrl : 'http://localhost:4200',
    }).publicUrl,
    'http://localhost:4200',
  );
});

test('invalid central information fails without falling back to consumer environment or web configuration' /**
 * Verifica el comportamiento esperado del caso de prueba.
 * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
 */, async () => {
  const invalid = [
    undefined,
    {},
    {
      ...information(),
      title : ' ',
    },
    {
      ...information(),
      publicUrl : 'https://u:p@example.invalid',
    },
    {
      ...information(),
      publicUrl : 'http://example.invalid',
    },
    {
      ...information(),
      publicUrl : 'https://example.invalid/',
    },
    {
      ...information(),
      publicUrl : 'https://example.invalid?x=1',
    },
    {
      ...information(),
      logos : { email: 'javascript:alert(1)' },
    },
    {
      ...information(),
      logos : { email: 'http://cdn.example.invalid/logo.png' },
    },
    {
      ...information(),
      theme : { primary: { 900: '#fff' } },
    },
  ];
  for (const data of invalid) {
    assert.throws(
      /**
       * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      () => platformInformation(data),
    );
    await assert.rejects(
      configurationService({
        repository : {
          /**
           * Resuelve getInformation aplicando los filtros y colaboradores del módulo.
           * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          getInformation : async () => ({ data }),
        },
      }).getInformation(),
      /**
       * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
       * @param {Error} error Fallo recibido por la continuación.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      (error) =>
        error.statusCode === 503 &&
        error.descriptionCode === 'N0005-CONFIG-INFORMATION-UNAVAILABLE',
    );
  }
  await assert.rejects(
    configurationService({
      repository : {
        /**
         * Resuelve getInformation aplicando los filtros y colaboradores del módulo.
         * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
         */
        getInformation : async () => {
          throw new Error('database unavailable');
        },
      },
    }).getInformation(),
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @param {Error} error Fallo recibido por la continuación.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    (error) => error.statusCode === 503,
  );
});

test('RPC separates server information from public menus and does not write on either read' /**
 * Verifica el comportamiento esperado del caso de prueba.
 * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
 */, async () => {
  const data       = {
    ...information(),
    menus     : menus(),
    uiLicense : 'never-return',
  };
  const repository = {
    /**
     * Obtiene el valor solicitado.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    get            : async () => ({ data }),
    /**
     * Resuelve getInformation aplicando los filtros y colaboradores del módulo.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    getInformation : async () => ({ data }),
    /**
     * Ejecuta put dentro del flujo de platform-information.test.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    put            : () => assert.fail('unexpected write'),
  };
  const messaging  = {
    /**
     * Ejecuta successResponse dentro del flujo de platform-information.test.
     * @param {number} statusCode Valor de statusCode utilizado por esta operación.
     * @param {Object} data Datos de entrada de la operación.
     * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    successResponse : (statusCode, data) => ({
      statusCode,
      data,
    }),
  };
  const controller = configurationController({
    repository,
    messaging,
  });
  assert.deepEqual(await controller.getOne(), {
    statusCode : 200,
    data       : { menus: menus() },
  });
  const server = await controller.getInformation();
  assert.deepEqual(server, {
    statusCode : 200,
    data       : platformInformation(information()),
  });
  assert.equal(Object.hasOwn(server.data, 'uiLicense'), false);
  // A missing information record does not make public navigation depend on it.
  /**
   * Resuelve getInformation aplicando los filtros y colaboradores del módulo.
   * @returns {Promise<null>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  repository.getInformation = async () => null;
  await assert.rejects(
    controller.getInformation(),
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @param {Error} error Fallo recibido por la continuación.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    (error) => error.statusCode === 503,
  );
  assert.deepEqual((await controller.getOne()).data, { menus: menus() });
});

test('navigation writes and concurrency preserve central platform information and return only menus' /**
 * Verifica el comportamiento esperado del caso de prueba.
 * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
 */, async () => {
  const stored = {
    data         : {
      ...information(),
      menus : menus(),
    },
    workspace    : 'allWorkspaces',
    userCreate   : '',
    userAssigned : '',
  };
  const before = platformInformation(stored.data);
  let writes   = 0;
  const model  = {
    db : {
      /** @param {Function} work Unidad. @returns {Promise<*>} Sesión para comprobar que la configuración usa una unidad atómica. */ async transaction(
        work,
      ) {
        return work({ id: 'config-write' });
      },
    },
    /**
     * Resuelve findOne aplicando los filtros y colaboradores del módulo.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {*} projection Valor de projection utilizado por esta operación.
     * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    findOne(filter, projection) {
      assert(filter);
      if (Object.hasOwn(projection, 'data.title')) {
        assert.deepEqual(projection, {
          'data.title'             : 1,
          'data.publicUrl'         : 1,
          'data.logos.email'       : 1,
          'data.theme.primary.900' : 1,
        });
        return {
          /**
           * Ejecuta lean dentro del flujo de platform-information.test.
           * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          lean : async () => ({ data: structuredClone(stored.data) }),
        };
      }
      assert.deepEqual(projection, { 'data.menus': 1 });
      return {
        /** @param {Object} value Sesión. @returns {Object} Consulta de la misma unidad. */ session(
          value,
        ) {
          assert.equal(value.id, 'config-write');
          return this;
        },
        /**
         * Ejecuta lean dentro del flujo de platform-information.test.
         * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        lean : async () => ({
          data : { menus: structuredClone(stored.data.menus) },
        }),
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
      writes++;
      assert.deepEqual(changes.$set['data.menus'], {
        public : [
          {
            key        : 'login',
            label      : 'Acceder',
            icon       : 'home',
            routerLink : '/login',
          },
        ],
        main   : [],
      });
      assert.equal(Object.hasOwn(changes.$set, 'data'), false);
      assert.deepEqual(filter.$and.at(-1), { 'data.menus': menus() });
      assert.deepEqual(options.projection, { 'data.menus': 1 });
      stored.data.menus = structuredClone(changes.$set['data.menus']);
      return {
        /**
         * Ejecuta lean dentro del flujo de platform-information.test.
         * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        lean : async () => structuredClone(stored),
      };
    },
  };
  const repository = createRepository({
      model,
      messaging : {
        /** @param {string} name Hecho. @param {Object} data Referencia. @param {Object} options Unidad. @returns {Promise<void>} Hecho mínimo de navegación con la misma sesión. */ async publishEvent(
          name,
          data,
          options,
        ) {
          assert.equal(name, 'platform.resource.changed');
          assert.equal(data.resource, 'configuration');
          assert.equal(options.session.id, 'config-write');
        },
      },
    }),
    service        = configurationService({ repository });
  assert.deepEqual(await service.getInformation(), before);
  const data     = {
    menus : {
      public : [
        {
          key        : 'login',
          label      : 'Acceder',
          icon       : 'home',
          routerLink : '/login',
        },
      ],
      main   : [],
    },
  };
  const expected = configurationTag({ menus: menus() });
  const result   = await service.put(data, context, 'global', expected);
  assert.deepEqual(result, data);
  assert.deepEqual(await service.getInformation(), before);
  await assert.rejects(
    service.put(data, context, 'global', expected),
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @param {Error} error Fallo recibido por la continuación.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    (error) => error.statusCode === 412,
  );
  assert.equal(writes, 1);
});
