/** @file Pruebas de comportamiento de release-configuration.test en service-platform. */
'use strict';
const test                = require('node:test');
const assert              = require('node:assert/strict');
const { releaseWebBuild } = require('../server/modules/release/services/release-web-build');
const identity            = {
  id          : 'fixture-release',
  sourceSha   : 'a'.repeat(40),
  environment : 'test',
};

test('Release verifies builds against the current central Platform URL rather than an environment or request URL' /**
 * Verifica el comportamiento esperado del caso de prueba.
 * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
 */, async () => {
  let publicUrl = 'https://first.example.invalid',
    reads       = 0;
  const requested = [];
  const verify    = releaseWebBuild({
    environment    : 'test',
    /**
     * Resuelve getInformation aplicando los filtros y colaboradores del módulo.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    getInformation : async () => {
      reads++;
      return { publicUrl };
    },
    /**
     * Ejecuta fetcher dentro del flujo de release-configuration.test.
     * @param {*} url Valor de url utilizado por esta operación.
     * @param {Object} options Opciones que delimitan el comportamiento de la operación.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    fetcher        : async (url, options) => {
      requested.push(url);
      assert.equal(options.redirect, 'error');
      assert.equal(options.cache, 'no-store');
      const pathname = new URL(url).pathname;
      const body     =
        pathname === '/healthz'
          ? {
              status    : 'ok',
              releaseId : identity.id,
              sourceSha : identity.sourceSha,
            }
          : pathname === '/build-info.json'
            ? identity
            : {
                appData   : identity,
                hashTable : { '/index.html': 'fixture' },
              };
      return {
        ok         : true,
        redirected : false,

        /**
         * Ejecuta json dentro del flujo de release-configuration.test.
         * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        json       : async () => body,
      };
    },
  });
  assert.deepEqual(await verify(), identity);
  publicUrl = 'https://second.example.invalid';
  assert.deepEqual(await verify(), identity);
  assert.equal(reads, 2);
  assert(
    requested.slice(0, 3).every(
      /**
       * Comprueba la condición requerida para cada elemento.
       * @param {*} url Valor de url utilizado por esta operación.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      (url) => url.startsWith('https://first.example.invalid/'),
    ),
  );
  assert(
    requested.slice(3).every(
      /**
       * Comprueba la condición requerida para cada elemento.
       * @param {*} url Valor de url utilizado por esta operación.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      (url) => url.startsWith('https://second.example.invalid/'),
    ),
  );
});

test('unavailable configuration or unsafe origins never cause an HTTP request' /**
 * Verifica el comportamiento esperado del caso de prueba.
 * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
 */, async () => {
  for (const getInformation of [
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
     */
    async () => {
      throw new Error('unavailable');
    },
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async () => ({ publicUrl: 'https://user:pass@example.invalid' }),
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async () => ({ publicUrl: 'http://external.example.invalid' }),
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async () => ({ publicUrl: 'https://example.invalid/redirect' }),
  ]) {
    await assert.rejects(
      releaseWebBuild({
        environment : 'test',
        getInformation,
        /**
         * Ejecuta fetcher dentro del flujo de release-configuration.test.
         * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        fetcher     : () => assert.fail('unexpected HTTP request'),
      })(),
      /**
       * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
       * @param {Error} error Fallo recibido por la continuación.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      (error) =>
        error.statusCode === 503 && error.descriptionCode === 'RELEASE_PUBLIC_URL_REQUIRED',
    );
  }
});

test('the verification deadline also bounds the central configuration lookup' /**
 * Verifica el comportamiento esperado del caso de prueba.
 * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
 */, async () => {
  await assert.rejects(
    releaseWebBuild({
      environment    : 'test',
      /**
       * Resuelve getInformation aplicando los filtros y colaboradores del módulo.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      getInformation : () =>
        new Promise(
          /**
           * Coordina la resolución o el rechazo de la operación asíncrona.
           * @returns {void} Finaliza después de aplicar los efectos de la operación.
           */
          () => {},
        ),
      timeoutMs      : 20,
      /**
       * Ejecuta fetcher dentro del flujo de release-configuration.test.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      fetcher        : () => assert.fail('unexpected HTTP request'),
    })(),
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @param {Error} error Fallo recibido por la continuación.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    (error) =>
      error.statusCode === 503 && error.descriptionCode === 'RELEASE_WEB_BUILD_UNAVAILABLE',
  );
});
