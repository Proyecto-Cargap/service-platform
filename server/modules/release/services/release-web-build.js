/** @file Verifica la identidad de la web desplegada únicamente en el dominio confiable del entorno. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');

/**
 * @param {Object} dependencies Entorno del servidor, transporte HTTP y plazo total de verificación.
 * @returns {Function} Comprobador inyectable de metadatos públicos; nunca acepta URLs del cliente.
 */
function releaseWebBuild({
  environment,
  publicUrl,
  getInformation,
  fetcher,
  timeoutMs = 5000,
} = {}) {
  /**
   * Ejecuta verify dentro del flujo de release-web-build.
   * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  return async function verify() {
    const controller = new AbortController();
    let deadline;
    const timeout = new Promise(
      /**
       * Coordina la resolución o el rechazo de la operación asíncrona.
       * @param {*} _ Valor de _ utilizado por esta operación.
       * @param {Function} reject Continuación que rechaza la promesa con su causa.
       * @returns {void} Finaliza después de aplicar los efectos de la operación.
       */
      (_, reject) => {
        deadline = setTimeout(
          /**
           * Ejecuta la continuación cuando vence el intervalo configurado.
           * @returns {void} Finaliza después de aplicar los efectos de la operación.
           */
          () => {
            controller.abort();
            reject(new ApplicationError(503, 'RELEASE_WEB_BUILD_UNAVAILABLE'));
          },
          timeoutMs,
        );
      },
    );
    try {
      let origin;
      try {
        const information   = await Promise.race([
          Promise.resolve().then(
            /**
             * Procesa el resultado de la operación asíncrona.
             * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
             */
            () => (publicUrl === undefined ? getInformation() : { publicUrl }),
          ),
          timeout,
        ]);
        const configuredUrl = information.publicUrl;
        const parsed        = new URL(configuredUrl);
        if (
          !environment ||
          parsed.origin !== configuredUrl ||
          parsed.username ||
          parsed.password ||
          (parsed.protocol !== 'https:' &&
            !(parsed.protocol === 'http:' && ['localhost', '127.0.0.1'].includes(parsed.hostname)))
        )
          throw new Error();
        origin = parsed.origin;
      } catch (error) {
        if (
          error instanceof ApplicationError &&
          error.descriptionCode === 'RELEASE_WEB_BUILD_UNAVAILABLE'
        )
          throw error;
        throw new ApplicationError(503, 'RELEASE_PUBLIC_URL_REQUIRED');
      }
      const result                    = await Promise.race([
        Promise.all(
          ['/healthz', '/build-info.json', '/ngsw.json'].map(
            /**
             * Proyecta cada elemento en la estructura requerida por la operación.
             * @param {*} path Valor de path utilizado por esta operación.
             * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
             */
            async (path) => {
              const response = await fetcher(origin + path, {
                signal   : controller.signal,
                redirect : 'error',
                cache    : 'no-store',
                headers  : { Accept: 'application/json' },
              });
              if (!response.ok || response.redirected)
                throw new ApplicationError(503, 'RELEASE_WEB_BUILD_UNAVAILABLE');
              return response.json();
            },
          ),
        ),
        timeout,
      ]);
      const [health, build, manifest] = result;
      if (
        !build ||
        typeof build !== 'object' ||
        Array.isArray(build) ||
        typeof build.id !== 'string' ||
        !/^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$/.test(build.id) ||
        typeof build.sourceSha !== 'string' ||
        !/^[a-f0-9]{40}$/.test(build.sourceSha) ||
        build.environment !== environment ||
        health?.status !== 'ok' ||
        health.releaseId !== build.id ||
        health.sourceSha !== build.sourceSha ||
        !manifest?.appData ||
        !manifest.hashTable ||
        typeof manifest.hashTable !== 'object' ||
        Array.isArray(manifest.hashTable) ||
        Object.keys(manifest.hashTable).length === 0 ||
        ['id', 'sourceSha', 'environment'].some(
          /**
           * Comprueba si el elemento satisface la condición.
           * @param {string} key Clave del campo o recurso que se procesa.
           * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          (key) => manifest.appData[key] !== build[key],
        )
      )
        throw new ApplicationError(503, 'RELEASE_WEB_BUILD_UNVERIFIED');
      return {
        id        : build.id,
        sourceSha : build.sourceSha,
        environment,
      };
    } catch (error) {
      controller.abort();
      if (error instanceof ApplicationError) throw error;
      throw new ApplicationError(503, 'RELEASE_WEB_BUILD_UNAVAILABLE', {
        cause : error,
      });
    } finally {
      clearTimeout(deadline);
    }
  };
}

module.exports = { releaseWebBuild };
