/**
 * @file Controladores de navegación pública e información interna de la plataforma. Adaptan los datos
 * recibidos a las operaciones de aplicación y construyen sus respuestas.
 */
'use strict';
const { ApplicationError }     = require('../../../shared/application-error');
const { configurationService } = require('../services/configuration.service');

/**
 * Construye los handlers de lectura pública y actualización de la configuración.
 *
 * @param {Object} [dependencies] Mensajería y repositorio de la configuración singleton.
 * @returns {Object<string, Function>} Handlers RPC de las operaciones expuestas por el módulo.
 */
function configurationController({ messaging, repository } = {}) {
  const handlers                     = {};
  const configurationServiceInstance = configurationService({ repository });

  /**
   * Construye la respuesta 200 y conserva el estado público de errores del servicio.
   *
   * @param {Function} operation Operación que devuelve la lectura o actualización solicitada.
   * @returns {Promise<Object>} Envelope 200 con el resultado de la operación.
   * @throws {ApplicationError} Conserva estado y descripción; usa 503 N0005-CONFIG-UNAVAILABLE si faltan.
   */
  async function execute(operation) {
    try {
      return messaging.successResponse(200, await operation());
    } catch (error) {
      throw new ApplicationError(
        error.statusCode || 503,
        error.descriptionCode || 'N0005-CONFIG-UNAVAILABLE',
      );
    }
  }

  /**
   * Devuelve la configuración pública mediante la lectura guest explícita del servicio.
   *
   * @returns {Promise<Object>} Envelope 200 con el JSON de navegación pública.
   * @throws {ApplicationError} Conserva el estado del servicio; los fallos sin estado
   * se convierten a 503 N0005-CONFIG-UNAVAILABLE.
   */
  handlers.getOne = () =>
    execute(
      /**
       * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      () => configurationServiceInstance.get(),
    );

  /** Devuelve sólo la información de plataforma usada por los microservicios, sin configuración UI.
   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  handlers.getInformation = () =>
    execute(
      /**
       * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      () => configurationServiceInstance.getInformation(),
    );

  /**
   * Delega el reemplazo de la configuración usando la identidad verificada de mensajería.
   *
   * @param {Object} mqMessage data.configuration contiene el JSON completo; data.scope selecciona
   * el alcance de actualización.
   * @returns {Promise<Object>} Envelope 200 con la configuración devuelta por el servicio.
   * @throws {ApplicationError} 400 por contenido inválido, 403 por denegación; otros fallos
   * conservan su estado o se convierten a 503.
   */
  handlers.update = (mqMessage) =>
    execute(
      /**
       * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      () =>
        configurationServiceInstance.put(
          mqMessage.data.configuration,
          messaging.getContext(),
          mqMessage.data.scope === undefined ? '' : mqMessage.data.scope,
          mqMessage.data.expectedTag,
        ),
    );

  return handlers;
}
module.exports = { configurationController };
