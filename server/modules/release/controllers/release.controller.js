/** @file Adaptación RPC de release; cada operación usa un servicio nuevo y devuelve el envelope público. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');
const ReleaseService       = require('../services/release.service');
const { id }               = require('../../../../package.json');
const controller           = 'C003';
/**
 * Construye los handlers RPC de versiones con una instancia de servicio por operación.
 *
 * @param {Object} [dependencies] Mensajería, tiempo real, logger, identificadores y repositorios de
 * versiones.
 * @returns {Object<string, Function>} Handlers RPC de las operaciones expuestas por el módulo.
 */
function releaseController({
  messaging,
  logger,
  identifiers,
  repository,
  releases,
  verifyWebBuild,
} = {}) {
  /**
   * Compone releaseService con las dependencias recibidas y devuelve su interfaz.
   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  const releaseService = () =>
    new ReleaseService({
      messaging,
      logger,
      identifiers,
      repository,
      releases,
      verifyWebBuild,
    });
  return {
    /** @param {Object} message Solicitud administrativa sin dominio del cliente. @returns {Promise<Object>} Compilación web verificada del entorno. */
    async webBuild(message = {}) {
      return messaging.successResponse(200, await releaseService().webBuild());
    },
    /** @param {Object} message Solicitud autenticada sin filtros. @returns {Promise<Object>} Versión vigente o null. */
    async current(message = {}, snapshot = false) {
      if (!snapshot)
        return repository.snapshot(
          /** @returns {Promise<Object>} Caso canónico en un snapshot. */ () =>
            this.current(message, true),
        );
      return messaging.successResponse(200, await releaseService().current());
    },
    /** @param {Object} message Solicitud autenticada con page y perPage. @returns {Promise<Object>} Página de versiones anteriores. */
    async history(message = {}, snapshot = false) {
      if (!snapshot)
        return repository.snapshot(
          /** @returns {Promise<Object>} Caso canónico en un snapshot. */ () =>
            this.history(message, true),
        );
      const data = message.data === undefined ? {} : message.data;
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      return messaging.successResponse(200, await releaseService().history(data));
    },
    /**
     * Delega el registro de una versión al servicio de publicaciones.
     *
     * @param {Object} [mqMessage={}] data admite releaseDate, current, version, build, description,
     * image, workspace, userAssigned y scope.
     * @returns {Promise<Object>} Envelope 201 con el documento creado.
     * @throws {ApplicationError} 400 si data no es un objeto; propaga la denegación o el fallo del
     * servicio.
     */
    async create(mqMessage = {}) {
      const data = mqMessage.data === undefined ? {} : mqMessage.data;
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      const releaseServiceInstance = releaseService();
      const value                  = await releaseServiceInstance.create(data);
      return messaging.successResponse(201, value);
    },
    /**
     * Obtiene una versión mediante su código de negocio.
     *
     * @param {Object} [mqMessage={}] data.code identifica la versión; data.scope se transmite al
     * servicio.
     * @returns {Promise<Object>} Envelope 200 con el documento, o 204 con la descripción de versión
     * inexistente.
     * @throws {ApplicationError} 400 si data o code no son válidos; propaga los errores del
     * servicio.
     */
    async getOne(mqMessage = {}, snapshot = false) {
      if (!snapshot)
        return repository.snapshot(
          /** @returns {Promise<Object>} Caso canónico en un snapshot. */ () =>
            this.getOne(mqMessage, true),
        );
      const data = mqMessage.data === undefined ? {} : mqMessage.data;
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      if (typeof data.code !== 'string' || !data.code.trim() || data.code.length > 200)
        throw new ApplicationError(400, `${id}-${controller}-E0003:Release code is required`);
      const releaseServiceInstance = releaseService();
      const value                  = await releaseServiceInstance.getOne({ releaseCode: data.code }, data.scope);
      if (value === null)
        return {
          statusCode      : 204,
          descriptionCode : `${id}-C005-E0004:Release does not exist`,
        };
      return messaging.successResponse(200, value);
    },
    /**
     * Delega filtros, orden y paginación de versiones al servicio.
     *
     * @param {Object} [mqMessage={}] data admite fields, sort, order, page, perPage, search,
     * dateRange, condition y scope.
     * @returns {Promise<Object>} Envelope 200 con items y pagination devueltos por el servicio.
     * @throws {ApplicationError} 400 si data no es un objeto; propaga errores de paginación,
     * filtros o consulta.
     */
    async getList(mqMessage = {}, snapshot = false) {
      if (!snapshot)
        return repository.snapshot(
          /** @returns {Promise<Object>} Caso canónico en un snapshot. */ () =>
            this.getList(mqMessage, true),
        );
      const data = mqMessage.data === undefined ? {} : mqMessage.data;
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      const releaseServiceInstance = releaseService();
      const value                  = await releaseServiceInstance.getList(data);
      return messaging.successResponse(200, value);
    },
    /**
     * Selecciona una versión por código y transmite los cambios al servicio.
     *
     * @param {Object} [mqMessage={}] data incluye code y puede incluir releaseDate, version, build,
     * description, image y scope.
     * @returns {Promise<Object>} Envelope 200 sin datos cuando el servicio completa la operación.
     * @throws {ApplicationError} 400 si data o code no son válidos; propaga la denegación o el
     * fallo del servicio.
     */
    async update(mqMessage = {}) {
      const data = mqMessage.data === undefined ? {} : mqMessage.data;
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      if (typeof data.code !== 'string' || !data.code.trim() || data.code.length > 200)
        throw new ApplicationError(400, `${id}-${controller}-E0006:Release code is required`);
      const releaseServiceInstance = releaseService();
      await releaseServiceInstance.update({ releaseCode: data.code }, data);
      return messaging.successResponse(200);
    },
    /**
     * Selecciona la versión por código y delega su eliminación vigente al servicio.
     *
     * @param {Object} [mqMessage={}] data.code identifica la versión; data.scope se transmite al
     * servicio.
     * @returns {Promise<Object>} Envelope 200 sin datos cuando el servicio completa la operación.
     * @throws {ApplicationError} 400 si data o code no son válidos; propaga la denegación o el
     * fallo del servicio.
     */
    async delete(mqMessage = {}) {
      const data = mqMessage.data === undefined ? {} : mqMessage.data;
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      if (typeof data.code !== 'string' || !data.code.trim() || data.code.length > 200)
        throw new ApplicationError(400, `${id}-${controller}-E0006:Release code is required`);
      const releaseServiceInstance = releaseService();
      await releaseServiceInstance.delete({ releaseCode: data.code }, data.scope);
      return messaging.successResponse(200);
    },
    /**
     * Solicita al servicio marcar como vigente la versión seleccionada.
     *
     * @param {Object} [mqMessage={}] data.code identifica la versión; data.expectedBuildId confirma
     * la compilación verificada y data.scope se transmite al servicio.
     * @returns {Promise<Object>} Envelope 200 con el documento, o 204 con la descripción de versión
     * inexistente.
     * @throws {ApplicationError} 400 si data o code no son válidos; propaga la denegación o el
     * fallo del servicio.
     */
    async published(mqMessage = {}) {
      const data = mqMessage.data === undefined ? {} : mqMessage.data;
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      if (typeof data.code !== 'string' || !data.code.trim() || data.code.length > 200)
        throw new ApplicationError(400, `${id}-${controller}-E0003:Release code is required`);
      const releaseServiceInstance = releaseService();
      const value                  = await releaseServiceInstance.published(
        { releaseCode: data.code },
        data.scope,
        data.expectedBuildId,
      );
      if (value === null)
        return {
          statusCode      : 204,
          descriptionCode : `${id}-C005-E0004:Release does not exist`,
        };
      return messaging.successResponse(200, value);
    },
  };
}
module.exports = { releaseController };
