/** @file Adaptación RPC de instance; cada operación usa un servicio nuevo y devuelve el envelope público. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');
const InstanceService      = require('../services/instance.service');
const { id }               = require('../../../../package.json');
const controller           = 'C003';
/**
 * Construye los handlers RPC de instancias con una instancia de servicio por operación.
 *
 * @param {Object} [dependencies] Mensajería, tiempo real, logger, identificadores y repositorios de
 * instancias/versiones.
 * @returns {Object<string, Function>} Handlers RPC de las operaciones expuestas por el módulo.
 */
function instanceController({
  messaging,

  logger,
  identifiers,
  repository,
  releases,
} = {}) {
  /**
   * Compone instanceService con las dependencias recibidas y devuelve su interfaz.
   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  const instanceService = () =>
    new InstanceService({
      messaging,

      logger,
      identifiers,
      repository,
      releases,
    });
  return {
    /**
     * Registra una instancia o actualiza su registro existente mediante el servicio.
     *
     * @param {Object} [mqMessage={}] data incluye instanceCode, version, build, code, workspace,
     * userAssigned y scope.
     * @returns {Promise<Object>} Envelope 201 con el documento registrado.
     * @throws {ApplicationError} 400 si data no es un objeto; propaga la denegación o el fallo del
     * servicio.
     */
    async create(mqMessage = {}) {
      const data = mqMessage.data === undefined ? {} : mqMessage.data;
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      const instanceServiceInstance = instanceService();
      const value                   = await instanceServiceInstance.create(data);
      return messaging.successResponse(201, value);
    },
    /**
     * Obtiene una instancia por su código de negocio y conserva el selector solicitado.
     *
     * @param {Object} [mqMessage={}] data.code identifica la instancia; data.scope se transmite al
     * servicio.
     * @returns {Promise<Object>} Envelope 200 con el documento, o 204 con la descripción de
     * instancia inexistente.
     * @throws {ApplicationError} 400 si data o code no son válidos; propaga los errores del
     * servicio.
     */
    async getOne(mqMessage = {}) {
      const data = mqMessage.data === undefined ? {} : mqMessage.data;
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      if (typeof data.code !== 'string' || !data.code.trim() || data.code.length > 200)
        throw new ApplicationError(400, `${id}-${controller}-E0003:Instance code is required`);
      const instanceServiceInstance = instanceService();
      const value                   = await instanceServiceInstance.getOne({ instanceCode: data.code }, data.scope);
      if (value === null)
        return {
          statusCode      : 204,
          descriptionCode : `${id}-C005-E0004:Instance does not exist`,
        };
      return messaging.successResponse(200, value);
    },
    /**
     * Delega filtros y paginación de instancias al servicio.
     *
     * @param {Object} [mqMessage={}] data admite fields, sort, order, page, perPage, dateRange,
     * condition, version, build, sessionCode, instanceCode y scope.
     * @returns {Promise<Object>} Envelope 200 con items y pagination devueltos por el servicio.
     * @throws {ApplicationError} 400 si data no es un objeto; propaga errores de paginación,
     * filtros o consulta.
     */
    async getList(mqMessage = {}) {
      const data = mqMessage.data === undefined ? {} : mqMessage.data;
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      const instanceServiceInstance = instanceService();
      const value                   = await instanceServiceInstance.getList(data);
      return messaging.successResponse(200, value);
    },
    /**
     * Selecciona la instancia por código y delega la actualización de sesión.
     *
     * @param {Object} [mqMessage={}] data.code identifica la instancia; sessionCode y scope se
     * transmiten con data.
     * @returns {Promise<Object>} Envelope 200 sin datos cuando el servicio completa la operación.
     * @throws {ApplicationError} 400 si data o code no son válidos; propaga la denegación o el
     * fallo del servicio.
     */
    async update(mqMessage = {}) {
      const data = mqMessage.data === undefined ? {} : mqMessage.data;
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      if (typeof data.code !== 'string' || !data.code.trim() || data.code.length > 200)
        throw new ApplicationError(400, `${id}-${controller}-E0006:Instance code is required`);
      const instanceServiceInstance = instanceService();
      await instanceServiceInstance.update({ instanceCode: data.code }, data);
      return messaging.successResponse(200);
    },
    /**
     * Selecciona la instancia por código y delega su eliminación vigente al servicio.
     *
     * @param {Object} [mqMessage={}] data.code identifica la instancia; data.scope se transmite sin
     * normalizar valores explícitos.
     * @returns {Promise<Object>} Envelope 200 sin datos cuando el servicio completa la operación.
     * @throws {ApplicationError} 400 si data o code no son válidos; propaga la denegación o el
     * fallo del servicio.
     */
    async delete(mqMessage = {}) {
      const data = mqMessage.data === undefined ? {} : mqMessage.data;
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new ApplicationError(400, 'INVALID_INPUT');
      if (typeof data.code !== 'string' || !data.code.trim() || data.code.length > 200)
        throw new ApplicationError(400, `${id}-${controller}-E0006:Instance code is required`);
      const instanceServiceInstance = instanceService();
      await instanceServiceInstance.delete({ instanceCode: data.code }, data.scope);
      return messaging.successResponse(200);
    },
  };
}
module.exports = { instanceController };
