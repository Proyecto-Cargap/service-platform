/**
 * @file Controladores de mensajería de parámetros de configuración. Adaptan los datos recibidos a
 * las
 * operaciones de aplicación y construyen sus respuestas.
 */
'use strict';

const { ApplicationError } = require('../../../shared/application-error');
const { id }               = require('../../../../package.json');

const controller = 'C000';

/**
 * Construye los handlers técnicos de Settings usando el repositorio inyectado.
 *
 * @param {Object} [dependencies] Mensajería y repositorio de parámetros de configuración.
 * @returns {Object<string, Function>} Handlers RPC de las operaciones expuestas por el módulo.
 */
function settingsController({ messaging, repository: settingsRepository } = {}) {
  const handlers = {};
  /**
   * Consulta el parámetro indicado y conserva el selector de acceso recibido.
   *
   * @param {Object} mqMessage data.parameter identifica el parámetro; data.scope se transmite al
   * repositorio.
   * @returns {Promise<Object>} Envelope 200 con el parámetro encontrado.
   * @throws {ApplicationError} 400 si falta parameter o no existe el parámetro; conserva
   * ApplicationError y convierte otros fallos a 500.
   */
  handlers.getOne = async (mqMessage) => {
    try {
      if (mqMessage.data.parameter == undefined) {
        throw new ApplicationError(400, `${id}-${controller}-E0001:Parameter is required`);
      }

      const settings = await settingsRepository.get(
        mqMessage.data.parameter,
        mqMessage?.data?.scope === undefined ? '' : mqMessage.data.scope,
      );
      if (!settings) {
        throw new ApplicationError(400, `${id}-${controller}-E0002:Settings does not exist`);
      }

      return messaging.successResponse(200, settings);
    } catch (err) {
      if (err instanceof ApplicationError) throw err;
      throw new ApplicationError(500, 'INTERNAL_ERROR', { cause: err });
    }
  };

  /**
   * Consulta todos los parámetros elegibles sin aplicar paginación.
   *
   * @param {Object} [mqMessage] data.scope es opcional y se transmite al repositorio; sin mensaje
   * usa selector vacío.
   * @returns {Promise<Object>} Envelope 200 con la lista completa devuelta por el repositorio.
   * @throws {ApplicationError} Conserva ApplicationError del repositorio y convierte otros fallos a
   * 500 INTERNAL_ERROR.
   */
  handlers.getList = async (mqMessage) => {
    try {
      require('../../../shared/general').getPublicPagination(mqMessage?.data || {});
      const settings = await settingsRepository.getAll(
        mqMessage?.data?.scope === undefined ? '' : mqMessage.data.scope,
      );

      return messaging.successResponse(200, settings);
    } catch (err) {
      if (err instanceof ApplicationError) throw err;
      throw new ApplicationError(500, 'INTERNAL_ERROR', { cause: err });
    }
  };

  /**
   * Actualiza el contenido del parámetro indicado mediante el repositorio.
   *
   * @param {Object} mqMessage data contiene parameter y data como contenido; scope se transmite al
   * repositorio.
   * @returns {Promise<Object>} Envelope 200 sin datos cuando se actualiza un parámetro existente.
   * @throws {ApplicationError} 400 si falta parameter/contenido o no existe el parámetro; conserva
   * ApplicationError y convierte otros fallos a 500.
   */
  handlers.update = async (mqMessage) => {
    try {
      if (!mqMessage.data.parameter) {
        throw new ApplicationError(400, `${id}-${controller}-E0001:Parameter is required`);
      }

      if (!mqMessage.data.data) {
        throw new ApplicationError(400, `${id}-${controller}-E0003:Data is required`);
      }

      if (
        !(await settingsRepository.update(
          mqMessage.data.parameter,
          mqMessage.data.data,
          mqMessage?.data?.scope === undefined ? '' : mqMessage.data.scope,
        ))
      ) {
        throw new ApplicationError(400, `${id}-${controller}-E0002:Settings does not exist`);
      }

      return messaging.successResponse(200);
    } catch (err) {
      if (err instanceof ApplicationError) throw err;
      throw new ApplicationError(500, 'INTERNAL_ERROR', { cause: err });
    }
  };

  return handlers;
}
module.exports = { settingsController };
