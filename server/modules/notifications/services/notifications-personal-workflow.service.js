/** @file Operaciones de bandeja personal con responsabilidades y colaboradores explícitos. */
'use strict';
/** Compone los casos de uso de bandeja personal sin construir infraestructura.
 * @param {Object} dependencies Capacidades requeridas por estas operaciones.
 * @returns {Object} Operaciones con sus resultados, permisos y errores originales.
 */
function notificationsPersonalService({
  ApplicationError,
  messaging,
  pagination,
  personal,
  repository,
  text,
  visible,
  withSenders,
}) {
  return {
    /** Devuelve la página personal con conteo completo y contenido de texto.
     * @param {Object} data Datos de entrada de la operación.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async personalList(data) {
      const p      = pagination(data);
      const result = await repository.list(await personal(data), p.sort, p.page, p.perPage);
      return {
        ...result,
        items : await withSenders(result.items.map(visible), data.workspace),
      };
    },
    /** Devuelve sólo una notificación propia de un espacio autorizado.
     * @param {Object} data Datos de entrada de la operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async personalOne(data) {
      const item = visible(await repository.one(await personal(data)));
      return (await withSenders([item], data.workspace))[0];
    },
    /** Autoriza la notificación antes de resolver y transferir la foto de su autor inmutable.
     * @param {Object} data Datos de entrada de la operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async personalSenderPicture(data) {
      const size = data.size || '100';
      if (!['original', '050', '100', '200', '300', '500', '1000'].includes(size))
        throw new ApplicationError(400, 'INVALID_IMAGE_SIZE');
      const item = await repository.one(
        await personal({
          ...data,
          code : text(data.code, 100, true),
        }),
      );
      if (!item) throw new ApplicationError(404, 'NOTIFICATION_NOT_FOUND');
      if (typeof item.userCreate !== 'string' || !item.userCreate)
        throw new ApplicationError(404, 'NOTIFICATION_SENDER_PICTURE_NOT_FOUND');
      const response = await messaging.request(
        'service-security',
        'user-picture-read',
        {
          userCode : item.userCreate,
          size,
        },
        { timeoutMs: 310000 },
      );
      if (response.statusCode !== 200)
        throw new ApplicationError(
          response.statusCode || 503,
          response.descriptionCode || 'NOTIFICATION_SENDER_UNAVAILABLE',
        );
      return response.data;
    },
    /** Cuenta todas las no vistas del conjunto personal, independientemente de paginación.
     * @param {Object} data Datos de entrada de la operación.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async personalCount(data) {
      return {
        notifications : await repository.count(
          await personal({
            ...data,
            viewed : false,
          }),
        ),
      };
    },
    /** Marca como vista una notificación propia o todas las no vistas seleccionadas.
     * @param {Object} data Datos de entrada de la operación.
     * @param {*} [many] Valor de many utilizado por esta operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async personalView(data, many = false) {
      const filter = await personal({
        ...data,
        ...(many ? { viewed: false } : {}),
      });
      const result = await repository.update(filter, { viewed: true }, many);
      if (!many && !result) throw new ApplicationError(404, 'NOTIFICATION_NOT_FOUND');
      return many ? { modifiedCount: result.modifiedCount } : visible(result);
    },
    /** Elimina una notificación propia o todas las vistas del conjunto seleccionado.
     * @param {Object} data Datos de entrada de la operación.
     * @param {*} [many] Valor de many utilizado por esta operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async personalDelete(data, many = false) {
      const filter = await personal({
        ...data,
        ...(many ? { viewed: true } : {}),
      });
      const result = await repository.remove(filter, many);
      if (!many && !result) throw new ApplicationError(404, 'NOTIFICATION_NOT_FOUND');
      return many ? { deletedCount: result.deletedCount } : visible(result);
    },
  };
}
module.exports = { notificationsPersonalService };
