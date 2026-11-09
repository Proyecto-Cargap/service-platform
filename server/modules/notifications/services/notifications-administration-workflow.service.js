/** @file Operaciones de administración de notificaciones con responsabilidades y colaboradores explícitos. */
'use strict';
/** Compone los casos de uso de administración de notificaciones sin construir infraestructura.
 * @param {Object} dependencies Capacidades requeridas por estas operaciones.
 * @returns {Object} Operaciones con sus resultados, permisos y errores originales.
 */
function notificationsAdministrationService({
  administrative,
  jobAccess,
  pagination,
  repository,
  visible,
  withRecipients,
}) {
  return {
    /** Lista registros administrables con permisos exactos de consulta.
     * @param {Object} data Datos de entrada de la operación.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async adminList(data) {
      const p      = pagination(data);
      let selected = data;
      let extra    = {};
      if (data.broadcastCode !== undefined) {
        const job = await jobAccess('view', {
          ...data,
          code : data.broadcastCode,
        });
        selected = {
          ...data,
          workspace : job.workspace,
        };
        extra = { broadcastCode: job.broadcastCode };
      }
      const { filter } = await administrative('view', selected, extra);
      const result     = await repository.list(filter, p.sort, p.page, p.perPage);
      return {
        ...result,
        items : await withRecipients(result.items.map(visible)),
      };
    },
    /** Consulta un registro administrativo autorizado.
     * @param {Object} data Datos de entrada de la operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async adminOne(data) {
      return visible(await repository.one((await administrative('view', data)).filter));
    },
    /** Elimina un registro con permiso independiente de eliminación.
     * @param {Object} data Datos de entrada de la operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async adminDelete(data) {
      const item = visible(await repository.remove((await administrative('delete', data)).filter));
      return item;
    },
  };
}
module.exports = { notificationsAdministrationService };
