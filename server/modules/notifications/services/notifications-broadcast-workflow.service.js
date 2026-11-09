/** @file Operaciones de difusión de notificaciones con responsabilidades y colaboradores explícitos. */
'use strict';
/** Compone los casos de uso de difusión de notificaciones sin construir infraestructura.
 * @param {Object} dependencies Capacidades requeridas por estas operaciones.
 * @returns {Object} Operaciones con sus resultados, permisos y errores originales.
 */
function notificationsBroadcastService({
  ApplicationError,
  actor,
  administrative,
  audience,
  content,
  jobAccess,
  pagination,
  randomUUID,
  repository,
  text,
  visible,
}) {
  return {
    /** Prepara texto y audiencia completa; todavía no crea notificaciones.
     * @param {Object} data Datos de entrada de la operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async preview(data) {
      const { identity } = await administrative('create', data);
      const resolved     = await audience(data.workspace || identity.workspace);
      return visible(
        await repository.prepare({
          ...content(data),
          broadcastCode  : 'BRC-' + randomUUID(),
          workspace      : resolved.workspace,
          userCreate     : identity.userCode,
          userAssigned   : '',
          audience       : resolved.userCodes,
          audienceHash   : resolved.hash,
          recipientCount : resolved.userCodes.length,
        }),
      );
    },
    /** Acepta un trabajo o exige nueva confirmación si cambiaron sus destinatarios.
     * @param {Object} data Datos de entrada de la operación.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async confirm(data) {
      const job = await jobAccess('create', data);
      if (job.state !== 'prepared')
        return {
          item            : visible(job),
          audienceChanged : false,
        };
      const confirmationToken = text(data.confirmationToken, 100, true);
      const resolved          = await audience(job.workspace);
      const result            = await repository.confirm(
        job.broadcastCode,
        resolved.userCodes,
        resolved.hash,
        confirmationToken,
      );
      return {
        item            : visible(result.item),
        audienceChanged : result.audienceChanged,
      };
    },
    /** Lista trabajos administrables con consulta paginada.
     * @param {Object} data Datos de entrada de la operación.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async broadcasts(data) {
      const { identity, scope } = await administrative('view', data);
      const p                   = pagination(data);
      const condition           = scope === 'workspace' ? { workspace: identity.workspace } : {};
      if (data.workspace && data.workspace !== 'allWorkspaces') {
        if (scope === 'workspace' && data.workspace !== identity.workspace)
          throw new ApplicationError(403, 'FORBIDDEN');
        condition.workspace = text(data.workspace, 100, true);
      }
      const result = await repository.jobs(condition, p.page, p.perPage);
      return {
        ...result,
        items : result.items.map(visible),
      };
    },
    /** Devuelve progreso persistido con permiso exacto de consulta.
     * @param {Object} data Datos de entrada de la operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async status(data) {
      try {
        return visible(await jobAccess('view', data));
      } catch (error) {
        if (error.statusCode !== 403) throw error;
        const job = await jobAccess('create', data);
        if (job.userCreate !== (await actor()).userCode)
          throw new ApplicationError(403, 'FORBIDDEN');
        return visible(job);
      }
    },
    /** Reintenta únicamente destinatarios fallidos y preserva el ledger de los persistidos.
     * @param {Object} data Datos de entrada de la operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async retry(data) {
      const job = await jobAccess('create', data);
      return visible(await repository.retry(job.broadcastCode));
    },
  };
}
module.exports = { notificationsBroadcastService };
