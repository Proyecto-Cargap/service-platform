/** @file Revalida sesión, espacio y pertenencia mediante contratos canónicos sin almacenar tokens de sesión. */
'use strict';
const { ApplicationError } = require('../../shared/application-error');
/**
 * Construye lecturas autorizadas sobre Security, Communication y Customer.
 * @param {Object} messaging Transporte autenticado entre servicios.
 * @returns {Object} Lectura de identidad vigente y elegibilidad de una entrega.
 */
function createPushAccess(messaging) {
  /**
   * Revalida el principal identificado por metadatos del servidor.
   * @param {Object} context Sesión, usuario y espacio que registraron el dispositivo.
   * @returns {Promise<Object|null>} Identidad pública o null cuando dejó de estar autorizada.
   */
  async function currentIdentity(context) {
    const result = await messaging.request(
      'service-security',
      'identity-get-current',
      {},
      { context },
    );
    if ([401, 403, 404].includes(result.statusCode)) return null;
    if (result.statusCode !== 200) throw new ApplicationError(503, 'PUSH_ACCESS_UNAVAILABLE');
    return result.data;
  }
  return {
    currentIdentity,
    /** Reconsulta acceso y vigencia por tipo antes de exponer un aviso genérico.
     * @param {Object} device Registro privado del dispositivo.
     * @param {Object} job Conversación o actividad y generación por entregar.
     * @returns {Promise<boolean>} La sesión y el destinatario conservan acceso a un evento vigente.
     */
    async eligible(device, job) {
      const context  = {
        user        : { userCode: device.userCode },
        sessionCode : device.sessionCode,
        workspace   : device.workspace,
      };
      const identity = await currentIdentity(context);
      if (
        !identity ||
        identity.userCode !== device.userCode ||
        identity.workspace !== device.workspace
      )
        return false;
      context.policies = identity.policies;
      context.workspaces = identity.workspaces;
      if (job.kind === 'ticket') return false;
      if (job.kind === 'customer-activity') {
        const result = await messaging.request('service-customer', 'activity-reminder-get-one', {
          activityCode     : job.activityCode,
          reminderRevision : job.reminderRevision,
        });
        if ([401, 403, 404].includes(result.statusCode)) return false;
        if (result.statusCode !== 200 || !Object.hasOwn(result.data || {}, 'item'))
          throw new ApplicationError(503, 'PUSH_ACCESS_UNAVAILABLE');
        const item = result.data.item;
        if (
          !item ||
          item.userCode !== device.userCode ||
          item.workspace !== device.workspace ||
          item.activityCode !== job.activityCode ||
          item.reminderRevision !== job.reminderRevision
        )
          return false;
        const activity = await messaging.request(
          'service-customer',
          'activity-get-one',
          { activityCode: job.activityCode },
          { context },
        );
        if ([401, 403, 404].includes(activity.statusCode)) return false;
        if (activity.statusCode !== 200) throw new ApplicationError(503, 'PUSH_ACCESS_UNAVAILABLE');
        return (
          activity.data?.reminderRevision === job.reminderRevision &&
          activity.data?.state === 'pending'
        );
      }
      const result = await messaging.request(
        'service-communication',
        'chat-get-one',
        { code: job.conversationCode },
        { context },
      );
      if ([401, 403, 404].includes(result.statusCode)) return false;
      if (result.statusCode !== 200 || typeof result.data?.unread !== 'number')
        throw new ApplicationError(503, 'PUSH_ACCESS_UNAVAILABLE');
      return result.data.unread > 0;
    },
  };
}
module.exports = { createPushAccess };
