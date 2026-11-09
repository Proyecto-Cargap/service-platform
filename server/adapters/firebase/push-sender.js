/** @file Envía avisos genéricos por FCM; las credenciales sólo se resuelven en infraestructura. */
'use strict';
const { createHash } = require('node:crypto');
/**
 * Construye el emisor con un SDK ya configurado por la composición.
 * @param {Object} messaging SDK de Firebase Admin con send.
 * @returns {Object} Capacidad send que no expone respuestas, FIDs ni credenciales en errores.
 */
function createPushSender(messaging) {
  return {
    /** Envía sin incluir cuerpo del chat, nombres ni archivos privados.
     * @param {Object} device Destinatario revalidado con dirección Firebase.
     * @param {Object} job Evento único y contexto acreditado.
     * @returns {Promise<void>} Firebase confirmó aceptación; no equivale a lectura o recepción.
     */
    async send(device, job) {
      const reminder = job.kind === 'customer-activity';
      const ticket   = job.kind === 'ticket';
      const collapse = createHash('sha256')
        .update(
          ticket
            ? `ticket:${job.ticketCode}`
            : reminder
              ? `customer:${job.activityCode}`
              : job.conversationCode,
        )
        .digest('hex');
      try {
        await messaging.send({
          fid          : device.fid,
          notification : {
            title : 'Xidden',
            body  : ticket
              ? `${job.folio}: ${job.eventLabel}`
              : reminder
                ? 'Tienes una actividad pendiente.'
                : 'Tienes un nuevo mensaje.',
          },
          data         : {
            kind      : ticket ? 'ticket' : reminder ? 'customer-activity' : 'chat',
            eventId   : job.eventId,
            bindingId : job.bindingId,
            userCode  : job.userCode,
            workspace : job.workspace,
            ...(ticket
              ? {
                  ticketCode       : job.ticketCode,
                  notificationCode : job.notificationCode,
                }
              : reminder
                ? {
                    activityCode     : job.activityCode,
                    notificationCode : job.notificationCode,
                  }
                : { conversationCode: job.conversationCode }),
          },
          android      : {
            priority     : 'high',
            ttl          : 3600000,
            collapseKey  : collapse,
            notification : {
              channelId  : reminder ? 'xidden_reminders' : 'xidden_messages',
              tag        : collapse,
              visibility : 'private',
              icon       : 'ic_notification',
              color      : '#E5213D',
            },
          },
          apns         : {
            headers : {
              'apns-push-type'   : 'alert',
              'apns-priority'    : '10',
              'apns-expiration'  : String(Math.floor(job.validUntil.getTime() / 1000)),
              'apns-collapse-id' : collapse,
            },
            payload : {
              aps : {
                sound    : 'default',
                threadId : collapse,
              },
            },
          },
        });
      } catch (cause) {
        const error = new Error('PUSH_PROVIDER_FAILED');
        if (
          [
            'messaging/registration-token-not-registered',
            'messaging/invalid-registration-token',
            'messaging/installation-not-registered',
            'messaging/installation-id-not-registered',
            'messaging/invalid-fid',
          ].includes(cause.code)
        ) {
          error.code = 'PUSH_ADDRESS_INVALID';
        }
        error.retryable = ['messaging/quota-exceeded', 'messaging/server-unavailable'].includes(
          cause.code,
        );
        throw error;
      }
    },
  };
}
module.exports = { createPushSender };
