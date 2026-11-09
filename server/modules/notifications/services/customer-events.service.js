/** @file Eventos de Customer: recordatorios revalidados con persistencia idempotente. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');

/** Compone la recepción de eventos sin abrir transportes ni construir modelos.
 * @param {Object} dependencies Mensajería, identificadores y persistencia de notificaciones.
 * @returns {Object} Manejadores de cambios y recordatorios.
 */
function customerEvents({ messaging, identifiers, reminders }) {
  return {
    /** Reconsulta vigencia y permisos; una reentrega recupera la misma notificación persistida.
     * @param {Object} message Identidad de actividad y generación del recordatorio.
     * @returns {Promise<Object|null>} Referencia creada o null cuando la entrega perdió vigencia.
     */
    async reminder(message) {
      if (message.originService !== 'service-customer')
        throw new ApplicationError(403, 'FORBIDDEN');
      const response = await messaging.request(
        'service-customer',
        'activity-reminder-get-one',
        message.data,
      );
      if (response?.statusCode !== 200 || !Object.hasOwn(response.data || {}, 'item'))
        throw new ApplicationError(503, 'CUSTOMER_REMINDER_UNAVAILABLE');
      const item = response.data.item;
      if (!item) return null;
      const reference = await reminders.persist(
        {
          notificationCode : await identifiers.getObjectCode('NOT'),
          sourceEventKey   : `customer.activity:${item.activityCode}:${item.reminderRevision}`,
          workspace        : item.workspace,
          userCreate       : '',
          userAssigned     : item.userCode,
          userCode         : item.userCode,
          title            : 'Actividad pendiente',
          content          : item.title,
          type             : 'info',
          action           : 'open',
          idObject         : item.activityCode,
          from             : '',
          link             : {
            route      : '/app/customer/activity/detail/' + encodeURIComponent(item.activityCode),
            parameters : [],
          },
          erased           : false,
          creationDate     : Date.now(),
          updateDate       : Date.now(),
        },
        item,
      );
      return reference;
    },
  };
}
module.exports = { customerEvents };
