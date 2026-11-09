/** @file Persiste un recordatorio y sus entregas en una sola transacción; reentregas recuperan las mismas identidades. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');

/** Compone persistencia atómica sin ejecutar envíos externos dentro de la transacción.
 * @param {Object} dependencies Modelos de notificaciones, dispositivos, entregas, reloj y comprobación de índices.
 * @returns {Object} Puerto persist para el evento acreditado de Customer.
 */
function createReminderPushRepository({
  notifications,
  devices,
  deliveries,
  now,
  ready,
  messaging,
}) {
  const changes = require('../../../adapters/mongodb/platform-changes').platformChanges({
    messaging,
    connection : notifications.db,
  });
  return {
    /** Guarda el aviso y la cola antes de confirmar el evento durable de Customer.
     * @param {Object} input Notificación personal con identidad estable por generación.
     * @param {Object} reminder Actividad, revisión y destinatario revalidados.
     * @returns {Promise<Object>} Referencia canónica que también permite deduplicar socket y push.
     */
    async persist(input, reminder) {
      await ready();
      const manifest = require('../../../mongodb-indexes.json').collections;
      await require('../../../library/messaging/indexes').verifyIndexes(notifications.db.db, {
        CONNECT_NOTIFICATIONS : manifest.CONNECT_NOTIFICATIONS,
      });
      return notifications.db.transaction(
        /** Reintenta únicamente la transacción local, sin efectos en Firebase.
         * @param {Object} session Sesión MongoDB del mismo servicio.
         * @returns {Promise<Object>} Identidad confirmada después del commit.
         */
        async (session) => {
          const existing = await notifications
            .findOne({
              sourceEventKey : input.sourceEventKey,
              workspace      : input.workspace,
              userAssigned   : input.userAssigned,
            })
            .session(session)
            .lean();
          const saved    =
            existing ||
            (await notifications
              .findOneAndUpdate(
                {
                  sourceEventKey : input.sourceEventKey,
                  workspace      : input.workspace,
                  userAssigned   : input.userAssigned,
                },
                { $setOnInsert: input },
                {
                  session,
                  upsert         : true,
                  returnDocument : 'after',
                  runValidators  : true,
                },
              )
              .lean());
          if (!saved) throw new ApplicationError(503, 'NOTIFICATION_PERSISTENCE_UNAVAILABLE');
          if (!existing && !saved.erased)
            await changes.emit('notification', saved, session, { created: true });
          const date       = now();
          const validUntil = new Date(saved.creationDate + 3600000);
          if ((reminder.kind === 'ticket' ? reminder.push : !saved.erased) && validUntil > date) {
            const cursor = devices
              .find({
                userCode  : reminder.userCode,
                workspace : reminder.workspace,
                expiresAt : { $gt: date },
              })
              .session(session)
              .lean()
              .cursor({ batchSize: 100 });
            for await (const device of cursor) {
              await deliveries.updateOne(
                { _id: `${input.sourceEventKey}:${device._id}` },
                {
                  $setOnInsert : {
                    kind             : reminder.kind || 'customer-activity',
                    ...(reminder.kind === 'ticket'
                      ? {
                          ticketCode : reminder.ticketCode,
                          folio      : reminder.folio,
                          eventLabel : reminder.eventLabel,
                          internal   : reminder.internal,
                        }
                      : {}),
                    installationId   : device._id,
                    bindingId        : device.bindingId,
                    eventId          : saved.notificationCode,
                    notificationCode : saved.notificationCode,
                    activityCode     : reminder.activityCode,
                    reminderRevision : reminder.reminderRevision,
                    userCode         : reminder.userCode,
                    workspace        : reminder.workspace,
                    state            : 'pending',
                    attempts         : 0,
                    nextAt           : date,
                    validUntil,
                    expiresAt        : new Date(date.getTime() + 7 * 86400000),
                  },
                },
                {
                  session,
                  upsert        : true,
                  runValidators : true,
                },
              );
            }
          }
          return {
            notificationCode : saved.notificationCode,
            workspace        : saved.workspace,
          };
        },
      );
    },
  };
}
module.exports = { createReminderPushRepository };
