/** @file Conecta persistencia, Firebase Admin y contratos de aplicación de notificaciones móviles. */
'use strict';
const { randomUUID } = require('node:crypto');
/**
 * Construye adaptadores explícitos sin iniciar el trabajador ni enviar mensajes.
 * @param {Object} dependencies Mensajería, logger y entorno exclusivo de composición.
 * @returns {Object} Controlador y trabajador con recursos compartidos de la aplicación.
 */
function createPush({ messaging, logger, env = process.env }) {
  const models       = require('../models/push.model').createPushModels(require('mongoose'));
  /** Reloj inyectado; evita lecturas de entorno o SDK en los casos de uso.
   * @returns {Date} Instante actual.
   */
  const now          = () => new Date();
  const repository   = require('../repositories/push.repository').createPushRepository({
    ...models,
    now,
    newId : randomUUID,
  });
  const access       = require('../../../adapters/messaging/push-access').createPushAccess(messaging);
  const available    = Boolean(env.PLATFORM_FIREBASE_PROJECT_ID);
  const registration = require('../services/push-registration.service').createPushRegistration({
    repository,
    currentIdentity : access.currentIdentity,
    now,
    available,
  });
  let sender;
  if (available) {
    const firebase = require('firebase-admin/app');
    const app      =
      firebase.getApps().find(
        /** Reutiliza exclusivamente la instancia nombrada de envío del proceso.
         * @param {Object} value Aplicación de Firebase Admin.
         * @returns {boolean} Coincide con el emisor propio de Platform.
         */
        (value) => value.name === 'xidden-push',
      ) ||
      firebase.initializeApp(
        {
          projectId  : env.PLATFORM_FIREBASE_PROJECT_ID,
          credential : env.PLATFORM_FIREBASE_CREDENTIALS_FILE
            ? firebase.cert(
                JSON.parse(
                  require('node:fs').readFileSync(env.PLATFORM_FIREBASE_CREDENTIALS_FILE, 'utf8'),
                ),
              )
            : firebase.applicationDefault(),
        },
        'xidden-push',
      );
    sender = require('../../../adapters/firebase/push-sender').createPushSender(
      require('firebase-admin/messaging').getMessaging(app),
    );
  }
  const delivery  = require('../services/push-delivery.service').createPushDelivery({
    repository,
    eligible : access.eligible,
    sender,
    now,
  });
  const reminders =
    require('../repositories/reminder-push.repository').createReminderPushRepository({
      ...models,
      messaging,
      notifications : require('../../notifications/models/notifications.model'),
      now,
      ready         : repository.ready,
    });
  return {
    reminders,
    controller : require('../controllers/push.controller').createPushController({
      messaging,
      registration,
      repository,
    }),
    worker     : require('../../../library/work/reactive-worker').createReactiveWorker({
      source    : require('../../../library/work/mongo-changes').createMongoChanges({
        connection  : models.deliveries.db,
        collections : ['PLATFORM_PUSH_DELIVERY'],
        logger,
        component   : 'push',
      }),
      nextAt    : repository.nextAt,
      tick      : delivery.tick,
      logger,
      enabled   : available,
      component : 'push',
    }),
  };
}
module.exports = { createPush };
