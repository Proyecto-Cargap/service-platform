/** @file Composición explícita de modelos, persistencia, casos de uso y worker del centro. */
'use strict';
const createNotificationsCenterRepository = require('../repositories/notifications-center.repository');
const { notificationsCenterController }   = require('../controllers/notifications-center.controller');
const { createNotificationsWorker }       = require('../workers/notifications-worker');
/**
 * Compone dependencias locales sin abrir conexiones ni iniciar el worker.
 * @param {Object} dependencies Mensajería y logger.
 * @returns {Object} Controlador y worker listos para arrancar por la aplicación.
 */
function createNotificationsCenter(dependencies) {
  const repository = createNotificationsCenterRepository({
    ...dependencies,
    notifications : require('../models/notifications.model'),
    broadcasts    : require('../models/notifications-broadcast.model'),
    recipients    : require('../models/notifications-recipient.model'),
  });
  return {
    controller : notificationsCenterController({
      ...dependencies,
      repository,
    }),
    worker     : createNotificationsWorker({
      ...dependencies,
      repository,
      changes : require('../../../library/work/mongo-changes').createMongoChanges({
        connection  : require('mongoose').connection,
        collections : ['PLATFORM_NOTIFICATION_BROADCAST', 'PLATFORM_NOTIFICATION_RECIPIENT'],
        logger      : dependencies.logger,
        component   : 'notifications',
      }),
    }),
  };
}
module.exports = { createNotificationsCenter };
