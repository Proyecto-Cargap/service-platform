/** @file DTO cerrados de registro del dispositivo y recepción de eventos mínimos de Chat. */
'use strict';
const { createActionDtos } = require('../../../contracts/action-dto');
module.exports = createActionDtos([
  'push-device-register',
  'communication.message.notification-requested',
]);
