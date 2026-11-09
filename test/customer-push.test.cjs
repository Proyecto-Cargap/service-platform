/** @file Recordatorios push: autorización por Customer, privacidad y generaciones vigentes, sin servicios reales. */
'use strict';
const test   = require('node:test');
const assert = require('node:assert/strict');
const {
  createPushRegistration,
} = require('../server/modules/push/services/push-registration.service');
const { createPushAccess } = require('../server/adapters/messaging/push-access');
const { createPushSender } = require('../server/adapters/firebase/push-sender');

test('permite registrar una sesión CRM sin permisos de Chat', /** @returns {Promise<void>} Comprueba una identidad de prueba sin ampliar acceso a conversaciones. */ async () => {
  let registered;
  const registration = createPushRegistration({
    available                                                               : true,
    /** @returns {Date} Reloj ficticio. */ now                              : () => new Date(1000),
    /** @returns {Promise<Object>} Identidad confirmada. */ currentIdentity : async () => ({
      userCode  : 'U1',
      workspace : 'W1',
      policies  : ['customer.activity.view.assigned'],
    }),
    repository                                                              : {
      /** @param {Object} data Instalación capturada. @returns {Promise<void>} Conserva los datos para comprobarlos. */ async register(
        data,
      ) {
        registered = data;
      },
    },
  });
  await registration.register(
    {
      installationId : 'I1',
      bindingId      : 'B1',
      generation     : 1,
      platform       : 'ios',
      fid            : 'fictional',
    },
    {
      user        : { userCode: 'U1' },
      sessionCode : 'S1',
    },
  );
  assert.equal(registered.userCode, 'U1');
  assert.equal(registered.workspace, 'W1');
});

test('revalida destinatario, generación y lectura actual antes de enviar un recordatorio', /** @returns {Promise<void>} Completar, reasignar o revocar impide la entrega. */ async () => {
  let valid   = true,
    recipient = 'U1',
    allowed   = true;
  const calls  = [];
  const access = createPushAccess({
    /** @param {string} service Dominio. @param {string} action Consulta canónica. @returns {Promise<Object>} Respuesta acreditada de prueba. */
    async request(service, action) {
      calls.push([service, action]);
      if (action === 'identity-get-current')
        return {
          statusCode : 200,
          data       : {
            userCode   : 'U1',
            workspace  : 'W1',
            policies   : ['customer.activity.view.assigned'],
            workspaces : [],
          },
        };
      if (action === 'activity-reminder-get-one')
        return {
          statusCode : 200,
          data       : {
            item : valid
              ? {
                  activityCode     : 'A1',
                  reminderRevision : 2,
                  userCode         : recipient,
                  workspace        : 'W1',
                }
              : null,
          },
        };
      return allowed
        ? {
            statusCode : 200,
            data       : {
              reminderRevision : 2,
              state            : 'pending',
            },
          }
        : { statusCode: 403 };
    },
  });
  const device = {
    userCode    : 'U1',
    workspace   : 'W1',
    sessionCode : 'S1',
  };
  const job    = {
    kind             : 'customer-activity',
    activityCode     : 'A1',
    reminderRevision : 2,
  };
  assert.equal(await access.eligible(device, job), true);
  assert.ok(
    calls.every(
      /** @param {Array} call Consulta capturada. @returns {boolean} No depende de Communication. */ (
        call,
      ) => call[0] !== 'service-communication',
    ),
  );
  valid = false;
  assert.equal(await access.eligible(device, job), false);
  valid = true;
  recipient = 'U2';
  assert.equal(await access.eligible(device, job), false);
  recipient = 'U1';
  allowed = false;
  assert.equal(await access.eligible(device, job), false);
});

test('FCM separa canal y destino sin incluir datos comerciales', /** @returns {Promise<void>} El aviso genérico usa identidades para deduplicación y navegación. */ async () => {
  let payload;
  const sender = createPushSender({
    /** @param {Object} value Carga del proveedor. @returns {Promise<string>} Aceptación ficticia. */ async send(
      value,
    ) {
      payload = value;
      return 'accepted';
    },
  });
  await sender.send(
    { fid: 'fictional' },
    {
      kind             : 'customer-activity',
      activityCode     : 'A1',
      notificationCode : 'N1',
      eventId          : 'N1',
      bindingId        : 'B1',
      userCode         : 'U1',
      workspace        : 'W1',
      validUntil       : new Date(100000),
    },
  );
  assert.equal(payload.android.notification.channelId, 'xidden_reminders');
  assert.equal(payload.notification.body, 'Tienes una actividad pendiente.');
  assert.deepEqual(payload.data, {
    kind             : 'customer-activity',
    eventId          : 'N1',
    bindingId        : 'B1',
    userCode         : 'U1',
    workspace        : 'W1',
    activityCode     : 'A1',
    notificationCode : 'N1',
  });
  assert.equal(payload.data.conversationCode, undefined);
});
