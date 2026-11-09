/** @file Entrega de recordatorios Customer: revalidación por intento, identidad idempotente y rechazo de origen no acreditado. */
'use strict';
const test   = require('node:test');
const assert = require('node:assert/strict');
const {
  customerEvents,
} = require('../server/modules/notifications/services/customer-events.service');

test('recordatorios revalidan vigencia y conservan una referencia por actividad, generación y destinatario' /** @returns {Promise<void>} Comprueba recuperación tras reentrega, revocación e indisponibilidad sin falso éxito. */, async () => {
  let eligible  = true,
    unavailable = false,
    sequence    = 0;
  const rows = new Map(),
    notices  = [];
  const handler = customerEvents({
    messaging   : {
      /** @param {string} service Productor. @param {string} action Consulta canónica. @param {Object} input Actividad y generación. @returns {Promise<Object>} Autorización vigente simulada. */
      async request(service, action, input) {
        assert.equal(service, 'service-customer');
        assert.equal(action, 'activity-reminder-get-one');
        return unavailable
          ? { statusCode: 503 }
          : {
              statusCode : 200,
              data       : {
                item : eligible
                  ? {
                      ...input,
                      workspace    : 'workspace-a',
                      userCode     : 'alice',
                      title        : 'Llamar',
                      customerCode : 'customer-a',
                    }
                  : null,
              },
            };
      },
    },
    realtime    : {
      /** @param {Object} target Destinatario. @param {string} channel Canal. @param {Object} data Invalidación. @returns {number} Publicación registrada. */
      publish(target, channel, data) {
        notices.push({
          target,
          channel,
          data,
        });
        return 1;
      },
    },
    identifiers : {
      /** @returns {Promise<string>} Código de aviso nuevo sólo para una inserción inicial. */
      async getObjectCode() {
        return 'notification-' + ++sequence;
      },
    },
    reminders   : {
      /** @param {Object} input Aviso público. @returns {Promise<Object>} Emula el puerto con clave única durable. */
      async persist(input) {
        const key = [input.sourceEventKey, input.workspace, input.userAssigned].join('|');
        if (!rows.has(key))
          rows.set(key, {
            notificationCode : input.notificationCode,
            workspace        : input.workspace,
          });
        return rows.get(key);
      },
    },
  });
  const message = {
    originService : 'service-customer',
    data          : {
      activityCode     : 'activity-a',
      reminderRevision : 2,
    },
  };
  const first   = await handler.reminder(message);
  assert.deepEqual(await handler.reminder(message), first);
  assert.equal(rows.size, 1);
  assert.equal(notices.length, 0);
  eligible = false;
  assert.equal(await handler.reminder(message), null);
  assert.equal(rows.size, 1);
  unavailable = true;
  await assert.rejects(handler.reminder(message), { statusCode: 503 });
  await assert.rejects(
    handler.reminder({
      ...message,
      originService : 'service-directory',
    }),
    { statusCode: 403 },
  );
});
