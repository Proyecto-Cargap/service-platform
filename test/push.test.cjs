/** @file Pruebas de privacidad, revisión, pertenencia y resultados inciertos de mensajería push. */
'use strict';
const test                   = require('node:test');
const assert                 = require('node:assert/strict');
const { createPushDelivery } = require('../server/modules/push/services/push-delivery.service');
const {
  createPushRegistration,
} = require('../server/modules/push/services/push-registration.service');
const { createPushSender } = require('../server/adapters/firebase/push-sender');
const { createPushAccess } = require('../server/adapters/messaging/push-access');
const dtos                 = require('../server/modules/push/dtos/push.actions.dto');
const date                 = new Date('2026-10-27T12:00:00.000Z');
const device               = {
  fid         : 'fictional-installation-address',
  userCode    : 'U1',
  workspace   : 'W1',
  sessionCode : 'S1',
};
const job                  = {
  eventId          : 'M1',
  conversationCode : 'C1',
  bindingId        : 'B1',
  userCode         : 'U1',
  workspace        : 'W1',
  attempts         : 1,
  validUntil       : new Date(date.getTime() + 60000),
};
/** Construye colaboradores verificables sin red ni datos reales.
 * @param {Object} options Variaciones del proveedor y de la autorización.
 * @returns {Object} Caso de uso y efectos observados.
 */
function fixture(options = {}) {
  const results = [];
  let sends       = 0,
    reads         = 0,
    invalidations = 0;
  const repository = {
    /** @returns {Promise<Object>} Reclama una entrega ficticia. */
    async claim() {
      return job;
    },
    /** @returns {Promise<Object|null>} Puede simular revocación entre lecturas. */
    async device() {
      reads++;
      return options.replaced && reads > 1 ? null : device;
    },
    /** @returns {Promise<boolean>} Permite comprobar pérdida del lease. */
    async beginSending() {
      return !options.leaseLost;
    },
    /** @param {Object} value Trabajo. @param {string} state Resultado. @param {string} code Código seguro. @returns {Promise<void>} Conserva sólo evidencia. */
    async finish(value, state, code) {
      results.push({
        state,
        code,
      });
    },
    /** @returns {Promise<void>} Registra retiro del destinatario inválido. */
    async invalidate() {
      invalidations++;
    },
  };
  const delivery   = createPushDelivery({
    repository,
    /** @returns {Date} Reloj determinista. */ now : () => date,
    /** @returns {Promise<boolean>} Autorización vigente o incidencia recuperable. */
    async eligible() {
      if (options.accessError) throw new Error('offline');
      return options.allowed !== false;
    },
    sender                                         : {
      /** @returns {Promise<void>} Emula aceptación, rechazo o respuesta perdida. */
      async send() {
        sends++;
        if (options.error) throw options.error;
      },
    },
  });
  return {
    delivery,
    results,
    /** @returns {Object} Efectos sin exponer direcciones. */ counts : () => ({
      sends,
      invalidations,
    }),
  };
}
test('entrega únicamente con acceso y lease vigentes', /** @returns {Promise<void>} Comprueba envío y resultado. */ async () => {
  const f = fixture();
  await f.delivery.tick();
  assert.equal(f.counts().sends, 1);
  assert.equal(f.results[0].state, 'sent');
  for (const options of [{ allowed: false }, { replaced: true }, { leaseLost: true }]) {
    const denied = fixture(options);
    await denied.delivery.tick();
    assert.equal(denied.counts().sends, 0);
  }
});
test('respuesta perdida queda incierta; errores inequívocos y preflight admiten reintento', /** @returns {Promise<void>} Evita duplicar efectos externos. */ async () => {
  for (const [options, expected] of [
    [{ error: new Error('response lost') }, 'uncertain'],
    [{ error: Object.assign(new Error('rejected'), { retryable: true }) }, 'pending'],
    [{ accessError: true }, 'pending'],
    [{ error: Object.assign(new Error('invalid'), { code: 'PUSH_ADDRESS_INVALID' }) }, 'discarded'],
  ]) {
    const f = fixture(options);
    await f.delivery.tick();
    assert.equal(f.results[0].state, expected);
    assert.equal(f.counts().invalidations, expected === 'discarded' ? 1 : 0);
  }
});
test('FCM recibe sólo aviso genérico, destino FID y navegación vinculada', /** @returns {Promise<void>} Comprueba privacidad y canal nativo. */ async () => {
  let message;
  const sender = createPushSender({
    /** @param {Object} value Carga saliente. @returns {Promise<void>} Captura sin red. */ async send(
      value,
    ) {
      message = value;
    },
  });
  await sender.send(device, job);
  assert.equal(message.fid, device.fid);
  assert.deepEqual(message.notification, {
    title : 'Xidden',
    body  : 'Tienes un nuevo mensaje.',
  });
  assert.deepEqual(Object.keys(message.data).sort(), [
    'bindingId',
    'conversationCode',
    'eventId',
    'kind',
    'userCode',
    'workspace',
  ]);
  assert.equal(message.android.notification.channelId, 'xidden_messages');
  assert.equal(message.android.notification.visibility, 'private');
  assert.equal(message.apns.headers['apns-collapse-id'].length, 64);
});
test('errores de Firebase se redactan y clasifican sin conservar detalles del SDK', /** @returns {Promise<void>} Evita filtrar direcciones o credenciales. */ async () => {
  const sender = createPushSender({
    /** @returns {Promise<void>} Simula error con información privada. */ async send() {
      throw Object.assign(new Error('private address'), {
        code : 'messaging/installation-not-registered',
      });
    },
  });
  await assert.rejects(
    sender.send(device, job),
    /** @param {Error} error Error público. @returns {boolean} Se retiró toda información privada. */ (
      error,
    ) =>
      error.code === 'PUSH_ADDRESS_INVALID' &&
      error.message === 'PUSH_PROVIDER_FAILED' &&
      !error.cause,
  );
});
test('registro usa identidad canónica e ignora identidades proporcionadas en los datos', /** @returns {Promise<void>} Acredita el destino desde Security. */ async () => {
  let saved;
  const service = createPushRegistration({
    available                              : true,
    /** @returns {Date} Fecha fija. */ now : () => date,
    /** @returns {Promise<Object>} Sesión revalidada. */ async currentIdentity() {
      return {
        userCode  : 'U1',
        workspace : 'W1',
        policies  : ['communication.chat.view.owner'],
      };
    },
    repository                             : {
      /** @param {Object} value Registro acreditado. @returns {Promise<void>} Guarda evidencia. */ async register(
        value,
      ) {
        saved = value;
      },
    },
  });
  const result  = await service.register(
    {
      installationId : 'I1',
      bindingId      : 'B1',
      generation     : 2,
      platform       : 'ios',
      fid            : device.fid,
      userCode       : 'ATTACKER',
      workspace      : 'OTHER',
    },
    {
      user        : { userCode: 'U1' },
      sessionCode : 'S1',
    },
  );
  assert.equal(saved.userCode, 'U1');
  assert.equal(saved.workspace, 'W1');
  assert.equal(saved.sessionCode, 'S1');
  assert.deepEqual(result, { bindingId: 'B1' });
  await assert.rejects(service.register({}, { user: { userCode: 'OTHER' } }), { statusCode: 401 });
});
test('revalidación de entrega exige sesión, espacio, pertenencia y mensajes pendientes', /** @returns {Promise<void>} Distingue acceso retirado de fallo técnico. */ async () => {
  let identity = {
    userCode   : 'U1',
    workspace  : 'W1',
    policies   : [],
    workspaces : ['W1'],
  };
  let unread = 1,
    calls    = 0;
  const access = createPushAccess({
    /** @param {string} target Servicio. @param {string} action Contrato. @param {Object} data Datos mínimos. @param {Object} options Contexto confiable. @returns {Promise<Object>} Respuesta simulada. */
    async request(target, action, data, options) {
      calls++;
      assert.equal(options.context.sessionCode, 'S1');
      return {
        statusCode : 200,
        data       : action === 'identity-get-current' ? identity : { unread },
      };
    },
  });
  assert.equal(await access.eligible(device, job), true);
  unread = 0;
  assert.equal(await access.eligible(device, job), false);
  identity = {
    ...identity,
    workspace : 'W2',
  };
  const before = calls;
  assert.equal(await access.eligible(device, job), false);
  assert.equal(calls - before, 1);
});
test('DTO cierra registro y evento, rechaza identidades externas y fechas inválidas', /** @returns {void} Comprueba contratos sin servidor. */ () => {
  const value = {
    installationId : '12345678-1234-4234-8234-123456789012',
    bindingId      : '12345678-1234-4234-8234-123456789013',
    generation     : 1,
    platform       : 'android',
    fid            : device.fid,
  };
  assert.deepEqual(dtos['push-device-register'].input(value), value);
  assert.throws(
    /** @returns {Object} Ejecuta una entrada inválida que debe rechazarse. */ () =>
      dtos['push-device-register'].input({
        ...value,
        userCode : 'U2',
      }),
  );
  assert.throws(
    /** @returns {Object} Ejecuta una entrada inválida que debe rechazarse. */ () =>
      dtos['push-device-register'].input({
        ...value,
        generation : 0,
      }),
  );
  const event = {
    messageCode        : 'M1',
    conversationCode   : 'C1',
    sequence           : 1,
    workspace          : 'W1',
    recipientUserCodes : ['U1'],
    creationDate       : date.getTime(),
  };
  assert.deepEqual(dtos['communication.message.notification-requested'].input(event), event);
  assert.throws(
    /** @returns {Object} Ejecuta una entrada inválida que debe rechazarse. */ () =>
      dtos['communication.message.notification-requested'].input({
        ...event,
        creationDate : 'bad',
      }),
  );
});
