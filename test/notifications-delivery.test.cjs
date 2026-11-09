/** @file Pruebas de comportamiento de notifications-delivery.test en service-platform. */
'use strict';
const test                 = require('node:test');
const assert               = require('node:assert/strict');
const notificationsService = require('../server/modules/notifications/services/notifications.service');
test('notifications return the committed result without a second socket delivery' /**
 * Verifica el comportamiento esperado del caso de prueba.
 * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
 */, async () => {
  let writes = 0,
    sends    = 0;
  const service = notificationsService({
    logger      : {
      /** @returns {void} Un fallo posterior a persistir se propaga directamente al llamador. */
      error() {},
    },
    messaging   : {
      /**
       * Resuelve getContext aplicando los filtros y colaboradores del módulo.
       * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      getContext : () => ({
        user     : { userCode: 'u' },
        policies : ['platform.notifications.create.global'],
      }),
    },
    repository  : {
      /**
       * Crea el registro y devuelve el resultado de la operación.
       * @param {*} value Valor de value utilizado por esta operación.
       * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      create : async (value) => {
        writes++;
        return value;
      },
    },
    identifiers : {
      /**
       * Resuelve getObjectCode aplicando los filtros y colaboradores del módulo.
       * @returns {Promise<string>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      getObjectCode : async () => 'N1',
    },
    /**
     * Acredita el aviso canónico y simula un fallo posterior a persistir.
     * @param {Object} notice Identidad y espacio de la notificación creada.
     * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
     */
    sendToUser  : async (notice) => {
      assert.deepEqual(notice, {
        channel  : 'channel-notification-alerts',
        action   : 'notifications-changed',
        userCode : 'u',
        data     : {
          notificationCode : 'N1',
          workspace        : 'w',
        },
      });
      sends++;
      throw new Error('offline');
    },
  });
  const result  = await service.create({
    scope       : 'global',
    workspace   : 'w',
    userCode    : 'u',
    alertObject : {
      title : 'Title',
      type  : 'info',
    },
  });
  assert.equal(result.notificationCode, 'N1');
  assert.equal(writes, 1);
  assert.equal(sends, 0);
});
