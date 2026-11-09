/** @file Pruebas de comportamiento de notifications-audience.test en service-platform. */
'use strict';
const test                       = require('node:test');
const assert                     = require('node:assert/strict');
const notificationsCenterService = require('../server/modules/notifications/services/notifications-center.service');
const {
  createNotificationsWorker,
} = require('../server/modules/notifications/workers/notifications-worker');

test('la difusión recorre toda la membresía mediante el contrato genérico paginado' /**
 * Verifica el comportamiento esperado del caso de prueba.
 * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
 */, async () => {
  const pages = [];
  let prepared;
  const identity = {
    userCode   : 'autor',
    workspace  : 'equipo',
    workspaces : ['equipo'],
    policies   : ['platform.notifications.create.workspace'],
  };
  const service  = notificationsCenterService({
    messaging  : {
      /**
       * Invoca el puerto remoto conservando el contexto y las opciones de entrega.
       * @param {string} target Valor de target utilizado por esta operación.
       * @param {string} action Valor de action utilizado por esta operación.
       * @param {Object} data Datos de entrada de la operación.
       * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      request : async (target, action, data) => {
        assert.equal(target, 'service-security');
        if (action === 'identity-get-current')
          return {
            statusCode : 200,
            data       : identity,
          };
        assert.equal(action, 'workspace-users-get-list');
        assert.equal(data.workspace, 'equipo');
        assert.equal(data.perPage, 100);
        pages.push(data.page);
        return {
          statusCode : 200,
          data       : {
            items      : Array.from(
              { length: data.page === 1 ? 100 : 5 },

              /**
               * Construye un elemento de la colección resultante.
               * @param {*} _ Valor de _ utilizado por esta operación.
               * @param {number} index Valor de index utilizado por esta operación.
               * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
               */
              (_, index) => ({ userCode: `u${(data.page - 1) * 100 + index}` }),
            ),
            pagination : {
              page    : data.page,
              perPage : 100,
              total   : 105,
            },
          },
        };
      },
    },
    repository : {
      /**
       * Ejecuta prepare dentro del flujo de notifications-audience.test.
       * @param {*} value Valor de value utilizado por esta operación.
       * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      prepare : async (value) => {
        prepared = value;
        return {
          ...value,
          state : 'prepared',
        };
      },
    },
    realtime   : {},
  });
  const result   = await service.preview({
    title     : 'Aviso',
    content   : 'Contenido',
    type      : 'info',
    workspace : 'equipo',
  });
  assert.deepEqual(pages, [1, 2]);
  assert.equal(prepared.audience.length, 105);
  assert.equal(new Set(prepared.audience).size, 105);
  assert.equal(result.recipientCount, 105);
});

test('el worker revalida pertenencia con el mismo contrato antes de persistir cada destinatario' /**
 * Verifica el comportamiento esperado del caso de prueba.
 * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
 */, async () => {
  const outcomes = [],
    releases     = [],
    notices      = [];
  const job    = { workspace: 'equipo' };
  const worker = createNotificationsWorker({
    repository : {
      /**
       * Ofrece el punto de extensión indexesReady sin efectos por defecto.
       * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
       */
      indexesReady     : async () => {},

      /**
       * Adquiere la propiedad temporal del trabajo disponible.
       * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      claim            : async () => job,
      /**
       * Ejecuta batch dentro del flujo de notifications-audience.test.
       * @returns {Promise<Array<*>>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      batch            : async () => [{ userCode: 'vigente' }, { userCode: 'retirado' }],
      /**
       * Renueva la propiedad temporal del trabajo.
       * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      renew            : async () => ({ matchedCount: 1 }),
      /**
       * Ejecuta persistRecipient dentro del flujo de notifications-audience.test.
       * @param {*} selected Valor de selected utilizado por esta operación.
       * @param {*} row Valor de row utilizado por esta operación.
       * @param {*} member Valor de member utilizado por esta operación.
       * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      persistRecipient : async (selected, row, member) => {
        assert.equal(selected, job);
        outcomes.push([row.userCode, member]);
        return member
          ? {
              notificationCode : 'n',
              workspace        : 'equipo',
            }
          : null;
      },
      /**
       * Ejecuta failRecipient dentro del flujo de notifications-audience.test.
       * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
       */
      failRecipient    : async () => {
        assert.fail('la membresía válida no debe fallar');
      },
      /**
       * Libera la propiedad temporal del trabajo.
       * @param {*} selected Valor de selected utilizado por esta operación.
       * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
       */
      release          : async (selected) => {
        releases.push(selected);
      },
    },
    messaging  : {
      /**
       * Invoca el puerto remoto conservando el contexto y las opciones de entrega.
       * @param {string} target Valor de target utilizado por esta operación.
       * @param {string} action Valor de action utilizado por esta operación.
       * @param {Object} data Datos de entrada de la operación.
       * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      request : async (target, action, data) => {
        assert.equal(target, 'service-security');
        assert.equal(action, 'workspace-users-get-list');
        assert.equal(data.workspace, 'equipo');
        return {
          statusCode : 200,
          data       : { items: data.userCodes[0] === 'vigente' ? [{ userCode: 'vigente' }] : [] },
        };
      },
    },
    realtime   : {
      /**
       * Publica el mensaje en el ámbito autorizado.
       * @param {...*} args Valor de args utilizado por esta operación.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      publish : (...args) => notices.push(args),
    },
  });
  assert.equal(await worker.tick(), true);
  assert.deepEqual(outcomes, [
    ['vigente', true],
    ['retirado', false],
  ]);
  assert.deepEqual(releases, [job]);
  assert.equal(notices.length, 0);
});
