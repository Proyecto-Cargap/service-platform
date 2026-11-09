/** @file Productor de notificaciones aislado para aceptación de clientes, con Mongo y repositorios reales. */
'use strict';
const mongoose               = require('mongoose');
const { startIsolatedMongo } = require('./isolated-mongo.cjs');
/** @returns {Promise<Object>} Casos canónicos, hechos atómicos y cierre de recursos propios. */
async function fixture() {
  const db        = await startIsolatedMongo({
    binary : process.env.TEST_MONGOD_BINARY,
    mongoose,
  });
  const actor     = {
    user        : { userCode: 'U1' },
    sessionCode : 'notifications-local',
    workspace   : 'W1',
    policies    : ['view', 'create', 'delete'].map(
      /** @param {string} action Permiso. @returns {string} Concesión. */ (action) =>
        `platform.notifications.${action}.global`,
    ),
  };
  const access    = { permitted: true };
  const messaging = {
    /** @returns {Object} Principal sintético acreditado. */ getContext                                                : () => actor,
    /** @param {number} statusCode Estado. @param {*} data Datos. @returns {Object} Sobre canónico. */ successResponse :
      (statusCode, data) => ({
        statusCode,
        descriptionCode : 'OK',
        data,
      }),
    /** @param {string} service Propietario. @param {string} action Capacidad. @param {Object} query Consulta. @returns {Promise<Object>} Sólo referencias Security del escenario. */
    async request(service, action, query) {
      if (service !== 'service-security') throw new Error('UNEXPECTED_SERVICE');
      if (!access.permitted) return { statusCode: 403 };
      if (action === 'identity-get-current')
        return {
          statusCode : 200,
          data       : {
            userCode   : 'U1',
            workspace  : 'W1',
            workspaces : ['W1'],
            policies   : actor.policies,
          },
        };
      if (action === 'user-profile-get-list')
        return {
          statusCode : 200,
          data       : {
            items : query.codes.map(
              /** @param {string} userCode Referencia. @returns {Object} Perfil público. */ (
                userCode,
              ) => ({
                userCode,
                name : userCode,
              }),
            ),
          },
        };
      if (action === 'workspace-users-get-list')
        return {
          statusCode : 200,
          data       : {
            items      : Array.from(
              { length: 100 },
              /** @param {*} value Reserva. @param {number} index Índice. @returns {Object} Destinatario. */ (
                value,
                index,
              ) => ({ userCode: 'U' + (index + 1) }),
            ),
            pagination : {
              page    : 1,
              perPage : 100,
              total   : 100,
            },
          },
        };
      throw new Error('UNREGISTERED_SECURITY_READ:' + action);
    },
    /** @param {string} name Hecho. @param {Object} data Referencia. @param {Object} options Misma unidad atómica. @returns {Promise<void>} Outbox real de la prueba. */
    async publishEvent(name, data, options) {
      await db.connection.db.collection('TEST_OUTBOX').insertOne(
        {
          name,
          data,
        },
        { session: options.session },
      );
    },
  };
  const models    = {};
  for (const [key, file] of [
    ['notifications', 'notifications'],
    ['broadcasts', 'notifications-broadcast'],
    ['recipients', 'notifications-recipient'],
  ]) {
    const original = require('../../server/modules/notifications/models/' + file + '.model');
    models[key] = db.connection.model(
      original.modelName,
      original.schema,
      original.collection.name,
    );
    await models[key].createCollection();
    await models[key].createIndexes();
  }
  await db.connection.db.createCollection('TEST_OUTBOX');
  const repository =
    require('../../server/modules/notifications/repositories/notifications-center.repository')({
      ...models,
      messaging,
    });
  const creation   =
    require('../../server/modules/notifications/repositories/notifications.repository')({
      model : models.notifications,
      messaging,
    });
  const controller =
    require('../../server/modules/notifications/controllers/notifications-center.controller').notificationsCenterController(
      {
        repository,
        messaging,
      },
    );
  return {
    actor,
    access,
    models,
    repository,
    db                                                                         : db.connection.db,
    /** @param {string} operation Capacidad canónica. @param {Object} data Selector. @returns {Promise<Object>} Sobre autorizado. */
    call                                                                       : (operation, data = {}) => controller.execute(operation, { data }),
    /** @param {number} index Identidad. @param {Object} values Valores públicos. @returns {Promise<Object>} Alta y hecho dentro de la misma transacción. */
    create                                                                     : (index, values = {}) =>
      creation.createEvent({
        notificationCode : 'N' + String(index).padStart(3, '0'),
        workspace        : 'W1',
        userCode         : 'U1',
        userAssigned     : 'U1',
        userCreate       : '',
        sourceEventKey   : 'test-' + index,
        sourceKind       : 'event',
        title            : 'Aviso ' + String(index).padStart(3, '0'),
        content          : 'Contenido de la notificación',
        type             : 'info',
        viewed           : false,
        creationDate     : 1790000000000 - index * 60000,
        updateDate       : 1790000000000,
        ...values,
      }),
    /** @returns {Promise<void>} Libera exclusivamente Mongo efímero. */ close : () => db.stop(),
  };
}
module.exports = { fixture };
