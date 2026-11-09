/**
 * @file Catálogo de acciones y eventos propios del servicio. Relaciona cada nombre público con su
 * controlador de aplicación.
 */
'use strict';
/**
 * Catálogo de acciones y eventos de la aplicación. La validación de sus datos permanece en los
 * controladores.
 * @param {Object} options Colaboradores u opciones: messaging.
 * @returns {Array<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */

async function createCatalog({ messaging }) {
  const desktopRelease          =
    require('./desktop-release/controllers/desktop-release.controller').desktopReleaseController({
      messaging,
      readCurrent : require('../adapters/files/desktop-release.catalog').desktopReleaseCatalog(
        process.env.DESKTOP_RELEASE_ROOT || '/var/lib/xidden-desktop-documents',
      ),
    });
  const logger                  = require('../telemetry').logger;
  const pushComposition         = require('./push/compositions/push.composition').createPush({
    messaging,
    logger,
  });
  const push                    = pushComposition.controller;
  const identifiers             = require('./counters/repositories/counters.repository')({
    messaging,
    model : require('./counters/models/counters.model'),
  });
  const configurationRepository = require('./configuration/repositories/configuration.repository')({
    messaging,
    model : require('./configuration/models/configuration.model'),
  });
  const platformConfiguration   =
    require('./configuration/services/configuration.service').configurationService({
      repository : configurationRepository,
    });
  const instanceRepository      = require('./instance/repositories/instance.repository')({
    messaging,
    model : require('./instance/models/instance.model'),
  });
  const releaseRepository       = require('./release/repositories/release.repository')({
    messaging,
    model : require('./release/models/release.model'),
  });
  const notificationsRepository = require('./notifications/repositories/notifications.repository')({
    messaging,
    model : require('./notifications/models/notifications.model'),
  });
  const configurationController =
    require('./configuration/controllers/configuration.controller').configurationController({
      messaging,
      repository : configurationRepository,
    });
  const settingsController      =
    require('./settings/controllers/settings.controller').settingsController({
      messaging,
      repository : require('./settings/repositories/settings.repository')({
        messaging,
        model : require('./settings/models/settings.model'),
      }),
    });
  const instanceController      =
    require('./instance/controllers/instance.controller').instanceController({
      messaging,
      logger,
      identifiers,
      repository : instanceRepository,
      releases   : releaseRepository,
    });
  const releaseController       = require('./release/controllers/release.controller').releaseController({
    messaging,
    logger,
    identifiers,
    repository     : releaseRepository,
    releases       : releaseRepository,
    verifyWebBuild : require('./release/services/release-web-build').releaseWebBuild({
      environment    : process.env.ENV_NAME,
      fetcher        : fetch,
      /**
       * Resuelve getInformation aplicando los filtros y colaboradores del módulo.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      getInformation : () => platformConfiguration.getInformation(),
    }),
  });
  const notificationCreation    = require('./notifications/services/notifications.service')({
    messaging,
    logger,
    identifiers,
    repository : notificationsRepository,
  });
  const notificationsController =
    require('./notifications/controllers/notifications.controller').notificationsController({
      messaging,
      service : notificationCreation,
    });
  const notificationCenter      =
    require('./notifications/compositions/notifications.composition').createNotificationsCenter({
      messaging,
      logger,
    }).controller;
  const windows                 =
    await require('./realtime/compositions/realtime.composition').createRealtimeWindows({
      messaging,
      connection : require('../adapters/mongodb/connection').connection,
      /** @param {string} operation Consulta canónica registrada. @param {Object} data Selector validado. @returns {Promise<Object>} Lectura propia sin autorreferencia RPC. */
      async platformRead(operation, data) {
        if (operation === 'configuration') return configurationController.getOne();
        if (operation.startsWith('release.'))
          return releaseController[operation.slice(8)]({ data });
        return notificationCenter.execute(operation, { data });
      },
    });
  const notificationOperations  = [
    'personalList',
    'personalOne',
    'personalSenderPicture',
    'personalCount',
    'personalView',
    'personalViewAll',
    'personalDelete',
    'personalDeleteViewed',
    'adminList',
    'adminOne',
    'adminDelete',
    'preview',
    'confirm',
    'broadcasts',
    'status',
    'retry',
  ];
  const customerEvents          = require('./notifications/services/customer-events.service').customerEvents(
    {
      messaging,
      identifiers,
      reminders : pushComposition.reminders,
    },
  );
  return [
    ...['documents.repository.changed', 'documents.settings.changed'].map(
      /** @param {string} name Hecho mínimo de Documents. @returns {Object} Registro durable; el fanout independiente entrega a cada instancia. */
      (name) => ({
        name,
        kind      : 'event',
        execution : 'nonTransactional',
        retrySafe : true,
        /** @param {Object} message Sobre acreditado. @returns {void} Sólo el productor documental puede publicar estas referencias. */
        handler(message) {
          if (message.originService !== 'service-documents')
            throw new (require('../shared/application-error').ApplicationError)(403, 'FORBIDDEN');
        },
      }),
    ),
    {
      name    : 'documents-repositories-read',
      kind    : 'action',
      handler : windows.repositoriesRead,
    },
    {
      name    : 'documents-repository-read',
      kind    : 'action',
      handler : windows.repositoryRead,
    },
    {
      name    : 'realtime-read',
      kind    : 'action',
      handler : windows.read,
    },
    {
      name    : 'desktop-release-current',
      kind    : 'action',
      handler : desktopRelease.current,
    },
    {
      name    : 'desktop-release-download',
      kind    : 'action',
      handler : desktopRelease.download,
    },
    {
      name      : 'push-device-register',
      kind      : 'action',
      execution : 'nonTransactional',
      retrySafe : true,
      /** Registra la revisión de una instalación de la sesión autenticada.
       * @param {Object} message Entrada cerrada y origen del gateway.
       * @returns {Promise<Object>} Vinculación opaca confirmada.
       */
      handler   : (message) => push.register(message),
    },
    {
      name      : 'communication.message.notification-requested',
      kind      : 'event',
      execution : 'nonTransactional',
      retrySafe : true,
      /** Persiste las entregas antes de confirmar el evento de Communication.
       * @param {Object} message Evento mínimo acreditado por el transporte.
       * @returns {Promise<void>} Confirmación durable, idempotente por evento e instalación.
       */
      handler   : (message) => push.enqueue(message),
    },
    {
      name      : 'customer.activity.reminder-due',
      kind      : 'event',
      execution : 'nonTransactional',
      retrySafe : true,
      /** @param {Object} message Evento interno de Customer. @returns {Promise<Object|null>} Entrega revalidada e idempotente. */
      handler   : (message) => customerEvents.reminder(message),
    },
    ...notificationOperations.map(
      /**
       * Proyecta cada elemento en la estructura requerida por la operación.
       * @param {*} operation Valor de operation utilizado por esta operación.
       * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      (operation) => ({
        name    : `notifications-center-${operation}`,
        kind    : 'action',
        /**
         * Atiende el mensaje con el contrato y las reglas de la operación.
         * @param {Object} message Mensaje recibido con datos y contexto del transporte.
         * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        handler : (message) => notificationCenter.execute(operation, message),
      }),
    ),
    {
      name    : 'configuration-get-one',
      kind    : 'action',
      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => configurationController.getOne(message),
    },
    {
      name    : 'configuration-get-information',
      kind    : 'action',
      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => configurationController.getInformation(message),
    },
    {
      name    : 'configuration-update',
      kind    : 'action',
      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => configurationController.update(message),
    },
    {
      name    : 'instance-create',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => instanceController.create(message),
    },
    {
      name    : 'instance-get-list',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => instanceController.getList(message),
    },
    {
      name    : 'instance-get-one',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => instanceController.getOne(message),
    },
    {
      name    : 'instance-update',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => instanceController.update(message),
    },
    {
      name    : 'instance-delete',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => instanceController.delete(message),
    },

    {
      name    : 'notifications-create',
      kind    : 'action',
      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => notificationsController.create(message),
    },

    {
      name    : 'release-create',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => releaseController.create(message),
    },
    {
      name    : 'release-current',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => releaseController.current(message),
    },
    {
      name    : 'release-web-build',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => releaseController.webBuild(message),
    },
    {
      name    : 'release-history',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => releaseController.history(message),
    },
    {
      name    : 'release-get-list',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => releaseController.getList(message),
    },
    {
      name    : 'release-get-one',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => releaseController.getOne(message),
    },
    {
      name    : 'release-update',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => releaseController.update(message),
    },
    {
      name    : 'release-delete',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => releaseController.delete(message),
    },
    {
      name    : 'release-published',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => releaseController.published(message),
    },
    {
      name    : 'settings-get-one',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => settingsController.getOne(message),
    },
    {
      name    : 'settings-get-list',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => settingsController.getList(message),
    },
    {
      name    : 'settings-update',
      kind    : 'action',

      /**
       * Atiende el mensaje con el contrato y las reglas de la operación.
       * @param {Object} message Mensaje recibido con datos y contexto del transporte.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      handler : (message) => settingsController.update(message),
    },
  ];
}
module.exports = {
  createCatalog : require('../contracts/index').catalogFactory(createCatalog),
};
