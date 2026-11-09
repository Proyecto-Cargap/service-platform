/** @file Composición del servidor, salud y cierre; importar no inicia listeners ni conexiones. */
'use strict';
/**
 * Compone createApplication con las dependencias recibidas y devuelve su interfaz.
 * @param {Object} options Colaboradores u opciones: messaging, telemetry, env, processRef.
 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function createApplication({ messaging, telemetry, env = process.env, processRef = process }) {
  const host   = require('./shared/loopback-host').loopbackHost(
    env.PLATFORM_SOCKET_HOST,
    'PLATFORM_SOCKET_HOST',
  );
  const app    = require('express')();
  const server = require('http').createServer(app);
  const io     = require('socket.io')(server, {
    transports : ['websocket'],
    cors       : true,
    origin     : ['*'],
  });
  messaging.documentsRealtime =
    require('./adapters/socket/documents-realtime').createDocumentsRealtime({
      messaging,
    });
  const documentsEvents = require('./adapters/messaging/platform-events').createPlatformEvents({
    url       : env.MQ,
    namespace : require('./shared/runtime-identity').runtimeIdentity(env).namespace,
    documents : messaging.documentsRealtime,
    /** @param {Object} change Referencia durable sin credenciales. @returns {Promise<void>} Revalida ventanas de este proceso y sus proyecciones todavía registradas. */
    async access(change) {
      await messaging.documentsRealtime.accessChanged(change);
    },
    windows   : {
      /** @param {string} topic Ventana registrada. @param {Object} change Hecho del productor. @returns {Promise<void>} Fanout a ventanas de este proceso. */
      changed(topic, change) {
        return require('./modules/realtime/compositions/realtime.composition')
          .realtimeWindows()
          .changed(topic, change);
      },
    },
    billing   : require('./modules/notifications/services/billing-events.service').billingEvents({
      windows : {
        /** @param {string} topic Dominio. @param {Object} change Hecho durable. @returns {Promise<void>} Fanout local sobre las ventanas inicializadas por catálogo. */
        changed(topic, change) {
          return require('./modules/realtime/compositions/realtime.composition')
            .realtimeWindows()
            .changed(topic, change);
        },
      },
    }),
    logger    : telemetry.logger,
  });

  const lifecycle                = messaging;
  const pushWorker               = require('./modules/push/compositions/push.composition').createPush({
    messaging,
    logger : telemetry.logger,
    env,
  }).worker;
  const worker                   =
    require('./modules/notifications/compositions/notifications.composition').createNotificationsCenter(
      {
        messaging,
        telemetry,
        logger : telemetry.logger,
      },
    ).worker;
  const { registerSocketHealth } = require('./adapters/socket/health');
  const checkSocketHealth        = registerSocketHealth({
    messaging      : lifecycle,
    connection     : require('./adapters/mongodb/connection').connection,
    server,
    /**
     * Ejecuta documentsReady dentro del flujo de bootstrap.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    documentsReady : () => documentsEvents.ready(),
    releaseFile    : require('node:path').resolve(__dirname, '../release.json'),
  });
  telemetry.attachHealth(
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async () => (await checkSocketHealth()).data,
    {
      components : ['database', 'messaging', 'socket', 'documentsRealtime'],
    },
  );
  require('./adapters/socket/socket').registerSocketEvents({
    io,
    messaging : lifecycle,
  });
  lifecycle.installShutdown(
    /**
     * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
     * @param {Object} options Colaboradores u opciones: deadlineAt.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async ({ deadlineAt }) => {
      await documentsEvents.close();
      messaging.documentsRealtime.close();
      await worker.close({ deadlineAt });
      await pushWorker.close({ deadlineAt });
      return new Promise(
        /**
         * Coordina la resolución o el rechazo de la operación asíncrona.
         * @param {Function} resolve Continuación que confirma el resultado de la promesa.
         * @returns {void} Finaliza después de aplicar los efectos de la operación.
         */
        (resolve) => {
          const timer = setTimeout(
            /**
             * Ejecuta la continuación cuando vence el intervalo configurado.
             * @returns {void} Finaliza después de aplicar los efectos de la operación.
             */
            () => {
              server.closeAllConnections();
              resolve();
            },
            Math.max(0, deadlineAt - Date.now()),
          );
          timer.unref();
          io.close(
            /**
             * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
             * @returns {void} Finaliza después de aplicar los efectos de la operación.
             */
            () => {
              clearTimeout(timer);
              resolve();
            },
          );
        },
      );
    },
  );
  /**
   * Inicia el componente y sus colaboradores.
   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  const start = () =>
    lifecycle
      .start()
      .then(
        /**
         * Procesa el resultado de la operación asíncrona.
         * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        () => documentsEvents.start(),
      )
      .then(
        /**
         * Procesa el resultado de la operación asíncrona.
         * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        async () => {
          await worker.start();
          pushWorker.start();
        },
      )
      .then(
        /**
         * Procesa el resultado de la operación asíncrona.
         * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        () =>
          new Promise(
            /**
             * Coordina la resolución o el rechazo de la operación asíncrona.
             * @param {Function} resolve Continuación que confirma el resultado de la promesa.
             * @param {Function} reject Continuación que rechaza la promesa con su causa.
             * @returns {void} Finaliza después de aplicar los efectos de la operación.
             */
            (resolve, reject) => {
              server.once('error', reject);
              server.listen(
                env.PORT_SOCKET,
                host,

                /**
                 * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
                 * @returns {void} Finaliza después de aplicar los efectos de la operación.
                 */
                () => {
                  server.removeListener('error', reject);
                  resolve();
                },
              );
            },
          ),
      )
      .catch(
        /**
         * Gestiona el fallo de la operación asíncrona y conserva la política de error del módulo.
         * @param {Error} error Fallo recibido por la continuación.
         * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
         */
        async (error) => {
          await documentsEvents.close();
          messaging.documentsRealtime.close();
          telemetry.logger.error({
            component : 'service',
            event     : 'startup_failed',
            code      : error.code || 'STARTUP_FAILED',
          });
          await lifecycle.close().catch(
            /**
             * Gestiona el fallo de la operación asíncrona y conserva la política de error del módulo.
             * @returns {void} Finaliza después de aplicar los efectos de la operación.
             */
            () => {},
          );
          processRef.exitCode = 1;
        },
      );

  return {
    start,
    /**
     * Cierra el componente y libera sus recursos.
     * @param {Object} options Opciones que delimitan el comportamiento de la operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    close : async (options) => {
      await documentsEvents.close();
      messaging.documentsRealtime.close();
      await worker.close(options);
      await pushWorker.close(options);
      return lifecycle.close(options);
    },
  };
}
module.exports = { createApplication };
