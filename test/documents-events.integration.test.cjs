/** @file Outbox y RabbitMQ reales: documentos llegan a cada instancia y un reinicio exige reconciliación. */
'use strict';
const { reader, readIdentity }     = require('./helpers/repository-read.cjs');
const test                         = require('node:test');
const assert                       = require('node:assert/strict');
const mongoose                     = require('mongoose');
const { randomUUID }               = require('node:crypto');
const { startIsolatedMongo }       = require('./helpers/isolated-mongo.cjs');
const { startIsolatedAmqp, delay } = require('./helpers/isolated-amqp.cjs');
const { MessagingRuntime }         = require('../server/library/messaging/runtime');
const { createFacade }             = require('../server/library/messaging/facade');
const { createPlatformEvents }     = require('../server/adapters/messaging/platform-events');
const { createDocumentsRealtime }  = require('../server/adapters/socket/documents-realtime');
/** @param {Function} condition Predicado de integración. @returns {Promise<void>} Espera acotada sólo de la prueba. */
async function until(condition) {
  for (let i = 0; i < 300; i++) {
    if (condition()) return;
    await delay(50);
  }
  assert.fail('DOCUMENT_EVENT_TIMEOUT');
}
test(
  'outbox documental llega a dos procesos; reiniciar RabbitMQ marca discontinuidad y reanuda',
  {
    skip    :
      !process.env.TEST_MONGOD_BINARY ||
      !process.env.TEST_RABBITMQ_SERVER_BINARY ||
      !process.env.TEST_EPMD_BINARY,
    timeout : 90000,
  },
  /** @returns {Promise<void>} Broker y base efímeros sin datos de instalación. */ async () => {
    let db,
      broker,
      runtime,
      billingRuntime,
      intelligenceRuntime,
      securityRuntime,
      connectRuntime,
      communicationRuntime;
    const billingDeliveries      = [[], []];
    const intelligenceDeliveries = [[], []];
    const resourceDeliveries     = [[], []];
    const documentResources      = [[], []];
    const accessDeliveries       = [[], []];
    const connectDeliveries      = [[], []];
    const settingsDeliveries     = [[], []];
    const membershipDeliveries   = [[], []];
    const metaDeliveries         = [[], []];
    const templateDeliveries     = [[], []];
    const mailboxDeliveries      = [[], []];
    const subscribers = [],
      projections     = [],
      deliveries      = [[], []];
    let rootRevision = 0;
    const root = 'aaaaaaaaaaaaaaaaaaaaaaaa',
      file     = 'ffffffffffffffffffffffff';
    try {
      db = await startIsolatedMongo({
        binary : process.env.TEST_MONGOD_BINARY,
        mongoose,
      });
      broker = await startIsolatedAmqp({
        rabbitmqServer : process.env.TEST_RABBITMQ_SERVER_BINARY,
        epmd           : process.env.TEST_EPMD_BINARY,
      });
      const connection = db.connection,
        namespace      = 'documents-' + randomUUID();
      for (const [name, indexes] of Object.entries(
        require('../server/library/messaging/indexes.json'),
      ))
        for (const { key, ...options } of indexes)
          await connection.db.collection(name).createIndex(key, options);
      runtime = new MessagingRuntime({
        service : 'service-documents',
        namespace,
        url     : broker.url,
        config  : {
          startTimeoutMs    : 10000,
          rpcTimeoutMs      : 5000,
          shutdownTimeoutMs : 5000,
        },
        logger  : { /** @returns {void} No divulga cargas de integración. */ error() {} },
      });
      const publisher = createFacade(runtime);
      await runtime.start({ connection });
      billingRuntime = new MessagingRuntime({
        service : 'service-billing',
        namespace,
        url     : broker.url,
        config  : {
          startTimeoutMs    : 10000,
          rpcTimeoutMs      : 5000,
          shutdownTimeoutMs : 5000,
        },
        logger  : { /** @returns {void} No expone datos del fixture. */ error() {} },
      });
      await billingRuntime.start({ connection });
      const billingPublisher = createFacade(billingRuntime);
      intelligenceRuntime = new MessagingRuntime({
        service : 'service-intelligence',
        namespace,
        url     : broker.url,
        config  : {
          startTimeoutMs    : 10000,
          rpcTimeoutMs      : 5000,
          shutdownTimeoutMs : 5000,
        },
        logger  : { /** @returns {void} No registra contenido. */ error() {} },
      });
      await intelligenceRuntime.start({ connection });
      const intelligencePublisher = createFacade(intelligenceRuntime);
      securityRuntime = new MessagingRuntime({
        service : 'service-security',
        namespace,
        url     : broker.url,
        config  : {
          startTimeoutMs    : 10000,
          rpcTimeoutMs      : 5000,
          shutdownTimeoutMs : 5000,
        },
        logger  : { /** @returns {void} No divulga datos de prueba. */ error() {} },
      });
      await securityRuntime.start({ connection });
      connectRuntime = new MessagingRuntime({
        service : 'service-connect',
        namespace,
        url     : broker.url,
        config  : {
          startTimeoutMs    : 10000,
          rpcTimeoutMs      : 5000,
          shutdownTimeoutMs : 5000,
        },
        logger  : {
          /** @returns {void} No registra contenido de cuentas. */ error() {},
        },
      });
      await connectRuntime.start({ connection });
      const connectPublisher  = createFacade(connectRuntime);
      /** @param {string} userCode Cuenta modificada. @returns {Promise<Object>} Publicación durable desde Connect. */
      const publishConnect    = (userCode) =>
        connectPublisher.publishEvent(
          'connect.mail-account.changed',
          {
            userCode,
            listChange : 'update',
          },
          { context: {} },
        );
      const securityPublisher = createFacade(securityRuntime);
      /** @param {number} revision Revisión persistida. @returns {Promise<Object>} Publicación durable de acceso desde el productor real. */
      const publishAccess     = (revision) =>
        securityPublisher.publishEvent(
          'security.access.changed',
          {
            resource : 'user',
            code     : 'u',
            revision,
          },
          { context: {} },
        );

      /** @param {number} revision Revisión confirmada. @returns {Promise<Object>} Outbox real del productor. */
      const publishIntelligence = (revision) =>
        intelligencePublisher.publishEvent(
          'intelligence.run.changed',
          {
            runCode   : 'RUN1',
            workspace : 'W1',
            revision,
          },
          { context: {} },
        );

      /** @param {number} revision Revisión persistida. @returns {Promise<Object>} Evento desde el outbox real del servicio acreditado. */
      const publishBilling = (revision) =>
        billingPublisher.publishEvent(
          'billing.operation.changed',
          {
            item : {
              code         : 'OP1',
              workspace    : 'W1',
              revision,
              userCreate   : 'U1',
              userAssigned : 'U1',
            },
          },
          { context: {} },
        );
      for (let i = 0; i < 2; i++) {
        const handlers = {},
          identity     = {
            userCode        : 'U1',
            sessionCode     : 'S1',
            expiration      : Date.now() / 1000 + 600,
            activeWorkspace : 'W1',
            workspaces      : ['W1'],
            policies        : [],
          };
        const socket     = {
          id   : 'socket-' + i,
          data : { identity },
          /** @param {string} name Evento. @param {Function} listener Callback. @returns {void} Registra. */ on(
            name,
            listener,
          ) {
            handlers[name] = listener;
          },
          /** @param {string} name Canal. @param {Object} value Evento. @returns {void} Recibe. */ emit(
            name,
            value,
          ) {
            deliveries[i].push(value);
          },
        };
        const reads      = reader({
          receipts : require('../server/modules/realtime/repositories/read-receipts').readReceipts(
            connection,
          ),
        });
        const windows    = new (require('../server/library/realtime/windows').RealtimeWindows)({
          topics   : {},
          receipts : require('../server/modules/realtime/repositories/read-receipts').readReceipts(
            connection,
          ),
        });
        const projection = createDocumentsRealtime({
          /** @returns {Object} Coordinador real del proceso; esta integración observa manifiestos, el catálogo se prueba por HTTP/MCP separado. */
          windows                                                                      : () => windows,
          /** @returns {Object} Recibos Mongo compartidos por las instancias. */ reads : () => reads,
          messaging                                                                    : {
            /** @param {string} service Destino. @param {string} action Lectura. @returns {Promise<Object>} Proyección autorizada; Mongo y broker sí son reales. */ async request(
              service,
              action,
            ) {
              return {
                statusCode : 200,
                data       :
                  action === 'documents-entry-get-one'
                    ? {
                        item : {
                          id           : file,
                          repositoryId : root,
                          revision     : 1,
                          name         : 'Archivo',
                        },
                      }
                    : {
                        listItem : [
                          {
                            id           : root,
                            revision     : rootRevision,
                            capabilities : { view: true },
                          },
                        ],
                      },
              };
            },
          },
        });
        projections.push(projection);
        projection.bind(socket);
        projection.authenticateRepositories(
          socket,
          /** @returns {Promise<Object>} Principal verificado de prueba. */ async () => identity,
        );
        const subscriber = createPlatformEvents({
          url       : broker.url,
          namespace,
          documents : projection,
          /** @param {Object} change Referencia validada. @returns {Promise<void>} Fanout de Security en cada proceso. */
          async access(change) {
            accessDeliveries[i].push(change.revision);
          },
          windows   : {
            /** @param {string} topic Dominio. @param {Object} data Hecho validado. @returns {Promise<void>} Entrega independiente de cada proceso. */
            async changed(topic, data) {
              if (topic.startsWith('documents.'))
                documentResources[i].push({
                  topic,
                  revision : data.revision,
                });
              else if (topic === 'connect.mailbox')
                mailboxDeliveries[i].push(data.id || data.userCode);
              else if (topic === 'connect.meta-conversation') {
                assert.equal(data.source, 'communication');
                membershipDeliveries[i].push(data.revision);
              } else if (topic === 'connect.meta') metaDeliveries[i].push(data.code);
              else if (topic === 'connect.template') templateDeliveries[i].push(data.code);
              else if (topic === 'connect.settings') settingsDeliveries[i].push(data.parameter);
              else if (topic === 'connect.mail-account')
                connectDeliveries[i].push(data.userCode || data.parameter);
              else if (topic.startsWith('intelligence.') && topic !== 'intelligence.run') {
                assert.equal(topic, 'intelligence.' + data.resource);
                resourceDeliveries[i].push(data);
              } else {
                assert.equal(topic, 'intelligence.run');
                intelligenceDeliveries[i].push(data.revision);
              }
            },
          },
          billing   : {
            /** @param {Object} message Aviso validado por DTO. @param {string} resource Tipo registrado. @returns {Promise<void>} Cada proceso recibe su propio hecho. */
            async changed(message, resource) {
              assert.equal(resource, 'operation');
              billingDeliveries[i].push(message.data.item.revision);
            },
          },
          logger    : {},
        });
        subscribers.push(subscriber);
        await subscriber.start();
        let ready;
        await handlers['realtime'](
          {
            type           : 'subscribe',
            requestId      : 'subscribe',
            topic          : 'documents.entries',
            section        : 'repository',
            subscriptionId : 'repo',
            watchVersion   : 1,
            query          : { repositoryId: root },
          },
          /** @param {Object} ack Confirmación. @returns {void} Verifica sólo control. */ (ack) => {
            assert.equal(ack.statusCode, 200);
            ready = ack;
          },
        );
        const request = {
          subscriptionId : 'repo',
          watchVersion   : 1,
          receiptId      : ready.receiptId,
          query          : { repositoryId: root },
        };
        await reads.read(request, readIdentity(identity));
        await handlers.realtime(
          {
            ...request,
            type      : 'bind',
            requestId : 'bind',
            topic     : 'documents.entries',
            section   : 'repository',
          },
          /** @param {Object} ack Confirmación del recibo HTTP. @returns {void} Ambas instancias acreditan la base. */ (
            ack,
          ) => assert.equal(ack.statusCode, 200),
        );
      }
      communicationRuntime = new MessagingRuntime({
        service : 'service-communication',
        namespace,
        url     : broker.url,
        config  : {
          startTimeoutMs    : 10000,
          rpcTimeoutMs      : 5000,
          shutdownTimeoutMs : 5000,
        },
        logger  : { /** @returns {void} No registra cargas. */ error() {} },
      });
      await communicationRuntime.start({ connection });
      const communicationPublisher = createFacade(communicationRuntime);
      /** @param {number} revision Revisión de membresía. @returns {Promise<void>} Evento del propietario y emisor falso rechazado. */
      async function publishMembership(revision) {
        const change = {
          conversationCode : 'CHAT',
          workspace        : 'w',
          revision,
          users            : ['u'],
          messageCode      : null,
        };
        await connectPublisher.publishEvent('communication.chat.changed', change, { context: {} });
        await communicationPublisher.publishEvent('communication.chat.changed', change, {
          context : {},
        });
        await until(
          /** @returns {boolean} Todas las instancias reacreditan el panel Meta. */ () =>
            membershipDeliveries.every(
              /** @param {Array} list Revisiones. @returns {boolean} Último hecho. */ (list) =>
                list.includes(revision),
            ),
        );
        for (const list of membershipDeliveries)
          assert.equal(
            list.filter(
              /** @param {number} value Revisión. @returns {boolean} Igualdad. */ (value) =>
                value === revision,
            ).length,
            1,
          );
      }
      await publishMembership(1);
      const session = connection.getClient().startSession();
      try {
        await session.withTransaction(
          /** @returns {Promise<void>} Dato y evento comparten commit. */ async () => {
            await connection.db.collection('DOCUMENT_TEST_STATE').insertOne(
              {
                file,
                revision : 1,
              },
              { session },
            );
            await publisher.publishEvent(
              'documents.entry.changed',
              {
                repositoryId : root,
                entryId      : file,
                parentId     : null,
                operation    : 'upload',
                revision     : 1,
                changeCode   : 'one',
              },
              {
                session,
                context : {},
              },
            );
          },
        );
      } finally {
        await session.endSession();
      }
      await until(
        /** @returns {boolean} Fanout independiente. */ () =>
          deliveries.every(
            /** @param {Array} list Eventos. @returns {boolean} Invalidación recibida. */ (list) =>
              list.at(-1)?.type === 'patch',
          ),
      );
      await publishAccess(1);
      await until(
        /** @returns {boolean} Las dos colas recibieron la revisión de acceso. */
        () =>
          accessDeliveries.every(
            /** @param {Array} revisions Referencias recibidas. @returns {boolean} Incluye el cambio acreditado. */
            (revisions) => revisions.includes(1),
          ),
      );
      for (const resource of ['agent', 'team', 'tool', 'knowledge-base', 'trigger', 'settings']) {
        await intelligencePublisher.publishEvent(
          'intelligence.resource.changed',
          {
            resource,
            code      : resource + '-one',
            workspace : resource === 'settings' ? 'allWorkspaces' : 'w',
            revision  : 1,
            erased    : false,
          },
          { context: {} },
        );
      }
      await until(
        /** @returns {boolean} Cada proceso recibe los seis recursos desde el productor acreditado. */
        () =>
          resourceDeliveries.every(
            /** @param {Array} values Referencias. @returns {boolean} Todas las ventanas pueden actualizarse con este commit. */
            (values) =>
              new Set(
                values
                  .filter(
                    /** @param {Object} change Hecho. @returns {boolean} Revisión del ensayo. */
                    (change) => change.revision === 1,
                  )
                  .map(
                    /** @param {Object} change Hecho. @returns {string} Dominio. */
                    (change) => change.resource,
                  ),
              ).size === 6,
          ),
      );
      rootRevision++;
      await publisher.publishEvent(
        'documents.repository.changed',
        {
          repositoryId : root,
          workspace    : 'W1',
          kind         : 'workspace',
          ownerCode    : '',
          revision     : 1,
          erased       : false,
        },
        { context: {} },
      );
      await publisher.publishEvent(
        'documents.settings.changed',
        {
          parameter : 'documents-settings',
          revision  : 1,
        },
        { context: {} },
      );
      await until(
        /** @returns {boolean} Ambas instancias reciben las cuatro ventanas documentales de esta revisión. */
        () =>
          documentResources.every(
            /** @param {Array} deliveries Hechos. @returns {boolean} Fanout completo después de la publicación. */
            (deliveries) =>
              new Set(
                deliveries
                  .filter(
                    /** @param {Object} item Hecho. @returns {boolean} Revisión esperada. */
                    (item) => item.revision === 1,
                  )
                  .map(
                    /** @param {Object} item Entrega. @returns {string} Ventana afectada. */
                    (item) => item.topic,
                  ),
              ).size === 4,
          ),
      );
      await publishIntelligence(1);
      await until(
        /** @returns {boolean} Ambos procesos reciben Intelligence. */ () =>
          intelligenceDeliveries.every(
            /** @param {Array} list Entregas. @returns {boolean} Primera revisión. */ (list) =>
              list.includes(1),
          ),
      );
      await publishBilling(1);
      await until(
        /** @returns {boolean} Las dos instancias reciben el hecho de Billing. */
        () =>
          billingDeliveries.every(
            /** @param {Array} list Revisiones. @returns {boolean} Una entrega causal. */
            (list) => list.includes(1),
          ),
      );
      await connectPublisher.publishEvent(
        'connect.settings.changed',
        {
          parameter : 'mail-connection',
          workspace : 'allWorkspaces',
        },
        { context: {} },
      );
      await connectPublisher.publishEvent(
        'connect.template.changed',
        {
          code      : 'T1',
          workspace : 'allWorkspaces',
        },
        { context: {} },
      );
      await connectPublisher.publishEvent(
        'connect.meta.changed',
        {
          resource : 'settings',
          code     : 'global',
        },
        { context: {} },
      );
      await publishConnect('first');
      await connectPublisher.publishEvent(
        'connect.mailbox.changed',
        {
          userCode  : 'u',
          mailboxId : 'box',
          section   : 'messages',
          folder    : 'INBOX',
          id        : 'first-message',
          kind      : 'membership',
        },
        { context: {} },
      );
      await until(
        /** @returns {boolean} Ambos procesos reciben el cambio de cuenta. */
        () =>
          metaDeliveries.every(
            /** @param {Array} list Referencias. @returns {boolean} Todas las instancias reciben la configuración. */ (
              list,
            ) => list.includes('global'),
          ) &&
          templateDeliveries.every(
            /** @param {Array} list Hechos recibidos. @returns {boolean} Todas las instancias reciben la plantilla. */ (
              list,
            ) => list.includes('T1'),
          ) &&
          mailboxDeliveries.every(
            /** @param {Array} list Hechos de buzón. @returns {boolean} Cada proceso recibe mensajes y operaciones durables. */
            (list) => list.includes('first-message'),
          ) &&
          settingsDeliveries.every(
            /** @param {Array} list Hechos SMTP. @returns {boolean} El detalle recibe el evento propio. */
            (list) => list.includes('mail-connection'),
          ) &&
          connectDeliveries.every(
            /** @param {Array} list Entregas. @returns {boolean} Cambio previo al reinicio. */ (
              list,
            ) => list.includes('first') && list.includes('mail-connection'),
          ),
      );
      await broker.restart();
      await until(
        /** @returns {boolean} Ambas instancias recuperan bajo permisos actuales. */ () =>
          deliveries.every(
            /** @param {Array} list Eventos. @returns {boolean} Interrupción y recuperación. */ (
              list,
            ) =>
              list.some(
                /** @param {Object} event Evento. @returns {boolean} Interrupción. */ (event) =>
                  event.type === 'error',
              ) && list.at(-1)?.type === 'resync-required',
          ),
      );
      await publishAccess(2);
      await until(
        /** @returns {boolean} Las dos colas recibieron la revisión de acceso. */
        () =>
          accessDeliveries.every(
            /** @param {Array} revisions Referencias recibidas. @returns {boolean} Incluye el cambio acreditado. */
            (revisions) => revisions.includes(2),
          ),
      );
      for (const resource of ['agent', 'team', 'tool', 'knowledge-base', 'trigger', 'settings']) {
        await intelligencePublisher.publishEvent(
          'intelligence.resource.changed',
          {
            resource,
            code      : resource + '-one',
            workspace : resource === 'settings' ? 'allWorkspaces' : 'w',
            revision  : 2,
            erased    : false,
          },
          { context: {} },
        );
      }
      await until(
        /** @returns {boolean} Cada proceso recibe los seis recursos desde el productor acreditado. */
        () =>
          resourceDeliveries.every(
            /** @param {Array} values Referencias. @returns {boolean} Todas las ventanas pueden actualizarse con este commit. */
            (values) =>
              new Set(
                values
                  .filter(
                    /** @param {Object} change Hecho. @returns {boolean} Revisión del ensayo. */
                    (change) => change.revision === 2,
                  )
                  .map(
                    /** @param {Object} change Hecho. @returns {string} Dominio. */
                    (change) => change.resource,
                  ),
              ).size === 6,
          ),
      );
      rootRevision++;
      await publisher.publishEvent(
        'documents.repository.changed',
        {
          repositoryId : root,
          workspace    : 'W1',
          kind         : 'workspace',
          ownerCode    : '',
          revision     : 2,
          erased       : false,
        },
        { context: {} },
      );
      await publisher.publishEvent(
        'documents.settings.changed',
        {
          parameter : 'documents-settings',
          revision  : 2,
        },
        { context: {} },
      );
      await until(
        /** @returns {boolean} Ambas instancias reciben las cuatro ventanas documentales de esta revisión. */
        () =>
          documentResources.every(
            /** @param {Array} deliveries Hechos. @returns {boolean} Fanout completo después de la publicación. */
            (deliveries) =>
              new Set(
                deliveries
                  .filter(
                    /** @param {Object} item Hecho. @returns {boolean} Revisión esperada. */
                    (item) => item.revision === 2,
                  )
                  .map(
                    /** @param {Object} item Entrega. @returns {string} Ventana afectada. */
                    (item) => item.topic,
                  ),
              ).size === 4,
          ),
      );
      await publishIntelligence(2);
      await until(
        /** @returns {boolean} Ambos procesos recuperan Intelligence tras el broker. */ () =>
          intelligenceDeliveries.every(
            /** @param {Array} list Entregas. @returns {boolean} Revisión posterior. */ (list) =>
              list.includes(2),
          ),
      );
      await publishBilling(2);
      await until(
        /** @returns {boolean} Recupera la suscripción efímera en cada proceso. */
        () =>
          billingDeliveries.every(
            /** @param {Array} list Revisiones recibidas. @returns {boolean} Continuidad restablecida. */
            (list) => list.includes(2),
          ),
      );
      await connectPublisher.publishEvent(
        'connect.settings.changed',
        {
          parameter : 'mail-connection',
          workspace : 'allWorkspaces',
        },
        { context: {} },
      );
      await connectPublisher.publishEvent(
        'connect.template.changed',
        {
          code      : 'T2',
          workspace : 'allWorkspaces',
        },
        { context: {} },
      );
      await connectPublisher.publishEvent(
        'connect.meta.changed',
        {
          resource : 'connection',
          code     : 'C2',
        },
        { context: {} },
      );
      await publishConnect('after-restart');
      await connectPublisher.publishEvent(
        'connect.mailbox.changed',
        {
          userCode  : 'u',
          mailboxId : 'box',
          section   : 'operations',
          id        : 'after-restart-operation',
          kind      : 'update',
        },
        { context: {} },
      );
      await until(
        /** @returns {boolean} La nueva suscripción recupera los cambios de Connect. */
        () =>
          metaDeliveries.every(
            /** @param {Array} list Referencias. @returns {boolean} Todas las instancias recuperan Meta después del reinicio. */ (
              list,
            ) => list.includes('C2'),
          ) &&
          templateDeliveries.every(
            /** @param {Array} list Hechos recibidos. @returns {boolean} La suscripción nueva conserva la distribución de plantillas. */ (
              list,
            ) => list.includes('T2'),
          ) &&
          mailboxDeliveries.every(
            /** @param {Array} list Hechos de buzón. @returns {boolean} Cada proceso recibe mensajes y operaciones durables. */
            (list) => list.includes('after-restart-operation'),
          ) &&
          settingsDeliveries.every(
            /** @param {Array} list Hechos SMTP. @returns {boolean} Ambas instancias recuperan la suscripción de detalle. */
            (list) => list.length === 2,
          ) &&
          connectDeliveries.every(
            /** @param {Array} list Entregas. @returns {boolean} Cambio posterior al reinicio. */ (
              list,
            ) =>
              list.includes('after-restart') &&
              list.filter(
                /** @param {string} name Entrega. @returns {boolean} Cambio de SMTP global. */
                (name) => name === 'mail-connection',
              ).length === 2,
          ),
      );
      await publishMembership(2);
      for (const list of deliveries) {
        assert.equal(
          list.filter(
            /** @param {Object} event Entrega. @returns {boolean} Cambio puntual de metadatos de raíz. */ (
              event,
            ) => event.type === 'patch' && event.data.metadataOnly,
          ).length,
          2,
        );
        assert.equal(
          list.filter(
            /** @param {Object} event Evento. @returns {boolean} Dato entregado una vez. */ (
              event,
            ) => event.type === 'patch' && !event.data.metadataOnly,
          ).length,
          1,
        );
        for (let i = 1; i < list.length; i++)
          assert.equal(list[i].delivery, list[i - 1].delivery + 1);
      }
    } finally {
      for (const subscriber of subscribers) await subscriber.close();
      for (const projection of projections) projection.close();
      await communicationRuntime?.close();
      await connectRuntime?.close();
      await securityRuntime?.close();
      await intelligenceRuntime?.close();
      await billingRuntime?.close();
      await runtime?.close();
      await broker?.stop();
      await db?.stop();
    }
  },
);
