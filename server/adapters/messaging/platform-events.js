/** @file Cada proceso Socket.IO recibe los hechos de Platform del exchange existente, sin competir por una cola común. */
'use strict';
const eventDtos              = require('../../contracts/registry');
const billingNames           = Object.keys(eventDtos).filter(
  /** @param {string} name Contrato registrado. @returns {boolean} Hechos públicos de Billing sin nombres aportados por clientes. */
  (name) => /^billing\.[a-z-]+\.changed$/.test(name),
);
const protocol               = require('../../library/messaging/protocol');
const { topology, eventKey } = require('../../library/messaging/topology');

/**
 * Suscripción efímera por proceso; tras una pérdida del broker recupera las ventanas afectadas por Gateway.
 * @param {Object} dependencies Broker existente, namespace, proyecciones y logger técnicos.
 * @returns {Object} Inicio, disponibilidad y cierre coordinados con Platform.
 */
function createPlatformEvents({
  url,
  namespace,
  documents,
  access,
  billing,
  windows,
  logger,
  connect = require('amqplib').connect,
}) {
  const exchange = topology(namespace, 'service-platform').exchanges.events;
  let started = false,
    connection,
    channel,
    retry,
    opening,
    closing   = false;
  /**
   * Ejecuta lost dentro del flujo de platform-events.
   * @param {*} candidate Valor de candidate utilizado por esta operación.
   * @returns {undefined} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  function lost(candidate) {
    if (closing || connection !== candidate) return;
    connection = undefined;
    channel = undefined;
    void candidate?.close().catch(
      /**
       * Gestiona el fallo de la operación asíncrona y conserva la política de error del módulo.
       * @returns {void} Finaliza después de aplicar los efectos de la operación.
       */
      () => {},
    );
    documents.unavailable();
    if (started && !retry) {
      retry = setTimeout(
        /**
         * Ejecuta la continuación cuando vence el intervalo configurado.
         * @returns {void} Finaliza después de aplicar los efectos de la operación.
         */
        () => {
          retry = undefined;
          void open()
            .catch(
              /**
               * Gestiona el fallo de la operación asíncrona y conserva la política de error del módulo.
               * @returns {void} Finaliza después de aplicar los efectos de la operación.
               */
              () => {},
            )
            .finally(
              /**
               * Libera o restablece los recursos al terminar la operación.
               * @returns {void} Finaliza después de aplicar los efectos de la operación.
               */
              () => {
                if (started && !channel) lost(connection);
              },
            );
        },
        1000,
      );
      retry.unref?.();
    }
  }
  /**
   * Ejecuta open dentro del flujo de platform-events.
   * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async function open() {
    if (opening) return opening;
    const work = (
      /**
       * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
       * @returns {Promise<undefined>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      async () => {
        const candidate = await connect(url, { timeout: 10000 });
        if (!started) {
          await candidate.close();
          return;
        }
        connection = candidate;
        candidate.on(
          'error',

          /**
           * Atiende el evento emitido por el colaborador.
           * @returns {void} Finaliza después de aplicar los efectos de la operación.
           */
          () => {},
        );
        candidate.on(
          'close',

          /**
           * Atiende el evento emitido por el colaborador.
           * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          () => lost(candidate),
        );
        const consumer = await candidate.createChannel();
        consumer.on(
          'error',

          /**
           * Atiende el evento emitido por el colaborador.
           * @returns {void} Finaliza después de aplicar los efectos de la operación.
           */
          () => {},
        );
        consumer.on(
          'close',

          /**
           * Atiende el evento emitido por el colaborador.
           * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          () => lost(candidate),
        );
        await consumer.assertExchange(exchange, 'topic', { durable: true });
        const { queue } = await consumer.assertQueue('', {
          durable    : false,
          exclusive  : true,
          autoDelete : true,
        });
        await consumer.bindQueue(queue, exchange, eventKey('documents.entry.changed', 1));
        if (windows) {
          await consumer.bindQueue(queue, exchange, eventKey('documents.repository.changed', 1));
          await consumer.bindQueue(queue, exchange, eventKey('documents.settings.changed', 1));
        }
        if (access || windows)
          await consumer.bindQueue(queue, exchange, eventKey('security.access.changed', 1));
        if (windows)
          for (const name of [
            'platform.resource.changed',
            'supplier.supplier.changed',
            'product.resource.changed',
            'customer.resource.changed',
            'security.resource.changed',
            'directory.resource.changed',
            'intelligence.run.changed',
            'intelligence.resource.changed',
            'connect.mail-account.changed',
            'connect.settings.changed',
            'connect.template.changed',
            'connect.meta.changed',
            'connect.mailbox.changed',
            'connect.meta-conversation.changed',
            'communication.chat.changed',
          ])
            await consumer.bindQueue(queue, exchange, eventKey(name, 1));
        if (billing)
          for (const name of billingNames)
            await consumer.bindQueue(queue, exchange, eventKey(name, 1));
        await consumer.prefetch(20);
        await consumer.consume(
          queue,
          /**
           * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
           * @param {Object} message Mensaje recibido con datos y contexto del transporte.
           * @returns {undefined} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          (message) => {
            if (!message) {
              lost(candidate);
              return;
            }
            void (
              /**
               * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
               * @returns {Promise<undefined>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
               */
              async () => {
                let envelope;
                try {
                  envelope = protocol.decode(message.content);
                  if (
                    envelope.kind !== 'event' ||
                    !(
                      ((access || windows) &&
                        envelope.source === 'service-security' &&
                        ['security.access.changed', 'security.resource.changed'].includes(
                          envelope.name,
                        )) ||
                      (windows &&
                        envelope.source === 'service-communication' &&
                        envelope.name === 'communication.chat.changed') ||
                      (windows &&
                        envelope.source === 'service-platform' &&
                        envelope.name === 'platform.resource.changed') ||
                      false ||
                      (windows &&
                        envelope.source === 'service-product' &&
                        envelope.name === 'product.resource.changed') ||
                      (windows &&
                        envelope.source === 'service-customer' &&
                        envelope.name === 'customer.resource.changed') ||
                      (windows &&
                        envelope.source === 'service-supplier' &&
                        envelope.name === 'supplier.supplier.changed') ||
                      false ||
                      false ||
                      (windows &&
                        envelope.source === 'service-connect' &&
                        [
                          'connect.mail-account.changed',
                          'connect.settings.changed',
                          'connect.template.changed',
                          'connect.meta.changed',
                          'connect.mailbox.changed',
                          'connect.meta-conversation.changed',
                        ].includes(envelope.name)) ||
                      (windows &&
                        envelope.source === 'service-directory' &&
                        envelope.name === 'directory.resource.changed') ||
                      (windows &&
                        envelope.source === 'service-intelligence' &&
                        ['intelligence.run.changed', 'intelligence.resource.changed'].includes(
                          envelope.name,
                        )) ||
                      (billing &&
                        envelope.source === 'service-billing' &&
                        billingNames.includes(envelope.name)) ||
                      (envelope.source === 'service-documents' &&
                        [
                          'documents.entry.changed',
                          'documents.repository.changed',
                          'documents.settings.changed',
                        ].includes(envelope.name))
                    ) ||
                    envelope.version !== 1
                  )
                    throw new Error('Invalid document event');
                } catch {
                  logger.warn?.({
                    component : 'documents-realtime',
                    event     : 'invalid_event',
                  });
                  try {
                    consumer.ack(message);
                  } catch {
                    /* La recuperación reconstruye las vistas. */
                  }
                  return;
                }
                try {
                  if (envelope.source === 'service-platform') {
                    const change = eventDtos[envelope.name].input(envelope.payload);
                    await windows.changed('platform.' + change.resource, change);
                  } else if (envelope.source === 'service-security') {
                    const change = eventDtos[envelope.name].input(envelope.payload);
                    if (access && envelope.name === 'security.access.changed') await access(change);
                    if (windows) {
                      {
                      }
                      const topics = new Set(['security.' + change.resource]);
                      if (change.resource === 'user') topics.add('security.directory');
                      if (change.resource === 'grant') topics.add('security.connection');
                      if (!access && envelope.name === 'security.access.changed')
                        topics.add('security.access');
                      if (['user', 'workspace'].includes(change.resource))
                        topics.add('security.session');
                      if (
                        envelope.name === 'security.resource.changed' &&
                        change.resource === 'user'
                      )
                        await windows.changed('platform.notification', {
                          ...change,
                          source : 'security',
                        });
                      if (['user', 'session'].includes(change.resource))
                        topics.add('security.profile');
                      for (const topic of topics) await windows.changed(topic, change);
                      if (['user', 'workspace'].includes(change.resource)) {
                      }
                      if (['user', 'workspace'].includes(change.resource))
                        await windows.changed('customer.pipeline', {
                          ...change,
                          source : 'security',
                        });
                    }
                  } else if (envelope.source === 'service-communication') {
                    const change = eventDtos[envelope.name].input(envelope.payload);
                    await windows.changed('connect.meta-conversation', {
                      ...change,
                      source : 'communication',
                    });
                  } else if (
                    envelope.source === 'service-connect' &&
                    [
                      'connect.mail-account.changed',
                      'connect.settings.changed',
                      'connect.template.changed',
                      'connect.meta.changed',
                      'connect.mailbox.changed',
                      'connect.meta-conversation.changed',
                    ].includes(envelope.name)
                  ) {
                    const change = eventDtos[envelope.name].input(envelope.payload);
                    if (envelope.name === 'connect.meta.changed')
                      await windows.changed('connect.meta', change);
                    else if (envelope.name === 'connect.template.changed')
                      await windows.changed('connect.template', change);
                    else if (envelope.name === 'connect.meta-conversation.changed')
                      await windows.changed('connect.meta-conversation', change);
                    else {
                      if (envelope.name !== 'connect.mailbox.changed')
                        await windows.changed('connect.mail-account', change);
                      if (envelope.name === 'connect.settings.changed')
                        await windows.changed('connect.settings', change);
                      else await windows.changed('connect.mailbox', change);
                    }
                  } else if (envelope.source === 'service-directory') {
                    const change = eventDtos[envelope.name].input(envelope.payload);
                    if (change.resource === 'contact')
                      await documents.resourceChanged?.(change, envelope.source);
                    if (change.resource === 'contact') {
                    }
                    if (change.resource === 'contact') {
                    }
                    if (change.resource === 'contact')
                      for (const topic of ['customer.customer', 'customer.pipeline'])
                        await windows.changed(topic, {
                          ...change,
                          source : 'directory',
                        });
                    if (change.resource === 'contact')
                      await windows.changed('security.directory', {
                        ...change,
                        source : 'directory',
                      });
                    if (change.resource === 'contact')
                      await windows.changed('supplier.supplier', {
                        ...change,
                        source : 'directory',
                      });
                    const topics = new Set(['directory.' + change.resource]);
                    if (['role', 'department', 'job'].includes(change.resource))
                      topics.add('directory.contact');
                    if (change.resource === 'settings') topics.add('directory.visibility');
                    for (const topic of topics) await windows.changed(topic, change);
                    if (change.resource === 'contact')
                      await windows.changed('platform.notification', {
                        ...change,
                        source : 'directory',
                      });
                    if (['contact', 'role', 'department', 'job'].includes(change.resource))
                      for (const topic of ['security.user', 'security.session', 'security.profile'])
                        await windows.changed(topic, {
                          ...change,
                          source : 'directory',
                        });
                  } else if (envelope.name === 'customer.resource.changed') {
                    const change = eventDtos[envelope.name].input(envelope.payload);
                    if (change.resource === 'customer')
                      await documents.resourceChanged?.(change, envelope.source);
                    const topics = new Set(['customer.' + change.resource]);
                    if (
                      ['customer', 'pipeline', 'pipeline-type', 'opportunity'].includes(
                        change.resource,
                      )
                    ) {
                      topics.add('customer.pipeline');
                      topics.add('customer.opportunity');
                      topics.add('customer.activity');
                    }
                    if (windows) {
                      for (const topic of topics) await windows.changed(topic, change);
                      if (change.resource === 'customer')
                        await windows.changed('product.assignment', {
                          ...change,
                          source : 'customer',
                        });
                    }
                    if (windows && change.resource === 'customer') {
                    }
                  } else if (windows && envelope.source === 'service-product') {
                    const change = eventDtos[envelope.name].input(envelope.payload);
                    const topics = new Set(['product.' + change.resource]);
                    if (['sku', 'price', 'price-list'].includes(change.resource)) {
                      topics.add('product.price');
                      topics.add('product.item');
                    }
                    for (const topic of topics) await windows.changed(topic, change);
                  } else if (envelope.source === 'service-supplier') {
                    await documents.resourceChanged?.(
                      eventDtos[envelope.name].input(envelope.payload),
                      envelope.source,
                    );
                    if (windows)
                      await windows.changed(
                        'supplier.supplier',
                        eventDtos[envelope.name].input(envelope.payload),
                      );
                    if (windows) {
                    }
                  } else if (envelope.source === 'service-intelligence') {
                    const change = eventDtos[envelope.name].input(envelope.payload);
                    await windows.changed(
                      envelope.name === 'intelligence.run.changed'
                        ? 'intelligence.run'
                        : 'intelligence.' + change.resource,
                      change,
                    );
                  } else if (envelope.source === 'service-billing') {
                    await billing.changed(
                      {
                        originService : envelope.source,
                        data          : eventDtos[envelope.name].input(envelope.payload),
                      },
                      envelope.name.slice('billing.'.length, -'.changed'.length),
                    );
                  } else if (envelope.source === 'service-documents') {
                    if (envelope.name === 'documents.entry.changed')
                      await documents.changed(envelope.payload, envelope.source);
                    else if (windows) {
                      const change = eventDtos[envelope.name].input(envelope.payload);
                      if (!change.parameter)
                        await documents.repositoryChanged?.(change, envelope.source);
                      for (const topic of change.parameter
                        ? ['documents.settings']
                        : ['documents.repository', 'documents.entry', 'documents.view'])
                        await windows.changed(topic, change);
                    }
                  } else {
                  }
                  consumer.ack(message);
                } catch (error) {
                  if ([400, 403].includes(error.statusCode)) {
                    try {
                      consumer.ack(message);
                    } catch {
                      /* El proceso perderá su cola al cerrar. */
                    }
                  } else {
                    await new Promise(
                      /**
                       * Coordina la resolución o el rechazo de la operación asíncrona.
                       * @param {Function} resolve Continuación que confirma el resultado de la promesa.
                       * @returns {void} Finaliza después de aplicar los efectos de la operación.
                       */
                      (resolve) => {
                        setTimeout(resolve, 1000);
                      },
                    );
                    try {
                      consumer.nack(message, false, true);
                    } catch {
                      /* La reconexión exige recuperar por Gateway las ventanas sin continuidad. */
                    }
                  }
                }
              }
            )();
          },
          { noAck: false },
        );
        if (!started || connection !== candidate) {
          await candidate.close().catch(
            /**
             * Gestiona el fallo de la operación asíncrona y conserva la política de error del módulo.
             * @returns {void} Finaliza después de aplicar los efectos de la operación.
             */
            () => {},
          );
          return;
        }
        channel = consumer;
        await documents.recover();
      }
    )()
      .catch(
        /**
         * Gestiona el fallo de la operación asíncrona y conserva la política de error del módulo.
         * @param {Error} error Fallo recibido por la continuación.
         * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
         */
        async (error) => {
          const candidate = connection;
          if (candidate) {
            lost(candidate);
            await candidate.close().catch(
              /**
               * Gestiona el fallo de la operación asíncrona y conserva la política de error del módulo.
               * @returns {void} Finaliza después de aplicar los efectos de la operación.
               */
              () => {},
            );
          } else if (started && !retry) {
            retry = setTimeout(
              /**
               * Ejecuta la continuación cuando vence el intervalo configurado.
               * @returns {void} Finaliza después de aplicar los efectos de la operación.
               */
              () => {
                retry = undefined;
                void open().catch(
                  /**
                   * Gestiona el fallo de la operación asíncrona y conserva la política de error del módulo.
                   * @returns {void} Finaliza después de aplicar los efectos de la operación.
                   */
                  () => {},
                );
              },
              1000,
            );
            retry.unref?.();
          }
          throw error;
        },
      )
      .finally(
        /**
         * Libera o restablece los recursos al terminar la operación.
         * @returns {void} Finaliza después de aplicar los efectos de la operación.
         */
        () => {
          opening = undefined;
        },
      );
    opening = work;
    return work;
  }
  return {
    /**
     * Inicia el componente y sus colaboradores.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async start() {
      if (started) return opening;
      started = true;
      try {
        await open();
      } catch (error) {
        started = false;
        clearTimeout(retry);
        retry = undefined;
        throw error;
      }
    },
    /**
     * Ejecuta ready dentro del flujo de platform-events.
     * @returns {boolean} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    ready() {
      return !!channel;
    },
    /**
     * Cierra el componente y libera sus recursos.
     * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
     */
    async close() {
      started = false;
      closing = true;
      clearTimeout(retry);
      retry = undefined;
      const candidate = connection;
      connection = undefined;
      channel = undefined;
      await candidate?.close().catch(
        /**
         * Gestiona el fallo de la operación asíncrona y conserva la política de error del módulo.
         * @returns {void} Finaliza después de aplicar los efectos de la operación.
         */
        () => {},
      );
      await opening?.catch(
        /**
         * Gestiona el fallo de la operación asíncrona y conserva la política de error del módulo.
         * @returns {void} Finaliza después de aplicar los efectos de la operación.
         */
        () => {},
      );
    },
  };
}
module.exports = {
  createPlatformEvents,
  windowEventNames : Object.freeze([
    'platform.resource.changed',
    'product.resource.changed',
    'customer.resource.changed',
    'supplier.supplier.changed',
    ...billingNames,
    'directory.resource.changed',
    'intelligence.run.changed',
    'intelligence.resource.changed',
    'connect.mail-account.changed',
    'connect.settings.changed',
    'connect.template.changed',
    'connect.meta.changed',
    'connect.mailbox.changed',
    'connect.meta-conversation.changed',
    'communication.chat.changed',
    'security.access.changed',
    'security.resource.changed',
  ]),
};
