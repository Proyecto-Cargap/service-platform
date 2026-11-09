/** @file Repositorios completos: aislamiento por conexión, deduplicación, renovación y recuperación. */
'use strict';
const test                     = require('node:test');
const assert                   = require('node:assert/strict');
const { repositoryWatches }    = require('../server/adapters/socket/documents-repositories-realtime');
const { reader, readIdentity } = require('./helpers/repository-read.cjs');
const A = 'aaaaaaaaaaaaaaaaaaaaaaaa',
  B     = 'bbbbbbbbbbbbbbbbbbbbbbbb';
/** @param {number} [maxPending] Límite de cola para ejercitar desbordamientos. @returns {Object} Dos espacios y transporte observable. */
function fixture(maxPending = 128) {
  const state   = {
    revoked : false,
    calls   : 0,
  };
  const reads   = reader();
  const service = repositoryWatches({
    /** @returns {Object} Lector real con almacenamiento aislado. */ reads : () => reads,
    maxPending,
    messaging                                                              : {
      /** @param {string} service Destino. @param {string} action Acción. @param {Object} data Entrada. @param {Object} options Autoridad. @returns {Promise<Object>} Repositorios autorizados. */ async request(
        service,
        action,
        data,
        options,
      ) {
        state.calls++;
        if (state.gate) await state.gate;
        if (action === 'documents-entry-get-one')
          return {
            statusCode : 200,
            data       : {
              item : {
                id           : data.entryId,
                repositoryId : state.target || A,
                revision     : 5,
                name         : 'archivo',
                parentId     : null,
              },
            },
          };
        assert.equal(action, 'documents-repositories-get-list');
        return {
          statusCode : 200,
          data       : {
            listItem : (state.revoked ? [] : options.context.workspace === 'W1' ? [A, B] : [B]).map(
              /** @param {string} id Raíz. @returns {Object} Proyección. */ (id) => ({
                id,
                revision     : 7,
                capabilities : { view: true },
              }),
            ),
          },
        };
      },
    },
  });
  /** @param {string} id Conexión. @param {string} workspace Espacio. @returns {Object} Socket. */
  function socket(id, workspace) {
    const messages = [];
    const value    = {
      id,
      messages,
      data : {
        identity : {
          userCode        : 'U1',
          activeWorkspace : workspace,
          actorType       : 'documents-delegated',
          delegation      : {
            grantCode : 'G1',
            clientId  : 'C1',
          },
        },
      },
      /** @param {string} channel Canal. @param {Object} message Evento. @returns {void} Captura la entrega. */ emit(
        channel,
        message,
      ) {
        assert.equal(channel, 'realtime');
        messages.push(message);
      },
    };
    service.authenticate(
      value,
      /** @returns {Promise<Object>} Identidad vigente. */ async () => ({
        userCode        : 'U1',
        activeWorkspace : workspace,
        policies        : [],
        workspaces      : [workspace],
        delegation      : {
          grantCode : 'G1',
          clientId  : 'C1',
        },
      }),
    );
    return value;
  }
  return {
    service,
    state,
    socket,
    /** @param {Object} socket Conexión. @param {Object} value Suscripción. @returns {Promise<Object>} ACK, lectura HTTP y bind sin listas por socket. */
    async watch(socket, value) {
      const ready = await service.watch(socket, value);
      assert.equal(ready.data, undefined);
      const request = {
        ...value,
        receiptId : ready.receiptId,
        query     : { repositoryId: value.repositoryId },
      };
      await reads.read(request, readIdentity(socket.data.identity));
      await service.bind(socket, request);
      return ready;
    },
  };
}
test('varios repositorios y espacios reciben cambios fuera de la página, movimientos y secuencias independientes', /** @returns {Promise<void>} Acredita ámbitos y deduplicación. */ async () => {
  const f = fixture(),
    one   = f.socket('one', 'W1'),
    two   = f.socket('two', 'W2');
  await f.watch(one, {
    subscriptionId : 'a',
    repositoryId   : A,
    watchVersion   : 1,
  });
  await f.watch(one, {
    subscriptionId : 'b',
    repositoryId   : B,
    watchVersion   : 1,
  });
  await f.watch(two, {
    subscriptionId : 'b',
    repositoryId   : B,
    watchVersion   : 1,
  });
  await assert.rejects(
    f.watch(two, {
      subscriptionId : 'bad',
      repositoryId   : A,
      watchVersion   : 1,
    }),
    { statusCode: 403 },
  );
  const event = {
    entryId                    : 'eeeeeeeeeeeeeeeeeeeeeeee',
    repositoryId               : B,
    previousRepositoryId       : A,
    operation                  : 'move',
    revision                   : 5,
    repositoryRevision         : 7,
    previousRepositoryRevision : 6,
    changeCode                 : 'move-1',
  };
  f.state.target = B;
  await f.service.changed(event);
  await f.service.changed(event);
  assert.equal(
    one.messages.filter(
      /** @param {Object} value Evento. @returns {boolean} Invalidación. */ (value) =>
        value.type === 'patch',
    ).length,
    2,
  );
  assert.equal(two.messages.at(-1).delivery, 1);
  assert.equal(two.messages.at(-1).data.entryId, event.entryId);
  f.service.unwatch(one.id, {
    subscriptionId : 'a',
    watchVersion   : 1,
  });
  await f.service.changed({
    ...event,
    changeCode : 'move-2',
  });
  assert.equal(one.messages.at(-1).subscriptionId, 'b');
  f.service.close();
});
test('los cambios de acceso actualizan capacidades y revocan concesiones sin recargar manifiestos', /** @returns {Promise<void>} El control causal queda dentro del canal estándar de la ventana afectada. */ async () => {
  const f  = fixture(),
    socket = f.socket('one', 'W1');
  await f.watch(socket, {
    subscriptionId : 'a',
    repositoryId   : A,
    watchVersion   : 1,
  });
  const calls = f.state.calls;
  await f.service.accessChanged({
    resource : 'user',
    code     : 'other',
    revision : 2,
  });
  assert.equal(f.state.calls, calls);
  await f.service.accessChanged({
    resource : 'group',
    code     : 'g',
    revision : 2,
  });
  assert.equal(socket.messages.at(-1).type, 'patch');
  assert.equal(socket.messages.at(-1).data.metadataOnly, true);
  assert.equal(socket.messages.at(-1).data.cursor, 7);
  assert.deepEqual(socket.messages.at(-1).data.upserts, []);
  f.state.revoked = true;
  await f.service.accessChanged({
    resource : 'grant',
    code     : 'G1',
    revision : 3,
  });
  assert.equal(socket.messages.at(-1).type, 'revoked');
  assert.equal(
    socket.messages.some(
      /** @param {Object} event Entrega. @returns {boolean} Detecta reconciliaciones de inventario indebidas. */ (
        event,
      ) => event.type === 'resync-required',
    ),
    false,
  );
  f.service.close();
});
test('no consulta en reposo; indisponibilidad, control y recuperación no inventan continuidad', /** @returns {Promise<void>} Preserva interrupción hasta recuperación explícita. */ async () => {
  const f  = fixture(),
    socket = f.socket('one', 'W1');
  await f.watch(socket, {
    subscriptionId : 'a',
    repositoryId   : A,
    watchVersion   : 1,
  });
  const calls = f.state.calls;
  await new Promise(
    /** @param {Function} resolve Continuación. @returns {*} Espera acotada de prueba. */ (
      resolve,
    ) => setTimeout(resolve, 50),
  );
  assert.equal(f.state.calls, calls);
  f.service.unavailable();
  await f.service.control();
  await assert.rejects(
    f.service.changed({
      repositoryId : A,
      entryId      : B,
      changeCode   : '1',
      operation    : 'replace',
    }),
    { statusCode: 503 },
  );
  await f.service.recover();
  assert.equal(socket.messages.at(-1).type, 'resync-required');
  f.state.revoked = true;
  await f.service.changed({
    repositoryId : A,
    entryId      : B,
    changeCode   : '2',
    operation    : 'replace',
  });
  assert.equal(socket.messages.at(-1).type, 'revoked');
  const length = socket.messages.length;
  await f.service.changed({
    repositoryId : A,
    changeCode   : '3',
  });
  assert.equal(socket.messages.length, length);
  f.service.close();
});
test('renovación de identidad estable mantiene suscripciones y watchVersion rechaza antigüedad', /** @returns {Promise<void>} No reinicia una secuencia acreditada. */ async () => {
  const f  = fixture(),
    socket = f.socket('one', 'W1');
  await f.watch(socket, {
    subscriptionId : 'a',
    repositoryId   : A,
    watchVersion   : 2,
  });
  f.service.authenticate(
    socket,
    /** @returns {Promise<Object>} Misma identidad renovada. */ async () => ({
      userCode        : 'U1',
      activeWorkspace : 'W1',
      policies        : [],
      workspaces      : ['W1'],
      delegation      : {
        grantCode : 'G1',
        clientId  : 'C1',
      },
    }),
  );
  await f.service.changed({
    repositoryId : A,
    entryId      : B,
    changeCode   : '1',
    operation    : 'replace',
  });
  assert.equal(socket.messages.at(-1).delivery, 1);
  await assert.rejects(
    f.watch(socket, {
      subscriptionId : 'a',
      repositoryId   : A,
      watchVersion   : 1,
    }),
    { statusCode: 409 },
  );
  f.service.reset(socket.id);
  await f.service.changed({
    repositoryId : A,
    changeCode   : '2',
  });
  assert.equal(socket.messages.length, 1);
  f.service.close();
});

/** @returns {Promise<void>} El desbordamiento descarta la ráfaga pendiente con una sola recuperación y libera capacidad. */
async function queueOverflow() {
  const f  = fixture(3),
    socket = f.socket('one', 'W1');
  try {
    await f.watch(socket, {
      subscriptionId : 'a',
      repositoryId   : A,
      watchVersion   : 1,
    });
    let release;
    f.state.gate = new Promise(
      /** @param {Function} resolve Barrera. @returns {void} Retiene una lectura de autorización. */
      (resolve) => {
        release = resolve;
      },
    );
    const pending = [];
    for (let index = 0; index < 20; index++)
      pending.push(
        f.service.changed({
          repositoryId       : A,
          entryId            : B,
          changeCode         : `change-${index}`,
          operation          : 'replace',
          revision           : 5,
          repositoryRevision : 7,
        }),
      );
    release();
    f.state.gate = null;
    await Promise.all(pending);
    assert.equal(
      socket.messages.filter(
        /** @param {Object} message Control. @returns {boolean} Sólo una recuperación por desbordamiento. */
        (message) => message.type === 'resync-required',
      ).length,
      1,
    );
    assert.ok(f.state.calls < 10, 'la cola no ejecuta las veinte lecturas descartadas');
    await f.service.changed({
      repositoryId       : A,
      entryId            : B,
      changeCode         : 'after',
      operation          : 'replace',
      revision           : 5,
      repositoryRevision : 8,
    });
    assert.equal(socket.messages.at(-1).type, 'patch');
  } finally {
    f.service.close();
  }
}
test(
  'una ráfaga desbordada pide recuperación puntual y acota las lecturas del servidor',
  queueOverflow,
);

test('un hecho de raíz actualiza sólo sus metadatos y no solicita manifiestos nuevos', /** @returns {Promise<void>} La otra raíz conserva su secuencia y la baja revoca la afectada. */ async () => {
  const f  = fixture(),
    socket = f.socket('metadata', 'W1');
  await f.watch(socket, {
    subscriptionId : 'a',
    repositoryId   : A,
    watchVersion   : 1,
  });
  await f.watch(socket, {
    subscriptionId : 'b',
    repositoryId   : B,
    watchVersion   : 1,
  });
  await f.service.control(A);
  await f.service.control(A);
  assert.equal(socket.messages.length, 1);
  assert.equal(socket.messages[0].subscriptionId, 'a');
  assert.equal(socket.messages[0].type, 'patch');
  assert.equal(socket.messages[0].data.metadataOnly, true);
  assert.equal(socket.messages[0].data.repository.revision, 7);
  assert.deepEqual(socket.messages[0].data.upserts, []);
  f.state.revoked = true;
  await f.service.control(A);
  assert.equal(socket.messages.at(-1).type, 'revoked');
  assert.equal(socket.messages.at(-1).subscriptionId, 'a');
  f.service.close();
});
