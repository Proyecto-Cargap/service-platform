/** @file Meta estándar: base HTTP, cambios puntuales, autorización, carreras y revocación. */
'use strict';
const test = require('node:test'),
  assert   = require('node:assert/strict');
const { metaState, metaWindow } = require('./helpers/meta-window.cjs');
const dtos                      = require('../server/contracts/registry');
test('Meta no lee ni entrega datos al suscribir; aplica sólo hechos propios posteriores a la base' /** @returns {Promise<void>} Usa ambos coordinadores y el registro de dominio reales. */, async () => {
  let state = metaState();
  const f   = metaWindow(/** @returns {Object} Snapshot canónico. */ () => structuredClone(state));
  try {
    const ack = await f.subscribe();
    assert.equal('data' in ack, false);
    assert.equal(f.calls.length, 0);
    assert.equal(f.deliveries.length, 0);
    const base = await f.read(ack);
    assert.deepEqual(base.data, state);
    assert.deepEqual(
      f.calls.map(
        /** @param {Object} call RPC. @returns {string} Operación. */ (call) => call.action,
      ),
      ['chat-get-one', 'meta-conversation-get'],
    );
    state = {
      ...state,
      stateRevision : 1,
      deliveryState : 'read',
    };
    await f.windows.changed('connect.meta-conversation', {
      chatConversationCode : 'OTHER',
      stateRevision        : 1,
    });
    assert.equal(f.calls.length, 2);
    await f.windows.changed('connect.meta-conversation', {
      chatConversationCode : 'CHAT',
      stateRevision        : 1,
    });
    assert.equal(f.deliveries.length, 1);
    assert.equal(f.deliveries[0].channel, 'realtime');
    assert.equal(f.deliveries[0].type, 'patch');
    assert(
      f.deliveries[0].data.operations.some(
        /** @param {Object} op Diferencia. @returns {boolean} Entrega actualizada. */ (op) =>
          op.path === '/deliveryState' && op.value === 'read',
      ),
    );
    await f.windows.changed('connect.meta-conversation', {
      chatConversationCode : 'CHAT',
      stateRevision        : 1,
    });
    assert.equal(f.calls.length, 4);
    assert.equal(f.deliveries.length, 1);
    f.chatStatus(403);
    state = {
      ...state,
      stateRevision : 2,
    };
    await f.windows.changed('connect.meta-conversation', {
      chatConversationCode : 'CHAT',
      stateRevision        : 2,
    });
    assert.equal(f.deliveries.at(-1).type, 'revoked');
    assert.equal(f.calls.at(-1).action, 'chat-get-one');
  } finally {
    f.windows.close();
  }
});
test('un hecho durante HTTP se conserva y el bind aplica la revisión posterior' /** @returns {Promise<void>} La base antigua nunca sobrescribe el cambio concurrente. */, async () => {
  let release,
    started,
    reads = 0,
    state = metaState();
  const entered = new Promise(
    /** @param {Function} resolve Señal. @returns {void} Retiene comienzo. */ (resolve) => {
      started = resolve;
    },
  );
  const hold    = new Promise(
    /** @param {Function} resolve Continuación. @returns {void} Retiene consulta. */ (resolve) => {
      release = resolve;
    },
  );
  const f       = metaWindow(
    /** @returns {Promise<Object>} Captura antes de la escritura concurrente. */ async () => {
      const data = structuredClone(state);
      if (++reads === 1) {
        started();
        await hold;
      }
      return data;
    },
  );
  try {
    const ack     = await f.subscribe();
    const pending = f.read(ack);
    await entered;
    state = {
      ...state,
      stateRevision : 1,
      mode          : 'human',
    };
    await f.windows.changed('connect.meta-conversation', {
      chatConversationCode : 'CHAT',
      stateRevision        : 1,
    });
    release();
    const base = await pending;
    assert.equal(base.data.stateRevision, 0);
    assert.equal(f.deliveries.at(-1).type, 'patch');
    assert.equal(reads, 2);
    assert.equal(f.windows.sessions.get('socket').windows.get('meta').window.base.stateRevision, 1);
  } finally {
    f.windows.close();
  }
});
test('ausencia, contratos cerrados y contexto revocado no se confunden con disponibilidad' /** @returns {Promise<void>} Rechaza campos privados y no consulta un chat sin permiso. */, async () => {
  const f = metaWindow(/** @returns {null} Chat sin vínculo. */ () => null);
  try {
    const ack = await f.subscribe();
    assert.equal((await f.read(ack)).data, null);
    assert.throws(
      /** @returns {Object} Evento inválido. */ () =>
        dtos['connect.meta-conversation.changed'].input({
          chatConversationCode : 'CHAT',
          stateRevision        : 1,
          secret               : 'PRIVATE',
        }),
    );
    f.identity.context.policies = [];
    const before = f.calls.length;
    await f.windows.reauthorize({}, /** @returns {boolean} Único principal afectado. */ () => true);
    assert.equal(f.deliveries.at(-1).type, 'revoked');
    assert.equal(f.calls.length, before);
  } finally {
    f.windows.close();
  }
});
