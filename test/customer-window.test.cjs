/** @file Contrato CRM estándar: lecturas canónicas, columnas independientes y autorización del productor. */
'use strict';
const test = require('node:test'),
  assert   = require('node:assert/strict');
const { producerWindows } = require('./helpers/producer-window.cjs');
const { customerWindows } = require('../server/modules/realtime/services/customer-window.service');
const { windowDelta }     = require('../server/library/realtime/window-delta');
/** @returns {Object} Contexto sintético acreditado por el transporte. */
function identity() {
  return {
    userCode  : 'u',
    workspace : 'w',
    context   : {
      user      : { userCode: 'u' },
      workspace : 'w',
      policies  : ['customer.customer.view.workspace'],
    },
  };
}
test('CRM registra quince secciones cerradas; las páginas y columnas proceden de HTTP con recibo del productor', /** @returns {Promise<void>} Contratos, identidad y ausencia de snapshots por socket. */ async () => {
  const calls  = [];
  let response = {
    statusCode : 200,
    data       : {
      items      : [],
      pagination : {
        page    : 2,
        perPage : 20,
        total   : 0,
      },
    },
  };
  const topics = producerWindows(customerWindows, {
    messaging : {
      /** @param {...*} args Consulta canónica. @returns {Promise<Object>} Sobre acreditado por el helper de transporte. */
      async request(...args) {
        calls.push(args);
        return response;
      },
    },
  });
  assert.equal(
    Object.values(topics).reduce(
      /** @param {number} sum Acumulado. @param {Object} sections Secciones. @returns {number} Total. */ (
        sum,
        sections,
      ) => sum + Object.keys(sections).length,
      0,
    ),
    15,
  );
  const spec = topics['customer.customer'].page,
    actor    = identity(),
    query    = {
      page      : 2,
      perPage   : 20,
      workspace : 'w',
      sort      : 'name',
    };
  assert.deepEqual((await spec.read(spec.validate(query), actor)).data, response.data);
  assert.deepEqual(calls[0], [
    'service-customer',
    'customer-get-list',
    query,
    { context: actor.context },
  ]);
  for (const bad of [
    { page: 1 },
    {
      page    : 1,
      perPage : 0,
    },
    {
      page    : 1,
      perPage : 101,
    },
    {
      page      : 1,
      perPage   : 20,
      condition : {},
    },
  ])
    assert.throws(/** @returns {Object} Consulta inválida. */ () => spec.validate(bad), {
      statusCode : 400,
    });
  const board = topics['customer.pipeline'].board;
  assert.throws(
    /** @returns {Object} Dos columnas no comparten suscripción. */ () =>
      board.validate({
        pipelineCode : 'p',
        windows      : [
          {
            stageCode : 'a',
            limit     : 20,
          },
          {
            stageCode : 'b',
            limit     : 20,
          },
        ],
      }),
    { statusCode: 400 },
  );
  assert.doesNotThrow(
    /** @returns {Object} Cabecera y agregados explícitos sin tarjetas. */ () =>
      board.validate({
        pipelineCode : 'p',
        windows      : [],
      }),
  );
  response = {
    statusCode : 200,
    data       : {
      pipelineCode     : 'p',
      pipelineTypeCode : 't',
      assignees        : [],
      customers        : [],
      workspace        : 'w',
      revision         : 1,
      pipelineRevision : 1,
      typeRevision     : 1,
      opportunityOrder : {
        field     : 'updateDate',
        direction : 'desc',
      },
      stages           : [],
      windows          : [
        {
          stageCode : 'a',
          limit     : 20,
          items     : [],
        },
      ],
      totals           : [
        {
          stageCode : 'a',
          currency  : 'MXN',
          count     : 2,
          amount    : '20',
        },
        {
          stageCode : 'a',
          currency  : 'USD',
          count     : 1,
          amount    : '10',
        },
        {
          stageCode : 'b',
          currency  : 'MXN',
          count     : 3,
          amount    : '30',
        },
      ],
    },
  };
  const column = await board.read(
    {
      pipelineCode : 'p',
      windows      : [
        {
          stageCode : 'a',
          limit     : 20,
        },
      ],
    },
    actor,
  );
  assert.equal(column.data.totals.length, 2);
  const updated = structuredClone(column.data);
  updated.totals[0].count = 3;
  assert.doesNotThrow(
    /** @returns {Object[]} Dos monedas de la misma etapa no se confunden con identidades duplicadas. */ () =>
      windowDelta(column.data, updated, board.collections),
  );
  response = {
    statusCode      : 403,
    descriptionCode : 'CUSTOMER_PARENT_FORBIDDEN',
  };
  await assert.rejects(spec.read(query, actor), {
    statusCode      : 403,
    descriptionCode : 'CUSTOMER_PARENT_FORBIDDEN',
  });
  response = { statusCode: 404 };
  await assert.rejects(
    topics['customer.opportunity'].detail.read({ opportunityCode: 'o' }, actor),
    { statusCode: 403 },
  );
  response = {
    statusCode : 200,
    data       : {
      items      : [],
      pagination : {
        page    : 1,
        perPage : 20,
        total   : 0,
      },
      secret     : 'private',
    },
  };
  await assert.rejects(spec.read(query, actor), { statusCode: 503 });
});
test('CRM afecta los dos extremos de un movimiento y entrega deltas de tarjeta sin reemplazar la columna completa', /** @returns {void} Dependencias de padres, perfiles y orden permanecen acotadas. */ () => {
  const topics = customerWindows({ messaging: {} }),
    actor      = identity(),
    board      = topics['customer.pipeline'].board;
  const change = {
    resource          : 'opportunity',
    code              : 'o',
    revision          : 2,
    pipelineCode      : 'p',
    workspace         : 'w',
    stageCode         : 'b',
    previousStageCode : 'a',
    erased            : false,
  };
  for (const stage of ['a', 'b'])
    assert.equal(
      board.affects(
        {
          pipelineCode : 'p',
          windows      : [
            {
              stageCode : stage,
              limit     : 20,
            },
          ],
        },
        change,
        actor,
      ),
      true,
    );
  assert.equal(
    board.affects(
      {
        pipelineCode : 'p',
        windows      : [
          {
            stageCode : 'c',
            limit     : 20,
          },
        ],
      },
      change,
      actor,
    ),
    false,
  );
  assert.equal(
    board.affects(
      {
        pipelineCode : 'other',
        windows      : [],
      },
      change,
      actor,
    ),
    false,
  );
  const before = {
    windows : [
      {
        stageCode : 'a',
        limit     : 20,
        items     : [
          {
            opportunityCode : 'o',
            title           : 'Anterior',
            revision        : 1,
          },
          {
            opportunityCode : 'untouched',
            title           : 'Igual',
            revision        : 1,
          },
        ],
      },
    ],
  };
  const after  = structuredClone(before);
  after.windows[0].items[0].title = 'Nuevo';
  after.windows[0].items[0].revision = 2;
  assert.deepEqual(
    windowDelta(before, after, board.collections).map(
      /** @param {Object} op Operación. @returns {string} Ruta puntual. */ (op) => op.path,
    ),
    ['/windows/0/items/0/title', '/windows/0/items/0/revision'],
  );
  const customer = {
    resource  : 'customer',
    code      : 'c',
    workspace : 'w',
    revision  : 2,
    erased    : false,
  };
  assert.equal(
    topics['customer.opportunity'].detail.affects({ opportunityCode: 'o' }, customer, actor, {
      customerCode : 'c',
    }),
    true,
  );
  assert.equal(
    topics['customer.opportunity'].detail.affects({ opportunityCode: 'o' }, customer, actor, {
      customerCode : 'other',
    }),
    false,
  );
  assert.equal(
    topics['customer.customer'].page.affects(
      { sort: 'name' },
      {
        source   : 'directory',
        resource : 'contact',
        code     : 'not-yet-visible',
      },
      actor,
      { items: [] },
    ),
    true,
  );
  assert.equal(
    topics['customer.pipeline'].assignees.affects(
      {
        pipelineCode : 'p',
        codes        : ['u'],
      },
      {
        source   : 'security',
        resource : 'user',
        code     : 'other',
      },
      actor,
    ),
    false,
  );
  assert.equal(
    topics['customer.settings'].detail.affects(
      {},
      {
        resource  : 'settings',
        workspace : 'foreign',
      },
      actor,
    ),
    false,
  );
});

test('el perfil de responsable publica un patch de revisión aunque la fotografía conserve su URL', /** @returns {Promise<void>} La versión procede de Directory vía Security y Customer, sin relojes del cliente. */ async () => {
  let revision = 7;
  const topics = producerWindows(customerWindows, {
    messaging : {
      /** @returns {Promise<Object>} Proyección canónica mínima del responsable. */
      async request() {
        return {
          statusCode : 200,
          data       : {
            items : [
              {
                userCode        : 'u',
                name            : 'Nombre',
                pictureUrl      : '/customer/pipeline/p/assignees/u/picture',
                pictureRevision : revision,
              },
            ],
          },
        };
      },
    },
  });
  const spec = topics['customer.pipeline'].assignees,
    query    = {
      pipelineCode : 'p',
      codes        : ['u'],
    };
  const before = await spec.read(query, identity());
  revision++;
  const after = await spec.read(query, identity());
  assert.deepEqual(windowDelta(before.data, after.data, spec.collections), [
    {
      op    : 'replace',
      path  : '/items/0/pictureRevision',
      value : 8,
    },
  ]);
});
