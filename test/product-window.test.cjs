/** @file Ventanas Product: contrato cerrado, autorización del productor y composición de matrices. */
'use strict';
const test = require('node:test'),
  assert   = require('node:assert/strict');
const { producerWindows } = require('./helpers/producer-window.cjs');
const { productWindows }  = require('../server/modules/realtime/services/product-window.service');
/** @returns {Object} Principal sintético. */
function identity() {
  return {
    userCode  : 'u',
    workspace : 'w',
    context   : {
      workspace : 'w',
      user      : { userCode: 'u' },
      policies  : [
        'product.category.view.global',
        'product.price.view.global',
        'product.item.view.global',
        'product.assignment.view.workspace',
      ],
    },
  };
}
test('Product registra diecisiete secciones que leen únicamente contratos canónicos y proyecciones públicas', /** @returns {Promise<void>} Páginas explícitas, permisos y cursor emitido por el propietario. */ async () => {
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
  const topics = producerWindows(productWindows, {
    messaging : {
      /** @param {...*} args Consulta. @returns {Promise<Object>} Productor acreditado. */ async request(
        ...args
      ) {
        calls.push(args);
        return response;
      },
    },
  });
  assert.equal(
    Object.values(topics).reduce(
      /** @param {number} sum Total. @param {Object} sections Secciones. @returns {number} Total actualizado. */ (
        sum,
        sections,
      ) => sum + Object.keys(sections).length,
      0,
    ),
    17,
  );
  const spec = topics['product.category'].page,
    actor    = identity(),
    query    = {
      page    : 2,
      perPage : 20,
      search  : 'texto',
    };
  assert.deepEqual((await spec.read(spec.validate(query), actor)).data, response.data);
  assert.deepEqual(calls[0], [
    'service-product',
    'product-category-get-list',
    query,
    { context: actor.context },
  ]);
  for (const query of [
    {
      page    : 1,
      perPage : 0,
    },
    {
      page    : 1,
      perPage : 101,
    },
    { page: 1 },
    {
      page      : 1,
      perPage   : 20,
      workspace : 'other',
    },
    {
      page      : 1,
      perPage   : 20,
      condition : {},
    },
  ])
    assert.throws(/** @returns {Object} Entrada ajena al contrato. */ () => spec.validate(query), {
      statusCode : 400,
    });
  await assert.rejects(
    spec.read(query, {
      ...actor,
      context : { policies: ['product.category.view.workspace'] },
    }),
    { statusCode: 403 },
  );
  response.data.secret = 'PRIVATE';
  await assert.rejects(spec.read(query, actor), { statusCode: 503 });
  response = { statusCode: 404 };
  await assert.rejects(topics['product.category'].detail.read({ code: 'c' }, actor), {
    statusCode : 403,
  });
  response = {
    statusCode : 200,
    data       : {
      priceLists : [],
      items      : [],
      pagination : {
        page    : 1,
        perPage : 20,
        total   : 0,
      },
    },
  };
  await topics['product.price'].matrix.read(
    {
      page    : 1,
      perPage : 20,
    },
    actor,
  );
  assert.equal(calls.at(-1)[1], 'product-price-matrix-get-list');
  assert.deepEqual(topics['product.price'].matrix.collections, {
    '/priceLists' : 'code',
    '/items'      : 'variant.code',
  });
  response = {
    statusCode : 200,
    data       : { assignment: null },
  };
  await topics['product.assignment'].customer.read({ customerCode: 'c' }, actor);
  assert.equal(calls.at(-1)[1], 'product-customer-price-list-get');
});
test('Product limita cambios por recurso, relación, contexto y dependencia de precio', /** @returns {void} Las causas ordinarias no obligan a reconsultar secciones ajenas. */ () => {
  const topics = productWindows({ messaging: {} }),
    actor      = identity();
  const change = {
    resource  : 'sku',
    code      : 's',
    itemCode  : 'i',
    revision  : 2,
    workspace : 'allWorkspaces',
    erased    : false,
  };
  assert.equal(topics['product.sku'].page.affects({ itemCode: 'other' }, change, actor), false);
  assert.equal(topics['product.sku'].page.affects({ itemCode: 'i' }, change, actor), true);
  assert.equal(topics['product.item'].detail.affects({ code: 'i' }, change, actor), false);
  assert.equal(topics['product.item'].prices.affects({ code: 'i' }, change, actor), true);
  assert.equal(topics['product.item'].prices.affects({ code: 'other' }, change, actor), false);
  assert.equal(
    topics['product.price'].page.affects({}, change, actor, { items: [{ skuCode: 'other' }] }),
    false,
  );
  assert.equal(
    topics['product.price'].page.affects({}, change, actor, { items: [{ skuCode: 's' }] }),
    true,
  );
  assert.equal(topics['product.price'].matrix.affects({}, change, actor), true);
  const assignment = {
    ...change,
    resource     : 'assignment',
    workspace    : 'w',
    customerCode : 'c',
  };
  assert.equal(
    topics['product.assignment'].customer.affects({ customerCode: 'c' }, assignment, actor),
    true,
  );
  assert.equal(
    topics['product.assignment'].customer.affects(
      { customerCode: 'c' },
      {
        ...assignment,
        workspace : 'foreign',
      },
      actor,
    ),
    false,
  );
  assert.equal(
    topics['product.assignment'].page.affects(
      { customerCode: 'c' },
      {
        source   : 'customer',
        resource : 'customer',
        code     : 'c',
      },
      actor,
      { items: [] },
    ),
    true,
  );
});
