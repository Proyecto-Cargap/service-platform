/** @file Impide reintroducir el aviso de Product que provocaba recargas generales. */
'use strict';
const test   = require('node:test');
const assert = require('node:assert/strict');
const dtos   = require('../server/contracts/registry');
test('Product sólo conserva ventanas estándar, sin aviso histórico ni permiso de difusión' /** @returns {void} Verifica la retirada del contrato y del canal obsoletos. */, () => {
  assert.equal(dtos['product.item.changed'], undefined);
});
