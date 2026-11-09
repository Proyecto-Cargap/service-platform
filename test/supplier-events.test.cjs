/** @file Contrato de hechos Supplier y retirada del canal sustituido. */
'use strict';
const test   = require('node:test');
const assert = require('node:assert/strict');
const dtos   = require('../server/contracts/registry');
const item   = {
  supplierCode : 'SUP1',
  erased       : false,
  revision     : 2,
  workspace    : 'W1',
  userCreate   : 'OWNER',
  userAssigned : 'ASSIGNED',
};

test('El DTO rechaza datos fiscales, revisiones inválidas y avisos incompletos' /** @returns {void} El contrato admite únicamente la invalidación mínima acordada. */, () => {
  const dto = dtos['supplier.supplier.changed'];
  assert.deepEqual(dto.input({ item }), { item });
  for (const value of [
    {
      item : {
        ...item,
        total : '100',
      },
    },
    {
      item : {
        ...item,
        revision : 0,
      },
    },
    {
      item : {
        ...item,
        workspace : 1,
      },
    },
    { item: { supplierCode: 'SUP1' } },
  ])
    assert.throws(
      /** @returns {Object} Entrada inválida acreditada por el DTO real. */
      () => dto.input(value),
    );
});
