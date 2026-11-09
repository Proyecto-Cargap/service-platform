/** @file DTO de entrada y salida de las acciones de instance; reutilizan los esquemas vigentes del servicio. */
'use strict';
const { createActionDtos } = require('../../../contracts/action-dto');
const instanceActionDtos   = createActionDtos([
  'instance-create',
  'instance-get-list',
  'instance-get-one',
  'instance-update',
  'instance-delete',
]);
module.exports = instanceActionDtos;
