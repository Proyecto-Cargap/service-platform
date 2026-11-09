/** @file DTO de entrada y salida de las acciones de configuration; reutilizan los esquemas vigentes del servicio. */
'use strict';
const { createActionDtos }    = require('../../../contracts/action-dto');
const configurationActionDtos = createActionDtos([
  'configuration-get-one',
  'configuration-get-information',
  'configuration-update',
]);
module.exports = configurationActionDtos;
