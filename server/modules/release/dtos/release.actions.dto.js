/** @file DTO de entrada y salida de las acciones de release; reutilizan los esquemas vigentes del servicio. */
'use strict';
const { createActionDtos } = require('../../../contracts/action-dto');
const releaseActionDtos    = createActionDtos([
  'release-create',
  'release-current',
  'release-web-build',
  'release-history',
  'release-get-list',
  'release-get-one',
  'release-update',
  'release-delete',
  'release-published',
]);
module.exports = releaseActionDtos;
