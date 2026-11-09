/** @file Contratos compartidos por descubrimiento, validación de entrada y salida del catálogo de instaladores. */
'use strict';
const { createActionDtos } = require('../../../contracts/action-dto');
module.exports = createActionDtos(['desktop-release-current', 'desktop-release-download']);
