/** @file Carga entorno y telemetría antes de importar los adaptadores de la aplicación. */
'use strict';
require('./shared/runtime-environment').loadEnvironment();
require('./shared/runtime-identity').runtimeIdentity();
const telemetry = require('./telemetry');
telemetry.start();
const { createApplication } = require('./bootstrap');
void createApplication({
  messaging : require('./adapters/messaging/config'),
  telemetry,
}).start();
