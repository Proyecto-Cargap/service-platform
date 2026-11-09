/** @file Registro completo de DTO por dominio; cada acción tiene entrada y salida explícitas. */
'use strict';
module.exports = Object.freeze(
  Object.assign(
    Object.create(null),
    require('../modules/timeline/dtos/timeline.actions.dto'),
    require('../modules/realtime/dtos/realtime.actions.dto'),
    require('../modules/configuration/dtos/configuration.actions.dto'),
    require('../modules/instance/dtos/instance.actions.dto'),
    require('../modules/notifications/dtos/notifications.actions.dto'),
    require('../modules/push/dtos/push.actions.dto'),
    require('../modules/release/dtos/release.actions.dto'),
    require('../modules/desktop-release/dtos/desktop-release.actions.dto'),
    require('../modules/settings/dtos/settings.actions.dto'),
    require('../modules/system/dtos/system.actions.dto'),
  ),
);
