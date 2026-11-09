/**
 * @file Persistencia explícita de trabajos y destinatarios de notificaciones.
 */
'use strict';
const mongoose = require('mongoose');
const schema   = new mongoose.Schema(
  {
    /**
     * Trabajo al que pertenece el destinatario.
     */
    broadcastCode    : {
      type     : String,
      default  : undefined,
      required : true,
    },
    /**
     * Espacio confirmado del trabajo.
     */
    workspace        : {
      type     : String,
      default  : undefined,
      required : true,
    },
    /**
     * Autor autenticado de la difusión; permanece inmutable después de crear el ledger.
     */
    userCreate       : {
      type      : String,
      default   : undefined,
      required  : true,
      immutable : true,
    },
    /**
     * Destinatario responsable del registro de entrega, igual a userCode.
     */
    userAssigned     : {
      type     : String,
      default  : undefined,
      required : true,
    },
    /**
     * Usuario único destinatario del trabajo.
     */
    userCode         : {
      type     : String,
      default  : undefined,
      required : true,
    },
    /**
     * Código estable que no se recrea después de eliminar la notificación.
     */
    notificationCode : {
      type     : String,
      default  : undefined,
      required : true,
    },
    /**
     * Pendiente, persistido, omitido o fallido.
     */
    state            : {
      type     : String,
      default  : 'pending',
      required : true,
    },
    /**
     * Intentos fallidos; el automático admite tres reintentos.
     */
    attempts         : {
      type     : Number,
      default  : 0,
      required : true,
    },
    /**
     * Instante mínimo para el próximo intento.
     */
    availableAt      : {
      type     : Number,
      default  : 0,
      required : true,
    },
    /**
     * Código técnico estable sin datos sensibles.
     */
    errorCode        : {
      type     : String,
      default  : '',
      required : false,
    },
    /**
     * Fecha de la última transición del destinatario.
     */
    updateDate       : {
      type     : Number,
      default  : Date.now,
      required : true,
    },
  },
  {
    versionKey : false,
    autoIndex  : false,
    strict     : true,
  },
);
schema.index(
  {
    broadcastCode : 1,
    userCode      : 1,
  },
  {
    unique : true,
    name   : 'notification_recipient_unique',
  },
);
schema.index({
  broadcastCode : 1,
  state         : 1,
  availableAt   : 1,
});
module.exports = mongoose.model(
  'PLATFORM_NOTIFICATION_RECIPIENT',
  schema,
  'PLATFORM_NOTIFICATION_RECIPIENT',
);
