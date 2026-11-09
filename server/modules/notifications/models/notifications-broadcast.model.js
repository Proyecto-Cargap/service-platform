/**
 * @file Persistencia explícita de trabajos y destinatarios de notificaciones.
 */
'use strict';
const mongoose       = require('mongoose');
const { randomUUID } = require('node:crypto');
const schema         = new mongoose.Schema(
  {
    /**
     * Código estable de la preparación y sus reintentos.
     */
    broadcastCode     : {
      type     : String,
      default  : undefined,
      required : true,
    },
    /**
     * Espacio destinatario de la difusión.
     */
    workspace         : {
      type     : String,
      default  : undefined,
      required : true,
    },
    /**
     * Autor autenticado al preparar la difusión.
     */
    userCreate        : {
      type      : String,
      default   : undefined,
      required  : true,
      immutable : true,
    },
    /**
     * Metadato canónico sin destinatario individual.
     */
    userAssigned      : {
      type     : String,
      default  : '',
      required : false,
    },
    /**
     * Título visible de la difusión.
     */
    title             : {
      type     : String,
      default  : undefined,
      required : true,
    },
    /**
     * Contenido de texto plano compartido.
     */
    content           : {
      type     : String,
      default  : undefined,
      required : true,
    },
    /**
     * Tipo visual de la notificación.
     */
    type              : {
      type     : String,
      default  : 'info',
      required : false,
    },
    /**
     * Ruta interna opcional validada por la aplicación.
     */
    link              : {
      type     : mongoose.Schema.Types.Mixed,
      default  : null,
      required : false,
    },
    /**
     * Preparación, ejecución o resultado persistido.
     */
    state             : {
      type     : String,
      default  : 'prepared',
      required : true,
    },
    /**
     * Audiencia completa que el administrador confirma.
     */
    audience          : {
      type     : [String],
      default  : [],
      required : false,
    },
    /**
     * Huella de la audiencia para detectar cambios al confirmar.
     */
    audienceHash      : {
      type     : String,
      default  : undefined,
      required : true,
    },
    /**
     * Token de la audiencia mostrada; rota cuando se requiere nueva confirmación humana.
     */
    confirmationToken : {
      type     : String,
      default  : randomUUID,
      required : true,
    },
    /**
     * Cantidad confirmada de destinatarios únicos.
     */
    recipientCount    : {
      type     : Number,
      default  : 0,
      required : true,
    },
    /**
     * Notificaciones persistidas; no representa recepción del navegador.
     */
    persistedCount    : {
      type     : Number,
      default  : 0,
      required : true,
    },
    /**
     * Destinatarios que dejaron de cumplir pertenencia o vigencia.
     */
    skippedCount      : {
      type     : Number,
      default  : 0,
      required : true,
    },
    /**
     * Destinatarios con fallo cierto después de agotar reintentos.
     */
    failedCount       : {
      type     : Number,
      default  : 0,
      required : true,
    },
    /**
     * Destinatarios que aún no tienen resultado terminal.
     */
    pendingCount      : {
      type     : Number,
      default  : 0,
      required : true,
    },
    /**
     * Fecha de preparación en milisegundos.
     */
    preparedAt        : {
      type     : Number,
      default  : Date.now,
      required : true,
    },
    /**
     * Fecha de aceptación durable en milisegundos.
     */
    confirmedAt       : {
      type     : Number,
      default  : 0,
      required : true,
    },
    /**
     * Fecha del resultado terminal en milisegundos.
     */
    completedAt       : {
      type     : Number,
      default  : 0,
      required : true,
    },
    /**
     * Token que cerca al proceso propietario del trabajo.
     */
    leaseToken        : {
      type     : String,
      default  : '',
      required : false,
    },
    /**
     * Vencimiento del lease del propietario en milisegundos.
     */
    leaseUntil        : {
      type     : Number,
      default  : 0,
      required : false,
    },
    /** Vencimiento durable del destinatario pendiente más próximo. */
    availableAt       : {
      type     : Number,
      default  : 0,
      required : true,
    },
    /**
     * Fecha de la última transición persistida.
     */
    updateDate        : {
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
  { broadcastCode: 1 },
  {
    unique : true,
    name   : 'notification_broadcast_code',
  },
);
schema.index({
  state      : 1,
  leaseUntil : 1,
});
module.exports = mongoose.model(
  'PLATFORM_NOTIFICATION_BROADCAST',
  schema,
  'PLATFORM_NOTIFICATION_BROADCAST',
);
