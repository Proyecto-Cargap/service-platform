/**
 * @file Esquema Mongoose de notificaciones: campos, valores predeterminados y reglas de persistencia.
 * Define la estructura utilizada al crear y consultar documentos de este recurso.
 */
'use strict';
const mongoose = require('mongoose');

const Schema = mongoose.Schema;

const schema = new Schema({
  /** Identidad de un evento interno; su unicidad impide duplicar avisos al reintentar. */
  sourceEventKey   : {
    type    : String,
    default : undefined,
  },
  /**
   * Espacio de trabajo del registro; allWorkspaces identifica recursos compartidos.
   */
  workspace        : {
    type     : String,
    default  : undefined,
    required : true,
  },
  /**
   * Código del usuario autor; permanece inmutable después de crear el registro.
   */
  userCreate       : {
    type      : String,
    default   : undefined,
    required  : false,
    immutable : true,
  },
  /**
   * Código del usuario responsable; vacío cuando no existe una asignación.
   */
  userAssigned     : {
    type     : String,
    default  : undefined,
    required : false,
  },
  /**
   * Código único de negocio que identifica la notificación.
   */
  notificationCode : {
    type     : String,
    default  : undefined,
    required : [true, 'Notification code not found'],
  },
  /**
   * Trabajo de difusión que originó el aviso; ausente para productores individuales existentes.
   */
  broadcastCode    : {
    type     : String,
    default  : undefined,
    required : false,
  },
  /**
   * Código del usuario destinatario de la notificación.
   */
  userCode         : {
    type     : String,
    default  : undefined,
    required : [true, 'User code not found'],
  },
  /**
   * Título que se presenta en la notificación.
   */
  title            : {
    type     : String,
    default  : undefined,
    required : [true, 'Title not found'],
  },
  /**
   * Contenido descriptivo de la notificación.
   */
  content          : {
    type     : String,
    default  : '',
    required : false,
  },
  /**
   * Acción de aplicación asociada a la notificación.
   */
  action           : {
    type     : String,
    default  : '',
    required : false,
  },
  /**
   * Datos del destino de navegación asociado a la notificación.
   */
  link             : {
    /**
     * Ruta de aplicación que recibe la navegación.
     */
    route      : {
      type     : String,
      default  : '',
      required : false,
    },
    /**
     * Identificador del recurso de destino.
     */
    idObject   : {
      type     : String,
      default  : '',
      required : false,
    },
    /**
     * Parámetros adicionales enviados a la ruta de destino.
     */
    parameters : {
      type     : [],
      default  : [],
      required : false,
    },
  },
  /**
   * Clasificación del recurso utilizada por las operaciones de aplicación.
   */
  type             : {
    type     : String,
    default  : '',
    required : false,
  },
  /**
   * Identificación del origen de la notificación.
   */
  from             : {
    type     : String,
    default  : '',
    required : false,
  },
  /**
   * Indica si el destinatario ya consultó la notificación.
   */
  viewed           : {
    type     : Boolean,
    default  : false,
    required : false,
  },
  /**
   * Marca de baja lógica; su valor no altera automáticamente los filtros de lectura.
   */
  erased           : {
    type     : Boolean,
    default  : false,
    required : false,
  },
  /**
   * Instante de la última modificación del registro, expresado en milisegundos.
   */
  updateDate       : {
    type     : Number,
    default  : Date.now,
    required : false,
  },
  /**
   * Instante de creación del registro, expresado en milisegundos.
   */
  creationDate     : {
    type     : Number,
    /**
     * Ejecuta default dentro del flujo de notifications.model.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    default  : function () {
      return new Date().getTime();
    },
    required : false,
  },
});
schema.pre(
  'validate',

  /**
   * Comprueba las condiciones de validateAccessMetadata antes de continuar.
   * @returns {void} Finaliza después de aplicar los efectos de la operación.
   */
  function validateAccessMetadata() {
    for (const field of ['workspace', 'userCreate', 'userAssigned']) {
      if (typeof this[field] !== 'string') this.invalidate(field, 'ACCESS_METADATA_REQUIRED');
    }
  },
);
schema.index({
  workspace  : 1,
  userCreate : 1,
});
schema.index({
  workspace    : 1,
  userAssigned : 1,
});

schema.set('versionKey', false);
schema.set('autoIndex', false);
schema.set('strict', false);
schema.index({
  userCode : 1,
  type     : -1,
});
schema.index({
  notificationCode : 1,
  type             : -1,
});
schema.index(
  {
    broadcastCode : 1,
    userAssigned  : 1,
  },
  {
    unique                  : true,
    name                    : 'notification_broadcast_recipient',
    partialFilterExpression : { broadcastCode: { $type: 'string' } },
  },
);

schema.index(
  {
    sourceEventKey : 1,
    workspace      : 1,
    userAssigned   : 1,
  },
  {
    unique                  : true,
    name                    : 'notification_source_event_recipient',
    partialFilterExpression : { sourceEventKey: { $type: 'string' } },
  },
);

module.exports = mongoose.model('CONNECT_NOTIFICATIONS', schema, 'CONNECT_NOTIFICATIONS');
