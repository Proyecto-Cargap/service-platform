/**
 * @file Esquema Mongoose de parámetros de configuración: campos, valores predeterminados y reglas de
 * persistencia.
 * Define la estructura utilizada al crear y consultar documentos de este recurso.
 */
'use strict';
const mongoose = require('mongoose');

const Schema = mongoose.Schema;

const schema = new Schema({
  /**
   * Espacio de trabajo del registro; allWorkspaces identifica recursos compartidos.
   */
  workspace    : {
    type     : String,
    default  : undefined,
    required : true,
  },
  /**
   * Código del usuario autor; permanece inmutable después de crear el registro.
   */
  userCreate   : {
    type      : String,
    default   : undefined,
    required  : false,
    immutable : true,
  },
  /**
   * Código del usuario responsable; vacío cuando no existe una asignación.
   */
  userAssigned : {
    type     : String,
    default  : undefined,
    required : false,
  },
  /**
   * Clave que identifica el parámetro de configuración.
   */
  parameter    : {
    type     : String,
    default  : undefined,
    required : [true, 'Parameter Name not found'],
  },
  /**
   * Contenido de aplicación del parámetro o configuración; admite valores heterogéneos.
   */
  data         : {
    type     : mongoose.Schema.Types.Mixed,
    default  : undefined,
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

schema.index({ parameter: 1 });

schema.set('versionKey', false);
schema.set('strict', false);

module.exports = mongoose.model('PLATFORM_SETTINGS', schema, 'PLATFORM_SETTINGS');
