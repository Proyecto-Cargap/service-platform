/**
 * @file Esquema Mongoose de configuración central de plataforma y navegación: campos, valores predeterminados y
 * reglas de persistencia.
 * Define la estructura utilizada al crear y consultar documentos de este recurso.
 */
'use strict';
const mongoose = require('mongoose');
const schema   = new mongoose.Schema(
  {
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
     * Identificador único del documento dentro de la colección.
     */
    _id          : {
      type     : String,
      default  : undefined,
      required : false,
      enum     : ['global'],
    },
    /**
     * Contenido de aplicación del parámetro o configuración; admite valores heterogéneos.
     */
    data         : {
      type     : mongoose.Schema.Types.Mixed,
      default  : undefined,
      required : true,
    },
    /**
     * Fecha de la última actualización de la navegación.
     */
    updatedAt    : {
      type     : Date,
      default  : Date.now,
      required : false,
    },
    /**
     * Código del usuario que realizó la última actualización de la navegación.
     */
    updatedBy    : {
      type     : String,
      default  : '',
      required : false,
    },
  },
  {
    versionKey : false,
    strict     : 'throw',
  },
);
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

module.exports = mongoose.model('PLATFORM_CONFIGURATION', schema, 'PLATFORM_CONFIGURATION');
