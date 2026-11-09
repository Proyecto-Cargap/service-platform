/**
 * @file Esquema Mongoose de versiones publicadas: campos, valores predeterminados y reglas de
 * persistencia.
 * Define la estructura utilizada al crear y consultar documentos de este recurso.
 */
'use strict';
const mongoose    = require('mongoose');
const { INDEXES } = require('../contracts/release.contract');

const Schema         = mongoose.Schema;
const webBuildSchema = new Schema(
  {
    id          : {
      type      : String,
      required  : true,
      match     : /^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$/,
      immutable : true,
    },
    sourceSha   : {
      type      : String,
      required  : true,
      match     : /^[a-f0-9]{40}$/,
      immutable : true,
    },
    environment : {
      type      : String,
      required  : true,
      enum      : ['test', 'prod'],
      immutable : true,
    },
  },
  { _id: false },
);

const schema = new Schema({
  /**
   * Espacio de trabajo del registro; allWorkspaces identifica recursos compartidos.
   */
  workspace     : {
    type     : String,
    default  : undefined,
    required : true,
  },
  /**
   * Código del usuario autor; permanece inmutable después de crear el registro.
   */
  userCreate    : {
    type      : String,
    default   : undefined,
    required  : false,
    immutable : true,
  },
  /**
   * Código del usuario responsable; vacío cuando no existe una asignación.
   */
  userAssigned  : {
    type     : String,
    default  : undefined,
    required : false,
  },
  /**
   * Código único de negocio que identifica la publicación.
   */
  releaseCode   : {
    type     : String,
    default  : undefined,
    required : [true, 'release code not found'],
  },
  /**
   * Instante de publicación de la versión, expresado en milisegundos.
   */
  releaseDate   : {
    type     : Number,
    default  : 0,
    required : false,
  },
  /**
   * Estado de la versión: desarrollo, vigente o publicada.
   */
  state         : {
    type     : String,
    default  : '0-development',
    required : false,
    enum     : {
      values  : ['0-development', '1-current', '2-released'],
      message : '{VALUE} is not valid state',
    },
  },
  /**
   * Identificador de la versión del software.
   */
  version       : {
    type     : String,
    default  : '',
    required : false,
  },
  /**
   * Identificador de compilación del software.
   */
  build         : {
    type     : String,
    default  : '',
    required : false,
  },
  /**
   * Compilación web verificada al publicar; los registros históricos conservan null.
   */
  webBuild      : {
    type      : webBuildSchema,
    default   : null,
    required  : false,
    immutable : true,
  },
  /**
   * Descripción de los cambios incluidos en la versión.
   */
  description   : {
    type     : String,
    default  : '',
    required : false,
  },
  /**
   * Referencia de la imagen asociada a la versión.
   */
  image         : {
    type     : String,
    default  : '',
    required : false,
  },
  /**
   * Identificador de correlación de la operación que produjo el registro.
   */
  idTransaction : {
    type     : String,
    default  : '',
    required : false,
  },
  /**
   * Instante de la última modificación del registro, expresado en milisegundos.
   */
  updateDate    : {
    type     : Number,
    /**
     * Ejecuta default dentro del flujo de release.model.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    default  : function () {
      return new Date().getTime();
    },
    required : false,
  },
  /**
   * Marca de baja lógica; su valor no altera automáticamente los filtros de lectura.
   */
  erased        : {
    type     : Boolean,
    default  : false,
    required : false,
  },
  /**
   * Instante de creación del registro, expresado en milisegundos.
   */
  creationDate  : {
    type     : Number,
    /**
     * Ejecuta default dentro del flujo de release.model.
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
schema.index({ releaseCode: 1 });
for (const { key, ...options } of INDEXES) schema.index(key, options);

module.exports = mongoose.model('PLATFORM_RELEASE', schema, 'PLATFORM_RELEASE');
