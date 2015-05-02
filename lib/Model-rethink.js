var r = require('rethinkdb');

var connection;
var getDb = function(dbConfig){
  r.connect(dbConfig, function(err, conn){
    if(err) throw err;
    connection = conn;
  });
};

module.exports = function(Model, dbConfig){

  var RethinkModel = Model.subclass();

  if (!connection) {
    getDb(dbConfig);
  }

  var table = r.table(RethinkModel.prototype.table);

  var respond = function(callback, type, err, item){
    if(typeof callback === 'function'){
      if(err) return callback(err);
      var resp;
      switch(type){
        case 'array':
          resp = item.map(function(i){
            return new RethinkModel(i);
          });
          break;
        case 'raw':
          resp = item;
          break;
        case 'first':
          resp = item[0] ? new RethinkModel(item[0]) : null;
          break;
        default :
          resp = item ? new RethinkModel(item) : null;
          break;
      }
      callback(null, resp);
    } else if(err){
      throw err;
    }
  };

  RethinkModel.all = function(callback){
    table.run(connection, function(err, cursor) {
      if(err) return callback(err);
      cursor.toArray(respond.bind(null, callback, 'array'));
    });
  };

  RethinkModel.one = function(id, callback){
    table.get(id).run(connection, respond.bind(null, callback, 'item'));
  };

  RethinkModel.filter = function(predecate, opts, callback){
    if (!callback) {
      callback = opts;
      opts = {};
    }
    table.filter(predecate, opts).run(connection, respond.bind(null, callback, 'array'));
  };

  RethinkModel.prototype.save = function(obj, callback){
    if (!callback) {
      callback = obj;
      obj = {};
    }
    if(this.id){
      table.update(this, obj).run(connection, respond.bind(null, callback, 'item'));
    } else {
      table.insert(this, obj).run(connection, respond.bind(null, callback, 'item'));
    }
  };

  RethinkModel.destroy = function(id, obj, callback){
    if (!callback) {
      callback = obj;
      obj = {};
    }
    table.get(id).delete().run(connection, respond.bind(null,callback,'raw'));
  };

  RethinkModel.prototype.destroy = function(callback){
    RethinkModel.destroy(this.id,callback);
  };

  RethinkModel.prototype.getTable = function(callback){
    callback(table, connection);
  };

  return RethinkModel;
};
