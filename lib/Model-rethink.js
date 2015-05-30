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

  var tableName = RethinkModel.prototype.collection;

  if (!connection) {
    getDb(dbConfig);
  }

  var table = r.table(tableName);

  var respond = function(callback, type, err, cursor){
    if(typeof callback === 'function'){
      if(err) return callback(err);
      if(cursor && cursor.first_error) return callback(new Error(cursor.first_error));
      if (type === 'raw') {
        return callback(null, cursor);
      } else if (type === 'one') {
        return callback(null, new RethinkModel(cursor));
      }
      var items = [];
      cursor.each(function(err, item){
        if(err) return callback(err);
        if(item && item.new_val) item = item.new_val;
        items.push(item);
      }, function() {
        var resp;
        switch(type){
          case 'array':
            resp = items.map(function(i){
              return new RethinkModel(i);
            });
            break;
          default :
            resp = items[0] ? new RethinkModel(items[0]) : null;
            break;
        }
        callback(null, resp);
      });
    } else if(err){
      throw err;
    }
  };

  RethinkModel.all = function(ids, callback){
    if (!callback) {
      callback = ids;
      ids = null;
    }
    var op;
    if(ids) {
      op = table.getAll.apply(table, ids);
    } else {
      op = table;
    }
    op.run(connection, respond.bind(null, callback, 'array'));
  };

  RethinkModel.one = function(id, callback){
    if (!id || typeof id === 'function') {
      return callback(new Error('invalid id passed to .one'));
    }
    var q;
    var type = 'item';
    if(typeof id === 'object') {
      q = table.filter(id);
    } else {
      q = table.get(id);
      type = 'one';
    }
    q.run(connection, respond.bind(null, callback, type));
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
    var id = this.id
    var done = function(err, res) {
      if(res && res.first_error) return callback(new Error(res.first_error));
      if(err) return callback(err);
      var ids = id ? [id] : res.generated_keys
      callback(null, ids);
    }
    if(id){
      // don't change IDs
      var item = this.toObject()
      var id = item.id
      delete item.id
      table.get(id).update(item, obj).run(connection, done);
    } else {
      table.insert(this.toObject(), obj).run(connection, done);
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

  RethinkModel.prototype.getTable = function(callback) {
    callback.call(r, table, connection);
  };

  RethinkModel.getTable = function(callback) {
    callback.call(r, table, connection);
  }

  RethinkModel.r = r

  return RethinkModel;
};
