var crypto = require('crypto');
var minimist = require('minimist');
var redis = require('redis');

var lockScript = 'return redis.call("set", KEYS[1], ARGV[1], "NX", "PX", ARGV[2])';
var unlockScript = 'if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end';
var extendScript = 'if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end';

var PRODUCER_LOCK_KEY = 'producer.lock';
var LOCK_TIMEOUT = 500;
var EXTEND_LOCK_TIMEOUT = 100;
var MESSAGE_TIMEOUT = 0;

var lock;
var role;
var rdb;
var cnt;

function getMessage() {
  cnt = cnt || 0;
  return cnt++;
}

function eventHandler(msg, callback) {
  function onComplete() {
    var error = Math.random() > 0.85;
    callback(error, msg);
  }
  // processing takes time...
  setTimeout(onComplete, Math.floor(Math.random() * 1000));
}

function getRandomString() {
  return crypto.randomBytes(16).toString('hex');
}

function getLock() {
  var randomString = getRandomString();
  rdb.eval(lockScript, 1, PRODUCER_LOCK_KEY, randomString, LOCK_TIMEOUT, function(err, data) {
    if (err) {
      return console.error(err);
    }

    if (data) {
      lock = randomString;
      role = 'producer';
      startProducer();
    } else {
      setTimeout(getLock, LOCK_TIMEOUT);
      if (!role) {
        role = 'consumer';
        startConsumer();
      }
    }
  });
}

function startProducer() {
  console.log('I\'m a producer');
  function produce() {
    var msg = getMessage();
    rdb.rpush('messages', msg);
    setTimeout(produce, MESSAGE_TIMEOUT);
  }
  produce();

  function extendLock() {
    rdb.eval(extendScript, 1, PRODUCER_LOCK_KEY, lock, LOCK_TIMEOUT, function() {
      setTimeout(extendLock, EXTEND_LOCK_TIMEOUT);
    });
  }
  extendLock();
}

function startConsumer() {
  console.log('I\'m a consumer');
  var blpopQueue = function() {
    if (lock) {
      return;
    }

    rdb.lpop('messages', function(err, data) {
      if (err) {
        return console.error(err);
      }

      if (data) {
        eventHandler(data, function(err, msg) {
          if (err) {
            rdb.rpush('errors', msg);
          } else {
            // console.log(msg);
          }
          process.nextTick(blpopQueue);
        });
      } else {
        process.nextTick(blpopQueue);
      }
    });
  };
  blpopQueue();
}

function getErrors() {
  console.log('Errors:');
  var errorsQueue = function() {
    rdb.lpop('errors', function(err, data) {
      if (data) {
        console.log(data);
        process.nextTick(errorsQueue);
      } else {
        rdb.quit();
      }
    });
  };
  errorsQueue();
}

var rdb = redis.createClient();
var argv = minimist(process.argv.slice(2));

if (argv._.indexOf('getErrors') !== -1) {
  getErrors();
} else {
  getLock();
}
