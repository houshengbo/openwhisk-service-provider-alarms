'use strict';
/*
 'use strict' is not required but helpful for turning syntactical errors into true errors in the program flow
 http://www.w3schools.com/js/js_strict.asp
*/
var logger = require('./Logger');
var express = require('express');
var bodyParser = require('body-parser');
var CronJob = require('cron').CronJob;
var request = require('request');

var app = express();
app.use(bodyParser.json());

var tid = '??';
var triggers = {};

var triggersLimit = 10000;
var retriesBeforeDelete = 5;

/*var cloudantUsername = process.env.CLOUDANT_USERNAME;
var cloudantPassword = process.env.CLOUDANT_PASSWORD;
var cloudantDbPrefix = process.env.DB_PREFIX;*/

var cloudantUsername = "houshengbo";
var cloudantPassword = "000000000";
var cloudantDbPrefix = "openwhisk";

var cloudantDatabase = cloudantDbPrefix + "alarmservice";
var nano = require('nano')('https://' + cloudantUsername + ':' + cloudantPassword + '@' + cloudantUsername + '.cloudant.com');
nano.db.create(cloudantDatabase);
var db = nano.db.use(cloudantDatabase);

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// standard RAS endpoints
app.get("/ping", function pong(req, res) {
    res.send({msg: 'pong'});
});

app.post('/triggers', authorize, function(req, res) {
    var method = 'POST /triggers';
    logger.info(tid, method, 'Got trigger', req.body);

    var newTrigger = req.body;

    // early exits
    if(!newTrigger.namespace) return sendError(method, 400, 'no namespace provided', res);
    if(!newTrigger.name) return sendError(method, 400, 'no name provided', res);
    if(!newTrigger.cron) return sendError(method, 400, 'no cron provided', res);
    if(newTrigger.maxTriggers > triggersLimit) return sendError(method, 400, 'maxTriggers > ' + triggersLimit + ' is not allowed', res);

    newTrigger.apikey = req.user.uuid + ':' + req.user.key;
    try {
        createTrigger(newTrigger);
    }
    catch(e) {
        return sendError(method, 400, 'cron seems to be malformed', res);
    }

    var triggerIdentifier = getTriggerIdentifier(newTrigger.apikey, newTrigger.namespace, newTrigger.name);
    db.insert(newTrigger, triggerIdentifier, function(err) {
        if(!err) {
            res.status(200).json({ok: 'your trigger was created successfully'});
        }
    });
});

app.delete('/triggers/:namespace/:name', authorize, function(req, res) {
    var deleted = deleteTrigger(req.params.namespace, req.params.name, req.user.uuid + ':' + req.user.key);
    if(deleted) {
        res.status(200).json({ok: 'trigger ' + req.params.name + ' successfully deleted'});
    }
    else {
        res.status(404).json({error: 'trigger ' + req.params.name + ' not found'});
    }
});

function createTrigger(newTrigger) {
    var method = 'createTrigger';

    var triggerIdentifier = getTriggerIdentifier(newTrigger.apikey, newTrigger.namespace, newTrigger.name);
    var cronHandle = new CronJob(newTrigger.cron,
        function onTick() {
            var triggerHandle = triggers[triggerIdentifier];
            if(triggerHandle && triggerHandle.triggersLeft > 0 && triggerHandle.retriesLeft > 0) {
                fireTrigger(newTrigger.namespace, newTrigger.name, newTrigger.payload, newTrigger.apikey);
            }
        }
    );
    cronHandle.start();
    logger.info(tid, method, triggerIdentifier, 'created successfully');
    triggers[triggerIdentifier] = {
        cron: newTrigger.cron,
        cronHandle: cronHandle,
        triggersLeft: newTrigger.maxTriggers,
        retriesLeft: retriesBeforeDelete,
        apikey: newTrigger.apikey,
        name: newTrigger.name,
        namespace: newTrigger.namespace
    };
}

function fireTrigger(namespace, name, payload, apikey) {
    var method = 'fireTrigger';
    var triggerIdentifier = getTriggerIdentifier(apikey, namespace, name);
    var routerHost = process.env.ROUTER_HOST;
    var host = "https://" + routerHost + ":443";
    var keyParts = apikey.split(':');
    var triggerHandle = triggers[triggerIdentifier];

    triggerHandle.triggersLeft--;

    request({
        method: 'POST',
        uri: host + '/api/v1/namespaces/' + namespace + '/triggers/' + name,
        json: payload,
        auth: {
            user: keyParts[0],
            pass: keyParts[1]
        }
    }, function(err, res) {
        if(triggerHandle) {
            if(err || res.statusCode >= 400) {
                triggerHandle.retriesLeft--;
                triggerHandle.triggersLeft++; // setting the counter back to where it used to be
                logger.warn(tid, method, 'there was an error invoking', triggerIdentifier, err);
            }
            else {
                triggerHandle.retriesLeft = retriesBeforeDelete; // reset retry counter
                logger.info(tid, method, 'fired', triggerIdentifier, 'with', payload, triggerHandle.triggersLeft, 'triggers left');
            }

            if(triggerHandle.triggersLeft === 0 || triggerHandle.retriesLeft === 0) {
                if(triggerHandle.triggersLeft === 0) logger.info(tid, 'onTick', 'no more triggers left, deleting');
                if(triggerHandle.retriesLeft === 0) logger.info(tid, 'onTick', 'too many retries, deleting');
                deleteTrigger(triggerHandle.namespace, triggerHandle.name, triggerHandle.apikey);
            }
        }
        else {
            logger.info(tid, method, 'trigger', triggerIdentifier, 'was deleted between invocations');
        }
    });
}

function deleteTrigger(namespace, name, apikey) {
    var method = 'deleteTrigger';

    var triggerIdentifier = getTriggerIdentifier(apikey, namespace, name);
    if(triggers[triggerIdentifier]) {
        triggers[triggerIdentifier].cronHandle.stop();
        delete triggers[triggerIdentifier];

        logger.info(tid, method, 'trigger', triggerIdentifier, 'successfully deleted');

        db.get(triggerIdentifier, function(err, body) {
            if(!err) {
                db.destroy(body._id, body._rev, function(err) {
                    if(err) logger.error(tid, method, 'there was an error while deleting', triggerIdentifier, 'from database');
                });
            }
            else {
                logger.error(tid, method, 'there was an error while deleting', triggerIdentifier, 'from database');
            }
        });
        return true;
    }
    else {
        logger.info(tid, method, 'trigger', triggerIdentifier, 'could not be found');
        return false;
    }
}

function getTriggerIdentifier(apikey, namespace, name) {
    return apikey + '/' + namespace + '/' + name;
}

function resetSystem() {
    var method = 'resetSystem';
    logger.info(tid, method, 'resetting system from last state');
    db.list({include_docs: true}, function(err, body) {
        if(!err) {
            body.rows.forEach(function(trigger) {
                createTrigger(trigger.doc);
            });
        }
        else {
            logger.error(tid, method, 'could not get latest state from database');
        }
    });
}

function sendError(method, code, message, res) {
    logger.warn(tid, method, message);
    res.status(code).json({error: message});
}

function authorize(req, res, next) {
    if(!req.headers.authorization) return sendError(400, 'Malformed request, authentication header expected', res);

    var parts = req.headers.authorization.split(' ');
    if (parts[0].toLowerCase() !== 'basic' || !parts[1]) return sendError(400, 'Malformed request, basic authentication expected', res);

    var auth = new Buffer(parts[1], 'base64').toString();
    auth = auth.match(/^([^:]*):(.*)$/);
    if (!auth) return sendError(400, 'Malformed request, authentication invalid', res);

    req.user = {
        uuid: auth[1],
        key: auth[2]
    };

    next();
}

app.listen(8080, function () {
    logger.info(tid, 'init', 'listening on port 8080');
    resetSystem();
});
