
module.exports = function(RED) {
    "use strict";
    var mongo = require('mongodb');
    var ObjectID = require('mongodb').ObjectId;
    var MongoClient = mongo.MongoClient;

    function MongoNode(n) {
        RED.nodes.createNode(this,n);

        console.log("MongoDB URL: " + this.credentials.url);
        let node = this;
        node.client = new MongoClient(node.credentials.url, {
            useNewURLParser: true,
            useUnifiedTopology: true
        })
        node.client.connect()
            .then((db)=>{
                node.db = db.db(n.database);
                console.log("Connected and called");
            })
            .catch((err)=>{
                node.db = null;
            })
    }

    RED.nodes.registerType("mongodb",MongoNode,{
        credentials: {
            url: {type:"text"},
        }
    });

    function ensureValidSelectorObject(selector) {
        if (selector != null && (typeof selector != 'object' || Buffer.isBuffer(selector))) {
            return {};
        }
        return selector;
    }

    function MongoOutNode(n) {
        RED.nodes.createNode(this,n);
        this.collection = n.collection;
        this.mongodb = n.mongodb;
        this.payonly = n.payonly || false;
        this.upsert = n.upsert || false;
        this.multi = n.multi || false;
        this.operation = n.operation;
        this.mongoConfig = RED.nodes.getNode(this.mongodb);
        this.status({fill:"grey",shape:"ring",text:RED._("mongodb.status.connecting")});
        var node = this;
        if(!node.mongoConfig.db){
            node.status({fill:"red",shape:"ring",text:RED._("mongodb.status.error")})
        }
        node.status({fill:"green",shape:"dot",text:RED._("mongodb.status.connected")});

        node.on("input", function(msg, nodeSend, nodeDone){
            async function run(msg) {
                if(!node.mongoConfig.db){
                    node.status({fill:"red",shape:"ring",text:RED._("mongodb.status.error")})
                    return nodeDone("Unable to connect to MongoDB", msg);
                }
                node.status({fill:"green",shape:"dot",text:RED._("mongodb.status.connected")});
    
                let db = node.mongoConfig.db;
                let coll;
    
                if (node.collection) {
                    coll = db.collection(node.collection);
                }
                
                if (!node.collection) {
                    if (msg.collection) {
                        coll = db.collection(msg.collection);
                    }
                    else {
                        nodeDone(RED._("mongodb.errors.nocollection"),msg);
                        
                    }
                }
                delete msg._topic;
                delete msg.collection;
                if (node.operation === "insert") {
                    if (node.payonly) {
                        if (typeof msg.payload !== "object") {
                            msg.payload = {"payload": msg.payload};
                        }
                        if (msg.hasOwnProperty("_id") && !msg.payload.hasOwnProperty("_id")) {
                            msg.payload._id = msg._id;
                        }
                        
                        await coll.insertOne(msg.payload);
                    }
                    else {
                        await coll.insertOne(msg);
                    }
                }
                else if (node.operation === "update") {
                    if (typeof msg.payload !== "object") {
                        msg.payload = {"payload": msg.payload};
                    }
                    var query = msg.query || {};
                    var payload = msg.payload || {};
                    var options = {
                        upsert: node.upsert,
                        multi: node.multi
                    };
                    if (ObjectID.isValid(msg.query._id)) {
                        msg.query._id = new ObjectID(msg.query._id);
                    }
                    await coll.updateOne(query, payload, options);
                }
                else if (node.operation === "delete") {
                    await coll.removeOne(msg.payload);
                }
                msg.payload = "success";
                msg.trigger = 1;
                node.send([msg,null]);
                nodeDone(); 
              }

            run(msg).catch(e=>{
                console.log(e);
                node.status({fill:"red",shape:"ring",text:RED._("mongodb.status.error")});
                // msg.error = "Couldn't not connect to mongodb"
                msg.error = e
                node.send([null, msg]);
                // msg.error = e;
                nodeDone(msg.error, msg);
            });


        })
   
        node.on("close", function() {
            console.log(node.mongoConfig)
            console.log("98293492834283489")
            node.status({});
            if (node.tout) { clearTimeout(node.tout); }
            if (node.mongoConfig.client) { node.mongoConfig.client.close(); console.log("!@#!@#!@#") }
        });
    }
    RED.nodes.registerType("mongodb out",MongoOutNode);


    function MongoInNode(n) {
        RED.nodes.createNode(this,n);
        this.collection = n.collection;
        this.mongodb = n.mongodb;
        this.operation = n.operation || "find";
        this.mongoConfig = RED.nodes.getNode(this.mongodb);
        this.status({fill:"grey",shape:"ring",text:RED._("mongodb.status.connecting")});
        var node = this;
        if(!node.mongoConfig.db){
            node.status({fill:"red",shape:"ring",text:RED._("mongodb.status.error")})
        }
        node.status({fill:"green",shape:"dot",text:RED._("mongodb.status.connected")});

        node.on("input", function(msg, nodeSend, nodeDone){
            async function run(msg) {
                if(!node.mongoConfig.db){
                    node.status({fill:"red",shape:"ring",text:RED._("mongodb.status.error")})
                    return nodeDone("Unable to connect to MongoDB", msg);
                }
                node.status({fill:"green",shape:"dot",text:RED._("mongodb.status.connected")});
    
                let db = node.mongoConfig.db;
                let coll;
                if (!node.collection) {
                    if (msg.collection) {
                        coll = db.collection(msg.collection);
                    }
                    else {
                        return nodeDone(RED._("mongodb.errors.nocollection"),msg);
                    }
                }
                else {
                    coll = db.collection(node.collection);
                }
                var selector;
                if (node.operation === "find") {
                    msg.projection = msg.projection || {};
                    selector = ensureValidSelectorObject(msg.payload);
                    var limit = msg.limit;
                    if (typeof limit === "string" && !isNaN(limit)) {
                        limit = Number(limit);
                    } else if (typeof limit === "undefined") {
                        limit = 0;
                    }
                    var skip = msg.skip;
                    if (typeof skip === "string" && !isNaN(skip)) {
                        skip = Number(skip);
                    } else if (typeof skip === "undefined") {
                        skip = 0;
                    }

                    coll.find(selector)
                        .project(msg.projection)
                        .sort(msg.sort)
                        .limit(limit)
                        .skip(skip)
                        .toArray()
                        .then(items=>{
                            msg.payload = items;
                            delete msg.projection;
                            delete msg.sort;
                            delete msg.limit;
                            delete msg.skip;
                            node.send(msg);
                            nodeDone();
                        })
                        
                }
                else if (node.operation === "count") {
                    selector = ensureValidSelectorObject(msg.payload);
                    coll.count(selector)
                        .then((count)=>{
                            msg.payload = count;
                            node.send(msg);
                        })  
                        
                }
                else if (node.operation === "aggregate") {
                    msg.payload = (Array.isArray(msg.payload)) ? msg.payload : [];
                    coll.aggregate(msg.payload)
                        .then(cursor=>{
                            cursor.toArray()
                                .then(cursorDocs=>{
                                    node.send(msg);
                                })
                        })
                   
                }
              }

            run(msg).catch(e=>{
                console.log(e);
                node.status({fill:"red",shape:"ring",text:RED._("mongodb.status.error")});
                // msg.error = "Couldn't not connect to mongodb"
                msg.error = e
                node.send([null, msg]);
                // msg.error = e;
                nodeDone(msg.error, msg);
            });

        })
        node.on("close", function() {
            console.log(node.mongoConfig)
            console.log("98293492834283489")
            node.status({});
            if (node.tout) { clearTimeout(node.tout); }
            if (node.mongoConfig.client) { node.mongoConfig.client.close(); console.log("!@#!@#!@#") }
        });
    }
    RED.nodes.registerType("mongodb in",MongoInNode);
}