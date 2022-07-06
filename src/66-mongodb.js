"use strict";
/**** some basic type definitions for safety (or for illustration) ****/
Object.defineProperty(exports, "__esModule", { value: true });
// interface nodeConfig extends NodeDef {
//   user?: string;
//   password?: string;
//   hostname: string;
//   port: string;
//   db: string;
//   name: string;
//   connectOptions: string;
//   topology: string;
//   maxMsgLimit: number;
// }
const mongodb_1 = require("mongodb");
module.exports = function (RED) {
    "use strict";
    // var mongo = require("mongodb");
    var ObjectID = require("mongodb").ObjectID;
    // var MongoClient = mongo.MongoClient;
    function MongoNode(config) {
        RED.nodes.createNode(this, config);
        this.hostname = config.hostname;
        this.port = config.port;
        this.db = config.db;
        this.name = config.name;
        this.connectOptions = config.connectOptions;
        this.topology = config.topology;
        //console.log(this);
        var clustered = this.topology !== "direct" || false;
        var url = "mongodb://";
        if (this.topology === "dnscluster") {
            url = "mongodb+srv://";
        }
        if (this.credentials &&
            this.credentials.user &&
            this.credentials.password) {
            this.user = this.credentials.user;
            this.password = this.credentials.password;
        }
        else {
            this.user = config.user;
            this.password = config.password;
        }
        if (this.user) {
            url += this.user + ":" + this.password + "@";
        }
        if (clustered) {
            url += this.hostname + "/" + this.db;
        }
        else {
            url += this.hostname + ":" + this.port + "/" + this.db;
        }
        if (this.connectOptions) {
            url += "?" + this.connectOptions;
        }
        console.log("MongoDB URL: " + url);
        this.url = url;
    }
    RED.nodes.registerType("mongodb", MongoNode, {
        credentials: {
            user: { type: "text" },
            password: { type: "password" },
        },
    });
    function ensureValidSelectorObject(selector) {
        if (selector != null &&
            (typeof selector != "object" || Buffer.isBuffer(selector))) {
            return {};
        }
        return selector;
    }
    function MongoOutNode(config) {
        RED.nodes.createNode(this, config);
        this.collection = config.collection;
        this.mongodb = config.mongodb;
        this.payonly = config.payonly || false;
        this.upsert = config.upsert || false;
        this.multi = config.multi || false;
        this.operation = config.operation;
        this.mongoConfig = RED.nodes.getNode(this.mongodb);
        this.status({
            fill: "grey",
            shape: "ring",
            text: RED._("mongodb.status.connecting"),
        });
        var node = this;
        let client;
        async function runs() {
            client = new mongodb_1.MongoClient(node.mongoConfig.url);
            await client.connect();
            // nodeContext.connection_up = true;
            node.status({
                fill: "green",
                shape: "dot",
                text: RED._("mongodb.status.connected"),
            });
        }
        runs().catch((err) => {
            client = null;
            // nodeContext.connection_up = false;
            node.status({
                fill: "red",
                shape: "ring",
                text: RED._("mongodb.status.error"),
            });
        });
        console.log("OUTTTT WORKINGGG");
        node.on("input", function (msg, nodeSend, nodeDone) {
            async function run(msg) {
                if (!client) {
                    client = new mongodb_1.MongoClient(node.mongoConfig.url);
                    await client.connect();
                    node.status({
                        fill: "green",
                        shape: "dot",
                        text: RED._("mongodb.status.connected"),
                    });
                }
                node.client = client;
                var db = client.db();
                var coll;
                if (node.collection) {
                    coll = db.collection(node.collection);
                }
                if (!node.collection) {
                    if (msg.collection) {
                        coll = db.collection(msg.collection);
                    }
                    else {
                        nodeDone(Error(RED._("mongodb.errors.nocollection")));
                    }
                }
                delete msg._topic;
                delete msg.collection;
                if (coll) {
                    if (node.operation === "insert") {
                        if (node.payonly) {
                            if (typeof msg.payload !== "object") {
                                msg.payload = { payload: msg.payload };
                            }
                            if (msg.hasOwnProperty("_id") &&
                                !msg.payload.hasOwnProperty("_id")) {
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
                            msg.payload = { payload: msg.payload };
                        }
                        var query = msg.query || {};
                        var payload = msg.payload || {};
                        var options = {
                            upsert: node.upsert,
                            multi: node.multi,
                        };
                        if (ObjectID.isValid(msg.query._id)) {
                            msg.query._id = new ObjectID(msg.query._id);
                        }
                        await coll.updateOne(query, payload, options);
                    }
                    else if (node.operation === "delete") {
                        await coll.deleteOne(msg.payload);
                    }
                }
                msg.payload = "success";
                msg.trigger = true;
                node.send([msg, null]);
                nodeDone();
            }
            run(msg).catch((e) => {
                console.log("=================ERROR=====================");
                console.log(e);
                console.log("=================ERROR=====================");
                // runs().catch(()=>{});;
                // client = null;
                node.status({
                    fill: "red",
                    shape: "ring",
                    text: RED._("mongodb.status.error"),
                });
                // msg.error = "Couldn't not connect to mongodb"
                let error_msg = msg;
                error_msg.error = e;
                console.log("Sending Error MSG")
                node.send([null, error_msg]);
                // msg.error = e;
                nodeDone(Error(e));
                // nodeDone(msg.error, msg);
            });
        });
        node.on("close", function () {
            node.status({});
            if (node.tout) {
                clearTimeout(node.tout);
            }
            if (node.client) {
                node.client.close();
            }
        });
    }
    RED.nodes.registerType("mongodb out", MongoOutNode);
    function MongoInNode(config) {
        RED.nodes.createNode(this, config);
        this.collection = config.collection;
        this.mongodb = config.mongodb;
        this.operation = config.operation || "find";
        this.mongoConfig = RED.nodes.getNode(this.mongodb);
        this.status({
            fill: "grey",
            shape: "ring",
            text: RED._("mongodb.status.connecting"),
        });
        var node = this;
        var noerror = true;
        var client = null;
        async function run() {
            client = new mongodb_1.MongoClient(node.mongoConfig.url);
            await client.connect();
            node.status({
                fill: "green",
                shape: "dot",
                text: RED._("mongodb.status.connected"),
            });
        }
        run().catch((err) => {
            client = null;
            node.status({
                fill: "red",
                shape: "ring",
                text: RED._("mongodb.status.error"),
            });
        });
        node.on("input", function (msg, nodeSend, nodeDone) {
            async function run(msg) {
                if (!client) {
                    client = new mongodb_1.MongoClient(node.mongoConfig.url);
                    await client.connect();
                    node.status({
                        fill: "green",
                        shape: "dot",
                        text: RED._("mongodb.status.connected"),
                    });
                }
                node.client = client;
                var db = client.db();
                var coll;
                if (!node.collection) {
                    if (msg.collection) {
                        coll = db.collection(msg.collection);
                    }
                    else {
                        node.error(RED._("mongodb.errors.nocollection"));
                        return;
                    }
                }
                else {
                    coll = db.collection(node.collection);
                }
                var selector;
                if (node.operation === "find") {
                    let projection = msg.projection || {};
                    selector = ensureValidSelectorObject(msg.payload);
                    let limit = msg.limit;
                    if (typeof limit === "string" && !isNaN(+limit)) {
                        limit = Number(limit);
                    }
                    else if (typeof limit === "undefined") {
                        limit = 0;
                    }
                    var skip = msg.skip;
                    if (typeof skip === "string" && !isNaN(+skip)) {
                        skip = Number(skip);
                    }
                    else if (typeof skip === "undefined") {
                        skip = 0;
                    }
                    coll
                        .find(selector)
                        .project(projection)
                        .sort(msg.sort)
                        .limit(limit)
                        .skip(skip)
                        .toArray((err, items) => {
                        if (err) {
                            node.error(err, msg);
                        }
                        else {
                            msg.payload = items;
                            delete msg.projection;
                            delete msg.sort;
                            delete msg.limit;
                            delete msg.skip;
                            node.send(msg);
                        }
                    });
                }
                else if (node.operation === "count") {
                    selector = ensureValidSelectorObject(msg.payload);
                    coll.count(selector, function (err, count) {
                        if (err) {
                            node.error(err, msg);
                        }
                        else {
                            msg.payload = count;
                            node.send(msg);
                        }
                    });
                }
                else if (node.operation === "aggregate") {
                    msg.payload = Array.isArray(msg.payload) ? msg.payload : [];
                    let cursor = coll.aggregate(msg.payload);
                    cursor.toArray(function (cursorError, cursorDocs) {
                        //console.log(cursorDocs);
                        if (cursorError) {
                            node.error(cursorError, msg);
                        }
                        else {
                            msg.payload = cursorDocs;
                            node.send(msg);
                        }
                    });
                }
            }
            run(msg).catch((e) => {
                console.log(e);
                // client.close();
                client = null;
                node.status({
                    fill: "red",
                    shape: "ring",
                    text: RED._("mongodb.status.error"),
                });
                let error_msg = msg;
                error_msg.error = "Couldn't not connect to mongodb";
                node.send([null, error_msg]);
                nodeDone(e);
            });
        });
        // var connectToDB = function () {
        //   console.log("connecting:  " + node.mongoConfig.url);
        //   MongoClient.connect(
        //     node.mongoConfig.url,
        //     function (err: any, client: any) {
        //       if (err) {
        //         console.log("ERROR FOUND");
        //         node.on("input", function (msg, nodeSend, nodeDone) {
        //           let error_msg = msg as any;
        //           error_msg.error = err;
        //           node.send([null, msg]);
        //           // msg.error = e;
        //           nodeDone(Error(err));
        //         });
        //         node.status({
        //           fill: "red",
        //           shape: "ring",
        //           text: RED._("mongodb.status.error"),
        //         });
        //         if (noerror) {
        //           console.log("NOERRROR");
        //           node.error(err);
        //         }
        //         noerror = false;
        //         node.tout = setTimeout(connectToDB, 10000);
        //       } else {
        //         node.status({
        //           fill: "green",
        //           shape: "dot",
        //           text: RED._("mongodb.status.connected"),
        //         });
        //         node.client = client;
        //         var db = client.db();
        //         noerror = true;
        //         var coll;
        //         node.on("input", function (msg: MongoInMsg, nodeSend, done) {
        //           if (!node.collection) {
        //             if (msg.collection) {
        //               coll = db.collection(msg.collection);
        //             } else {
        //               node.error(RED._("mongodb.errors.nocollection"), msg);
        //               return;
        //               // return done();
        //             }
        //           } else {
        //             coll = db.collection(node.collection);
        //           }
        //           var selector;
        //           if (node.operation === "find") {
        //             let projection = msg.projection || {};
        //             selector = ensureValidSelectorObject(msg.payload);
        //             let limit = msg.limit;
        //             if (typeof limit === "string" && !isNaN(+limit)) {
        //               limit = Number(limit);
        //             } else if (typeof limit === "undefined") {
        //               limit = 0;
        //             }
        //             var skip = msg.skip;
        //             if (typeof skip === "string" && !isNaN(+skip)) {
        //               skip = Number(skip);
        //             } else if (typeof skip === "undefined") {
        //               skip = 0;
        //             }
        //             coll
        //               .find(selector)
        //               .project(msg.projection)
        //               .sort(msg.sort)
        //               .limit(limit)
        //               .skip(skip)
        //               .toArray(function (err: any, items: any) {
        //                 if (err) {
        //                   node.error(err, msg);
        //                 } else {
        //                   msg.payload = items;
        //                   delete msg.projection;
        //                   delete msg.sort;
        //                   delete msg.limit;
        //                   delete msg.skip;
        //                   node.send(msg);
        //                 }
        //               });
        //           } else if (node.operation === "count") {
        //             selector = ensureValidSelectorObject(msg.payload);
        //             coll.count(selector, function (err: any, count: any) {
        //               if (err) {
        //                 node.error(err, msg);
        //               } else {
        //                 msg.payload = count;
        //                 node.send(msg);
        //               }
        //             });
        //           } else if (node.operation === "aggregate") {
        //             msg.payload = Array.isArray(msg.payload) ? msg.payload : [];
        //             coll.aggregate(msg.payload, function (err: any, cursor: any) {
        //               if (err) {
        //                 node.error(err, msg);
        //               } else {
        //                 cursor.toArray(function (
        //                   cursorError: any,
        //                   cursorDocs: any
        //                 ) {
        //                   //console.log(cursorDocs);
        //                   if (cursorError) {
        //                     node.error(cursorError, msg);
        //                   } else {
        //                     msg.payload = cursorDocs;
        //                     node.send(msg);
        //                   }
        //                 });
        //               }
        //             });
        //           }
        //         });
        //       }
        //     }
        //   );
        // };
        // if (node.mongoConfig) {
        //   connectToDB();
        // } else {
        //   node.error(RED._("mongodb.errors.missingconfig"));
        // }
        node.on("close", function () {
            node.status({});
            if (node.tout) {
                clearTimeout(node.tout);
            }
            if (node.client) {
                node.client.close();
            }
        });
    }
    RED.nodes.registerType("mongodb in", MongoInNode);
};
