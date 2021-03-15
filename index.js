var Promise = require("bluebird");
const { v4: uuidv4 } = require("uuid");

var AWS = require("aws-sdk");
AWS.config.setPromisesDependency(Promise);

var utils = require("./utils");

global.__basePath = process.env.PWD;

module.exports = function (dynamoDbOptions) {
  "use strict";

  var dynamodb = new AWS.DynamoDB(dynamoDbOptions);

  var query = function (table, conditions, index, options) {
    var params = {
      TableName: table,
    };

    if (index) {
      params.IndexName = index;
    }

    // Legacy KeyConditions are in use
    if (conditions.constructor === Array) {
      var keys = {};
      conditions.forEach(function (condition) {
        keys[condition.key] = {
          ComparisonOperator: condition.operator,
          AttributeValueList: [utils.itemize(condition.value)],
        };
      });

      params.KeyConditions = keys;
    }

    // KeyConditionExpression are in use
    if (conditions.constructor === Object) {
      params.KeyConditionExpression = conditions.KeyConditionExpression;
      params.ExpressionAttributeNames = conditions.ExpressionAttributeNames;

      var ExpressionAttributeValues = {};

      var values = conditions.ExpressionAttributeValues;

      for (var p in values) {
        if (!values.hasOwnProperty(p)) {
          continue;
        }
        ExpressionAttributeValues[p] = utils.itemize(values[p]);
      }

      params.ExpressionAttributeValues = ExpressionAttributeValues;
    }

    if (options && options.reverse) {
      params.ScanIndexForward = false;
    }

    if (options && options.limit) {
      params.Limit = options.limit;
    }

    if (options && options.filter) {
      var queryFilter = {};

      options.filter.forEach(function (condition) {
        queryFilter[condition.key] = {
          ComparisonOperator: condition.operator,
          AttributeValueList: [utils.itemize(condition.value)],
        };
      });

      params.QueryFilter = queryFilter;
    }

    if (options && options.projection && options.projection.length) {
      params.ProjectionExpression = options.projection
        .map((p) => `#${p}`)
        .join(",");
      params.ExpressionAttributeNames = {};

      options.projection.forEach(function (p) {
        params.ExpressionAttributeNames[`#${p}`] = p;
      });
    }

    if (options && options.paginate) {
      var items = [];

      var paginate = function (lastItem) {
        params.Limit = options.paginate;

        if (lastItem) {
          params.ExclusiveStartKey = lastItem;
        }

        return dynamodb
          .query(params)
          .promise()
          .then(function (data) {
            data.Items.forEach(function (item) {
              items.push(utils.deitemize(item));
            });

            if (data.LastEvaluatedKey) {
              return paginate(data.LastEvaluatedKey);
            }

            return items;
          });
      };

      return paginate();
    }

    return dynamodb
      .query(params)
      .promise()
      .then(function (data) {
        var items = [];
        data.Items.forEach(function (item) {
          items.push(utils.deitemize(item));
        });
        return items;
      });
  };

  var list = function (table, conditions, options) {
    var params = {
      TableName: table,
    };

    var keys = {};

    conditions = conditions || [];
    conditions.forEach(function (condition) {
      keys[condition.key] = {
        ComparisonOperator: condition.operator,
        AttributeValueList: [utils.itemize(condition.value)],
      };
    });

    if (options && options.projection && options.projection.length) {
      params.ProjectionExpression = options.projection
        .map((p) => `#${p}`)
        .join(",");
      params.ExpressionAttributeNames = {};

      options.projection.forEach(function (p) {
        params.ExpressionAttributeNames[`#${p}`] = p;
      });
    }

    if (Object.keys(keys).length) {
      params.ScanFilter = keys;
    }

    if (options && options.paginate) {
      var items = [];

      var paginate = function (lastItem) {
        params.Limit = options.paginate;

        if (lastItem) {
          params.ExclusiveStartKey = lastItem;
        }

        return dynamodb
          .scan(params)
          .promise()
          .then(function (data) {
            data.Items.forEach(function (item) {
              items.push(utils.deitemize(item));
            });

            if (data.LastEvaluatedKey) {
              return paginate(data.LastEvaluatedKey);
            }

            return items;            
          });
      };

      return paginate();
    }

    if (options && options.limit) {
      params.Limit = options.limit
    }

    if (options && options.exclusiveStartKey) {
      params.ExclusiveStartKey = options.exclusiveStartKey
    }

    return dynamodb
      .scan(params)
      .promise()
      .then(function (data) {
        var items = [];
        data.Items.forEach(function (item) {
          items.push(utils.deitemize(item));
        });

        return {
          items,
          lastEvaluatedKey: data.LastEvaluatedKey
        }
      });
  };

  var get = function (table, id) {
    var params = {
      TableName: table,
      Key: {
        id: {
          S: id,
        },
      },
    };

    return dynamodb
      .getItem(params)
      .promise()
      .then(function (data) {
        return utils.deitemize(data.Item);
      });
  };

  var getItems = function (table, ids, options) {
    if (ids.constructor !== Array) {
      throw Error("ids is not an Array");
    }

    // Don't touch the original items, work on a copy
    var documents = ids.slice(0);

    if (!documents || documents.length === 0) {
      return Promise.resolve([]);
    }

    var items = [];

    options = options || {};

    var maxGetItems = 100; // Currently 25 on AWS

    // For fibonacci exponetial backoff
    var previous = 1;
    var anteprevious = 1;
    var delay = 0;

    // For reverting the back-off
    var series = [0];

    var getDocuments = function (Requests) {
      // Constructs the request
      var params = {
        RequestItems: {},
      };

      params.RequestItems[table] = Requests;

      return dynamodb
        .batchGetItem(params)
        .promise()
        .then(function (batchResponse) {
          var docs = batchResponse.Responses[table].map((i) =>
            utils.deitemize(i)
          );

          items = items.concat(docs);

          if (
            batchResponse.UnprocessedKeys[table] &&
            Array.isArray(batchResponse.UnprocessedKeys[table].Keys)
          ) {
            console.log(
              "%s unprocessed items",
              batchResponse.UnprocessedKeys[table].Keys.length
            );
            let delayAction;

            if (options.noDelay) {
              delayAction = Promise.resolve();
            } else {
              // Keep the list of delays
              series.push(delay);

              delay = previous + anteprevious;

              anteprevious = previous;
              previous = delay;

              console.log("Delaying next batch, %d seconds", delay);
              delayAction = Promise.delay(delay * 1000);
            }

            return delayAction.then(function () {
              return getDocuments(batchResponse.UnprocessedKeys[table]);
            });
          }

          // Revert the back of to the latest position
          // Not putting the back off to 0 to keep playing nicely
          if (series.length > 1) {
            previous = series.pop() || 0;
          }
        })
        .catch(function (err) {
          if (err.code === "ProvisionedThroughputExceededException") {
            console.log(
              "Provisioned Capacity error when getting items, delaying 15 seconds"
            );
            return Promise.delay(15 * 1000).then(function () {
              return getDocuments(Requests);
            });
          }

          console.log(err);
          throw err;
        });
    };

    // Returns a promise of the wripte operation
    var getOperation = function (documents) {
      var keys = documents.map(function (document) {
        if (!options.keys) {
          // By default, the parameters are an array of ids
          return {
            id: utils.itemize(document),
          };
        }

        // options.keys is specified as an array of values for hash or hash & key
        return options.keys.reduce(function (acc, key) {
          acc[key] = utils.itemize(document[key]);

          return acc;
        }, {});
      });

      const params = {
        ConsistentRead: options.consistentRead || false,
        Keys: keys,
      };

      if (options && Array.isArray(options.projection)) {
        params.ProjectionExpression = options.projection
          .map((p) => `#${p}`)
          .join(",");

        params.ExpressionAttributeNames = options.projection.reduce(
          (attributeNames, attributeName) => {
            attributeNames[`#${attributeName}`] = attributeName;
            return attributeNames;
          },
          {}
        );
      }

      return getDocuments(params);
    };

    // Contains arrays of $maxWriteItems items to save
    var documentsToGet = [];

    while (documents.length) {
      documentsToGet.push(documents.splice(0, maxGetItems));
    }

    // Wait for all promisses to be fullfilled one by one
    return Promise.each(documentsToGet, getOperation).then(() => items);
  };

  var create = function (table, document, conditions) {
    if (!document.id) {
      document.id = uuidv4();
    }

    var item = {};
    Object.keys(document).forEach(function (key) {
      item[key] = utils.itemize(document[key]);
    });

    var params = {
      ReturnConsumedCapacity: "TOTAL",
      ReturnItemCollectionMetrics: "SIZE",
      ReturnValues: "ALL_OLD",
      TableName: table,
      Item: item,
    };

    // KeyConditionExpression are in use
    if (conditions && conditions.constructor === Object) {
      params.ConditionExpression = conditions.ConditionExpression;

      if (conditions.ExpressionAttributeNames) {
        params.ExpressionAttributeNames = conditions.ExpressionAttributeNames;
      }

      var ExpressionAttributeValues = {};

      var values = conditions.ExpressionAttributeValues;

      for (var p in values) {
        if (!values.hasOwnProperty(p)) {
          continue;
        }
        ExpressionAttributeValues[p] = utils.itemize(values[p]);
      }

      params.ExpressionAttributeValues = ExpressionAttributeValues;
    }

    return dynamodb
      .putItem(params)
      .promise()
      .then(function () {
        return document;
      });
  };

  var createItems = function (table, items) {
    // Don't touch the original items, work on a copy
    var documents = items.slice(0);

    if (!documents || documents.length === 0) {
      return Promise.resolve();
    }

    var maxWriteItems = 25; // Currently 25 on AWS

    // For fibonacci exponetial backoff
    var previous = 1;
    var anteprevious = 1;
    var delay = 0;

    // For reverting the back-off
    var series = [0];

    var writeItems = function (PutRequests) {
      // Constructs the request
      var params = {
        RequestItems: {},
      };

      params.RequestItems[table] = PutRequests;

      return dynamodb
        .batchWriteItem(params)
        .promise()
        .then(function (batchResponse) {
          if (batchResponse.UnprocessedItems[table]) {
            // Keep the list of delays
            series.push(delay);

            delay = previous + anteprevious;

            anteprevious = previous;
            previous = delay;

            console.log(
              "%s unprocessed items",
              batchResponse.UnprocessedItems[table].length
            );
            console.log("Delaying next batch, %d seconds", delay);

            return Promise.delay(delay * 1000).then(function () {
              return writeItems(batchResponse.UnprocessedItems[table]);
            });
          }

          // Revert the back of to the latest position
          // Not putting the back off to 0 to keep playing nicely
          if (series.length > 1) {
            previous = series.pop() || 0;
          }
        })
        .catch(function (err) {
          if (err.code === "ProvisionedThroughputExceededException") {
            console.log(
              "Provisioned Capacity error when creating items, delaying 15 seconds"
            );

            return Promise.delay(15 * 1000).then(function () {
              return writeItems(PutRequests);
            });
          }

          console.log(err);
          throw err;
        });
    };

    // Returns a promise of the wripte operation
    var writeOperation = function (documentsToWrite) {
      var items = [];
      documentsToWrite.forEach(function (document) {
        // Dop not generate id if already present
        if (!document.id) {
          document.id = uuidv4();
        }

        var item = {};
        Object.keys(document).forEach(function (key) {
          item[key] = utils.itemize(document[key]);
        });

        items.push({
          PutRequest: {
            Item: item,
          },
        });
      });

      return writeItems(items);
    };

    // Contains arrays of $maxWriteItems items to save
    var documentsToWrite = [];

    while (documents.length) {
      documentsToWrite.push(documents.splice(0, maxWriteItems));
    }

    // Wait for all promisses to be fullfilled one by one
    return Promise.each(documentsToWrite, writeOperation);
  };

  var removeItems = function (table, items, keys) {
    // Don't touch the original items, work on a copy
    var documents = items.slice(0);

    if (!documents || documents.length === 0) {
      return Promise.resolve();
    }

    // By default id will be the key
    if (!keys) {
      keys = ["id"];
    }

    var maxWriteItems = 25; // Currently 25 on AWS

    // For fibonacci exponetial backoff
    var previous = 1;
    var anteprevious = 1;
    var delay = 0;

    // For reverting the back-off
    var series = [0];

    var deleteItems = function (PutRequests) {
      // Constructs the request
      var params = {
        RequestItems: {},
      };

      params.RequestItems[table] = PutRequests;

      return dynamodb
        .batchWriteItem(params)
        .promise()
        .then(function (batchResponse) {
          if (batchResponse.UnprocessedItems[table]) {
            // Keep the list of delays
            series.push(delay);

            delay = previous + anteprevious;

            anteprevious = previous;
            previous = delay;

            console.log(
              "%s unprocessed items",
              batchResponse.UnprocessedItems[table].length
            );
            console.log("Delaying next batch, %d seconds", delay);

            return Promise.delay(delay * 1000).then(function () {
              return deleteItems(batchResponse.UnprocessedItems[table]);
            });
          }

          // Revert the back of to the latest position
          // Not putting the back off to 0 to keep playing nicely
          if (series.length > 1) {
            previous = series.pop() || 0;
          }
        })
        .catch(function (err) {
          if (err.code === "ProvisionedThroughputExceededException") {
            console.log(
              "Provisioned Capacity error when deleting, delaying 15 seconds"
            );
            return Promise.delay(15 * 1000).then(function () {
              return deleteItems(PutRequests);
            });
          }

          throw err;
        });
    };

    // Returns a promise of the wripte operation
    var deleteOperation = function (documentsToDelete) {
      var items = [];
      documentsToDelete.forEach(function (document) {
        var item = {};
        Object.keys(document).forEach(function (key) {
          if (keys.indexOf(key) > -1) {
            item[key] = utils.itemize(document[key]);
          }
        });

        items.push({
          DeleteRequest: {
            Key: item,
          },
        });
      });

      return deleteItems(items);
    };

    // Contains arrays of $maxWriteItems items to save
    var documentsToDelete = [];

    while (documents.length) {
      documentsToDelete.push(documents.splice(0, maxWriteItems));
    }

    // Wait for all promisses to be fullfilled one by one
    return Promise.each(documentsToDelete, deleteOperation);
  };

  var update = function (table, document, conditions, keys, itemsToRemove) {
    // By default id will be the key
    if (!keys) {
      keys = ["id"];
    }

    var ExpressionAttributeNames = {};
    var ExpressionAttributeValues = {};
    var UpdateExpressions = [];

    Object.keys(document).forEach(function (key) {
      if (keys.indexOf(key) > -1) {
        return;
      }

      ExpressionAttributeNames["#" + key] = key;

      // Look for somthing like this: [!some_value]
      // This will make sure the value is not update if already set
      var conditional =
        typeof document[key] === "string" && /\[\!(.*)\]/g.exec(document[key]);

      // Look for somthing like this: [++]
      // This will increment the value
      var increment =
        typeof document[key] === "string" && /\[\+\+\]/g.exec(document[key]);

      // Look for somthing like this: [--]
      // This will decrement the value
      var decrement =
        typeof document[key] === "string" && /\[\-\-\]/g.exec(document[key]);

      if (conditional) {
        var value = isNaN(conditional[1]) ? conditional[1] : +conditional[1];

        ExpressionAttributeValues[":" + key] = utils.itemize(value);
        UpdateExpressions.push(
          "#" + key + " = " + "if_not_exists(#" + key + ", :" + key + ")"
        );
      } else if (increment) {
        ExpressionAttributeValues[":" + key] = utils.itemize(1);
        UpdateExpressions.push("#" + key + " = #" + key + " + :" + key);
      } else if (decrement) {
        ExpressionAttributeValues[":" + key] = utils.itemize(1);
        UpdateExpressions.push("#" + key + " = #" + key + " - :" + key);
      } else {
        ExpressionAttributeValues[":" + key] = utils.itemize(document[key]);
        UpdateExpressions.push("#" + key + " = " + ":" + key);
      }
    });

    var Key = {};
    keys.forEach(function (key) {
      Key[key] = utils.itemize(document[key]);
    });

    var params = {
      ReturnConsumedCapacity: "TOTAL",
      ReturnItemCollectionMetrics: "SIZE",
      ReturnValues: "ALL_NEW",
      Key: Key,
      ExpressionAttributeNames: ExpressionAttributeNames,
      ExpressionAttributeValues: ExpressionAttributeValues,
      UpdateExpression: "SET " + UpdateExpressions.join(", "),
      TableName: table,
    };

    if (conditions && conditions.constructor === Object) {
      params.ConditionExpression = conditions.ConditionExpression;

      // Append condition values to ExpressionAttributeValues
      var values = conditions.ExpressionAttributeValues;
      Object.keys(values).forEach(function (key) {
        params.ExpressionAttributeValues[key] = utils.itemize(values[key]);
      });
    }

    if (itemsToRemove) {
      params.UpdateExpression += " REMOVE " + itemsToRemove.join(", ");
    }

    return dynamodb
      .updateItem(params)
      .promise()
      .then(function (result) {
        // Because ReturnValues is set to ALL_NEW, returned result reflects what is now in database.
        return utils.deitemize(result.Attributes);
      })
      .catch(function (err) {
        if (err.code === "ProvisionedThroughputExceededException") {
          console.log(
            "Provisioned Capacity error when updating, delaying 15 seconds"
          );
          return Promise.delay(15 * 1000).then(function () {
            return update(table, document, conditions, keys, itemsToRemove);
          });
        }

        throw err;
      });
  };

  var remove = function (table, document) {
    var key = {};
    Object.keys(document).forEach(function (k) {
      key[k] = utils.itemize(document[k]);
    });

    var params = {
      TableName: table,
      Key: key,
    };

    return dynamodb.deleteItem(params).promise();
  };

  return {
    list: list,
    query: query,
    get: get,
    getItems: getItems,
    create: create,
    createItems: createItems,
    removeItems: removeItems,
    update: update,
    remove: remove,
  };
};
