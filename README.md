# Simple AWS DynamoDB driver for nodejs

Simple Node.js DynamoDB driver to perform basic CRUD operations, it is meant to be as simple as possible.

## Installation

```

  npm i dynamodb-driver

```

## Initialisation

```javascript

  var DDbDriver = require("dynamodb-driver"),
      table = DDbDriver(awsConfigObject, DynamoDbConfigObject);

```

### Example

```javascript

  var table = DDbDriver({
    region: "eu-west-1"
    // --- your AWS config object if need be (AWS.config.update(awsconfig));
  }, {
    dynamodb: '2012-08-10'
  });

```

## Create row

Creates a row in the specified table, and id will be automatically generated using shorId if non exists in the document
Returns a promise resolving a document that has just been created

### Usage

```javascript

  somePromise = table.create(tableName, documentToCrate);

```

### Example

```javascript

  table.create("Users", {
    firstName: "Marilyn",
    lastName: "Manson",
    active: true
  }).then(function(user) {

    // user is something like
    {
      id : "S1KtimR6"
      firstName: "Marilyn",
      lastName: "Manson",
      active: true
    }

  });

```

## Update row

### Usage

```javascript

  somePromise = table.update(tableName, documentToUpdate [, conditions] [, keys]);

```

conditions: An object with a ConditionExpression and a ExpressionAttributeValues
(See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html)

keys: Should you table is not using id as primary key, you can specify your primary and sort key here such as ["organisationId", "documentId"]

#### Conditional write

Specify a value within brackets with an exclamation mark to perform an update only if the value doesn't exist

```javascript
{
  id: "S1KtimR6",
  creationDate: "[!" + new Date().getTime() + "]"
}
```

#### Increment

Specify a value within brackets with ++ to perform an increment by one of the existing value

```javascript
{
  id: "S1KtimR6",
  count: "[++]"
}
```

#### Decrement

Specify a value within brackets with -- to perform a decrement by one of the existing value

```javascript
{
  id: "S1KtimR6",
  count: "[--]"
}
```

### Example

```javascript

  table.update("Users", {
    id: "S1KtimR6",
    isPowerUser: true
  }, {
    ConditionExpression: "active = :active",
    ExpressionAttributeValues: {
        ":active": true
    }
  }).then(function(user) {

    // user would be something like that
    {
      id : "S1KtimR6"
      firstName: "Marilyn",
      lastName: "Manson",
      active: true,
      isPowerUser: true
    }
  });

```

## Query rows

### Usage

```javascript

  somePromise = table.query(tableName, query [, indexName] [, options]);

```

query:
- could be an array for the query with the key, ComparisonOperator and AttributeValueList for legacy KeyConditions.
- could be an object with a KeyConditionExpression and ExpressionAttributeNames

### Example

```javascript

  // Using the legacy KeyConditions mode
  table.query("Users", [{
    key: "email",
    operator: "EQ",
    value: email
  },{
    key: "password",
    operator: "EQ",
    value: password
  }], "EmailPasswordIndex").then(function(users) {

    // users is an Array of users
    [{
      id: "S1KtimR6",
      firstName: "Marilyn"
      lasName: "Manson",
      active: true,
      isPowerUser: true
    }, {
      id: "Z1et9rR5",
      firstName: "Amaia"
      lasName: "Albistur",
      active: true,
      isPowerUser: false
    }]

  });

```

## Get row by Key

### Usage

```javascript

  somePromise = table.get(tableName, id);

```

### Example

```javascript

  table.get("Users", "Z1et9rR5").then(function(user) {

    // user is something like that
    {
      id: "Z1et9rR5",
      firstName: "Amaia"
      lasName: "Albistur",
      active: true,
      isPowerUser: false
    }

  });

```

## Get multiple rows by Key

### Usage

```javascript

  somePromise = table.getItems(tableName, arrayOfIds [, options]);

```
arrayOfIds:
An array of ids to retrieve, or an array of objects containing the partition and sort key of the items to retrieve

options: is an optional object<br />
options.consistentRead: true or false, will apply strongly consistent reads instead of the default setting (eventually consistent reads)<br />
options.keys: an array of 1 or 2 strings defining the partition and sort keys

### Example

```javascript

  table.getItems("Users", ["Z1et9rR5", "S1KtimR6"], {consistentRead: true}).then(function(users) {

    // users is an Array of users
    [{
      id: "S1KtimR6",
      firstName: "Marilyn"
      lasName: "Manson",
      active: true,
      isPowerUser: true
    }, {
      id: "Z1et9rR5",
      firstName: "Amaia"
      lasName: "Albistur",
      active: true,
      isPowerUser: false
    }]

  });

```

## Delete row

### Usage

```javascript

  somePromise = table.remove(tableName, id);

```

### Example

```javascript

  table.remove("Users", "Z1et9rR5").then(function(user) {
    // user is
    {
      id: "Z1et9rR5",
      firstName: "Amaia"
      lasName: "Albistur",
      active: true,
      isPowerUser: false
    }
  });

```

## Delete multiple rows by Key

### Usage

```javascript

  somePromise = table.removeItems(tableName, arrayOfObject[, keys]);

```
keys: when not using id as a primary, you must specify the HASH and optionally RANGE key

### Example

```javascript

  table.removeItems("Users", [{ id: "S1KtimR6"}, {id : "Z1et9rR5"}]).then(function(users) {

    // users is an Array of users
    [{
      id: "S1KtimR6",
      firstName: "Marilyn"
      lasName: "Manson",
      active: true,
      isPowerUser: true
    }, {
      id: "Z1et9rR5",
      firstName: "Amaia"
      lasName: "Albistur",
      active: true,
      isPowerUser: false
    }]

  });

```

## List rows

### Usage

```javascript

  somePromise = table.list(tableName[, query, options]);

```

Will perform a scan operation on the selected table

query: A specific query array to filter data against
options: An object with paginate property as number.
        It will recursively fetch batches of paginate value until all data has been fetched for this query
        (note, no exponential backing off)

### Example

```javascript

  table.list("Users", [{
    key: "isPowerUser",
    operator: "EQ",
    value: true
  }], {
    paginate: 10 // Will retieve all data by batches of 10
  }).then(function(users) {

    // users is an Array of users

    [{
      id: "S1KtimR6",
      firstName: "Marilyn"
      lasName: "Manson",
      active: true,
      isPowerUser: true
    }, {
      id: "X1ttUU45",
      firstName: "Francis"
      lasName: "Kuntz",
      active: true,
      isPowerUser: true
    }]

  });

```
