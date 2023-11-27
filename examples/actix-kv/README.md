# actix-kv

This example uses `lsm_tree`, `actix_web` and `serde_json` to provide a simple key-value store with a JSON REST API.

## REST API

### `POST /batch`

Upserts and/or removes items in an atomic batch.

```json
{
  "upsert": [
    ["key", "value"]
  ],
  "remove": ["another_key"]
}
```

### `PUT /{key}`

Upserts an item.

```json
{
  "item": SOME_JSON_VALUE
}
```

### `GET /{key}

Returns a single item.

### `DELETE /{key}`

Deletes an item.
