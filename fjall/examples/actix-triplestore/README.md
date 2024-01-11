# actix-triplestore

This example uses `fjall`, `actix_web` and `serde_json` to provide a simple triplestore with a JSON REST API.

## REST API

### `POST /{subject}`

Upserts a subject.

```json
{
  "item": {
    "name": "John"
  }
}
```

### `POST /{subject}/{verb}/{object}`

Upserts a relation between a subject and an object.

```json
{
  "item": {
    "created_at": 12345
  }
}
```

### `GET /{subject}`

Returns a subject if it exists.

### `GET /{subject}/{verb}?limit=1234`

Returns a list of edges, their data and the corresponding nodes.
