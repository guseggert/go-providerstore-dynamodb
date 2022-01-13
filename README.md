This is a [ProviderStore](https://github.com/libp2p/go-libp2p-kad-dht/blob/master/providers/providers_manager.go#L36) that stores DHT records in DynamoDB.

The schema of the table is:

- key (bytes)
  - Primary Key
  - typically this is the raw multihash of a CID
- ttl (number)
  - Sort Key
  - unix epoch timestamp of when the entry expires
  - configure your DynamoDB table to use this as an item TTL for auto-eviction
  - since this is a sort key, results for a given CID are ordered by this, so that you can return the most recently-cached providers first
- prov (bytes)
  - the raw peer ID of the provider of the given key
  
A DHT put maps to a single [PutItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html) request to DynamoDB.

DHT queries map to a DynamoDB [Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html), keyed on the CID multihash. The query returns an item per provider of the CID, up to a pre-configured limit. For popular CIDs with many providers, this could take many round-trips to DynamoDB to finish. A single round-trip can return up to 1 MB of items. Since items are automatically evicted by DynamoDB based on their TTL, this automatically excludes expired entries.
