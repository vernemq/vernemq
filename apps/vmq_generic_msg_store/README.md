# Building VerneMQ with RocksDB support

To build VerneMQ with RocksDB support (only), issue:

```shell
make with_rocksdb
```

To configure Verne to use RocksDB for the message store and the SWC store,
add the following to your vernemq.conf file.

```
# Use RocksDB Message Store
generic_message_store_engine = vmq_storage_engine_rocksdb
generic_message_store.directory = ./data/msgstore

# Use RocksDB for SWC Metadata Store
vmq_swc.db_backend = rocksdb
```

## RocksDB configuration

Any further RocksDB configuration needs to be done in the `advanced.config` file. 
Find a `rocksdb_advanced_example.config` in the `priv` directory, adapt it and copy it to
the /etc directory of your VerneMQ release.