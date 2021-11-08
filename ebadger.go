package ebadger

import (
	"encoding/json"

	"github.com/dgraph-io/badger/v3"
)

func SetMarshal(txn *badger.Txn, key []byte, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return txn.Set(key, data)
}

func GetUnmarshal(txn *badger.Txn, key []byte, value interface{}) error {
	it, err := txn.Get(key)
	if err != nil {
		return err
	}

	return it.Value(func(data []byte) error {
		return json.Unmarshal(data, value)
	})
}

func List(txn *badger.Txn, prefix []byte, lister func(item *badger.Item) error) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix

	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		err := lister(it.Item())
		if err != nil {
			return err
		}
	}

	return nil
}
