package ebadger

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
)

func Test_setGet(t *testing.T) {
	db, teardown := prepareDB(t)
	t.Cleanup(teardown)
	so := require.New(t)

	type data struct {
		ID    string    `json:"id"`
		Date  time.Time `json:""`
		Flag  bool      `json:"flag"`
		Float float64   `json:"float"`
		Int   int       `json:"int"`
	}

	someData := data{
		ID:    "bla-bla",
		Date:  time.Date(2021, time.November, 8, 8, 4, 30, 0, time.UTC),
		Flag:  true,
		Float: 36.6,
		Int:   42,
	}

	err := db.Update(func(txn *badger.Txn) error {
		return SetMarshal(txn, []byte(someData.ID), someData)
	})
	so.NoError(err)

	var retrivedData data
	err = db.View(func(txn *badger.Txn) error {
		return GetUnmarshal(txn, []byte(someData.ID), &retrivedData)
	})
	so.NoError(err)

	so.Equal(someData, retrivedData)
}

func TestList(t *testing.T) {
	db, teardown := prepareDB(t)
	t.Cleanup(teardown)
	so := require.New(t)

	testData := map[string]string{
		"elem1": "val1",
		"elem2": "val2",
		"elem3": "val3",
		"elem4": "val4",
		"elem5": "val5",
	}

	// insert test data
	err := db.Update(func(txn *badger.Txn) error {
		for key, value := range testData {
			if err := txn.Set([]byte(key), []byte(value)); err != nil {
				return err
			}
		}

		return nil
	})
	so.NoError(err)

	retrivedData := map[string]string{}
	err = db.View(func(txn *badger.Txn) error {
		return List(txn, []byte("elem"), func(item *badger.Item) error {
			return item.Value(func(val []byte) error {
				retrivedData[string(item.Key())] = string(val)
				return nil
			})
		})
	})
	so.NoError(err)

	so.Equal(testData, retrivedData)
}

func prepareDB(t *testing.T) (*badger.DB, func()) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}

	teardown := func() {
		err := db.Close()
		if err != nil {
			t.Errorf("erro closing db %v", err)
		}
	}

	return db, teardown
}
