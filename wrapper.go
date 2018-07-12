// Copyright © 2017-2018 The IPFN Developers. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ipfsdb

import (
	"net/http"

	"github.com/ethereum/go-ethereum/ethdb"

	shell "github.com/ipfs/go-ipfs-api"
	multihash "github.com/multiformats/go-multihash"

	cid "gx/ipfs/QmapdYm1b22Frv3k17fqrBYTFRxwiaVJkB299Mfn33edeB/go-cid"
)

// Wrap - Wraps database with IPFS storage.
func Wrap(db ethdb.Database) ethdb.Database {
	return WrapURL(db, "http://localhost:5001")
}

// WrapURL - Wraps database with IPFS storage.
func WrapURL(db ethdb.Database, url string) ethdb.Database {
	client := newClient(url)
	return &wrapDB{Database: db, client: client}
}

type wrapDB struct {
	ethdb.Database
	client *wrapClient
}

func (db *wrapDB) Get(key []byte) (value []byte, err error) {
	if v, err := db.Database.Get(key); err == nil {
		return v, nil
	}
	return db.client.Get(key)
}

func (db *wrapDB) Put(key []byte, value []byte) error {
	if err := db.Database.Put(key, value); err != nil {
		return err
	}
	return db.client.Put(key, value)
}

func (db *wrapDB) NewBatch() ethdb.Batch {
	return &wrapBatch{Batch: db.Database.NewBatch(), client: db.client}
}

type wrapBatch struct {
	ethdb.Batch
	client *wrapClient
}

func (batch *wrapBatch) Put(key, value []byte) error {
	if err := batch.client.Put(key, value); err != nil {
		return err
	}
	return batch.Batch.Put(key, value)
}

type wrapClient struct {
	*shell.Shell
}

func newClient(url string) *wrapClient {
	return &wrapClient{Shell: shell.NewShellWithClient(url, http.DefaultClient)}
}

func (client *wrapClient) Put(key, value []byte) (err error) {
	if len(value) == 0 {
		return
	}
	_, err = client.BlockPut(value, "eth-state-trie", "keccak-256", 32)
	return
}

func (client *wrapClient) Get(key []byte) (value []byte, err error) {
	mhash, _ := multihash.Encode(key, multihash.KECCAK_256)
	c := cid.NewCidV1(cid.EthStateTrie, mhash).String()
	value, err = client.BlockGet(c)
	return
}
