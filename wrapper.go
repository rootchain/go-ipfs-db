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

	cid "github.com/ipfs/go-cid"
	shell "github.com/ipfs/go-ipfs-api"
	multihash "github.com/multiformats/go-multihash"
)

var ipfs = shell.NewShellWithClient("http://localhost:5001", http.DefaultClient)

// Wrap - Wraps database with IPFS storage.
func Wrap(db ethdb.Database) ethdb.Database {
	return &dbWrap{Database: db}
}

type dbWrap struct {
	ethdb.Database
}

func (db *dbWrap) Get(key []byte) (value []byte, err error) {
	v, err := db.Database.Get(key)
	if err == nil {
		return v, nil
	}
	return ipfsGet(key)
}

func (db *dbWrap) Put(key []byte, value []byte) error {
	if err := ipfsPut(key, value); err != nil {
		return err
	}
	return db.Database.Put(key, value)
}

func (db *dbWrap) NewBatch() ethdb.Batch {
	return &batchWrap{Batch: db.Database.NewBatch()}
}

type batchWrap struct {
	ethdb.Batch
}

func (batch *batchWrap) Put(key, value []byte) error {
	if err := ipfsPut(key, value); err != nil {
		return err
	}
	return batch.Batch.Put(key, value)
}

func ipfsPut(key, value []byte) (err error) {
	if len(value) == 0 {
		return
	}
	_, err = ipfs.BlockPut(value, "eth-state-trie", "keccak-256", 32)
	return
}

func ipfsGet(key []byte) (value []byte, err error) {
	mhash, _ := multihash.EncodeName(key, "keccak-256")
	c := cid.NewCidV1(cid.EthStateTrie, mhash).String()
	value, err = ipfs.BlockGet(c)
	return
}
