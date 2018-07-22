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

	"github.com/ipfn/go-ipfn-cmd-util/logger"

	"github.com/ethereum/go-ethereum/ethdb"

	shell "github.com/ipfs/go-ipfs-api"

	mh "gx/ipfs/QmPnFwZ2JXKnXgMw8CdBPxn7FWh6LLdjUjxV1fKHuJnkr8/go-multihash"
	cid "gx/ipfs/QmapdYm1b22Frv3k17fqrBYTFRxwiaVJkB299Mfn33edeB/go-cid"
)

// Wrap - Wraps database with IPFS storage.
func Wrap(prefix cid.Prefix, db ethdb.Database) ethdb.Database {
	return WrapURL(prefix, db, "http://localhost:5001")
}

// WrapURL - Wraps database with IPFS storage.
func WrapURL(prefix cid.Prefix, db ethdb.Database, url string) ethdb.Database {
	client := newClient(prefix, url)
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
	return db.client.Put(value)
}

func (db *wrapDB) NewBatch() ethdb.Batch {
	return &wrapBatch{Batch: db.Database.NewBatch(), client: db.client}
}

type wrapBatch struct {
	ethdb.Batch
	client *wrapClient
}

func (batch *wrapBatch) Put(key, value []byte) error {
	if err := batch.Batch.Put(key, value); err != nil {
		return err
	}
	return batch.client.Put(value)
}

type wrapClient struct {
	*shell.Shell
	cid.Prefix
}

func newClient(prefix cid.Prefix, url string) *wrapClient {
	return &wrapClient{
		Shell:  shell.NewShellWithClient(url, http.DefaultClient),
		Prefix: prefix,
	}
}

func (client *wrapClient) Put(value []byte) (err error) {
	if len(value) == 0 {
		return
	}
	cid, err := client.BlockPut(value, cid.CodecToStr[client.Prefix.Codec], mh.Codes[client.Prefix.MhType], client.Prefix.MhLength)
	if err != nil {
		logger.Debugw("IPFS BlockPut", "err", err)
		return
	}
	logger.Debugw("IPFS BlockPut", "cid", cid)
	return
}

func (client *wrapClient) Get(key []byte) (value []byte, err error) {
	mhash, _ := mh.Encode(key, client.Prefix.MhType)
	c := cid.NewCidV1(client.Prefix.Codec, mhash).String()
	logger.Debugw("IPFS BlockGet", "cid", c)
	value, err = client.BlockGet(c)
	return
}
