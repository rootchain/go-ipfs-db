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
	"crypto/rand"
	"encoding/hex"
	"io"
	. "testing"

	"github.com/ethereum/go-ethereum/ethdb"
	cells "github.com/ipfn/go-ipfn-cells"
	multihash "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"

	cid "gx/ipfs/QmapdYm1b22Frv3k17fqrBYTFRxwiaVJkB299Mfn33edeB/go-cid"
)

func TestWrapped(t *T) {
	value, _ := hex.DecodeString("e6a020547d511b25302c027c2f24ba8ea63070d77d6c3c37c432498ed69190e2eadd8473646633")
	c, _ := cells.ParseCID("z45oqTS4xQ8rFEdSu9VNZbhZiSdabQvnZUc77iXHUctzUk9jvis")
	hash := c.Digest()

	db := Wrap(ethdb.NewMemDatabase())
	err := db.Put(hash, value)
	assert.Equal(t, err, nil)

	db = Wrap(ethdb.NewMemDatabase())
	res, err := db.Get(hash)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, value)
}

func TestWrapped_Rand(t *T) {
	for index := 0; index < 3; index++ {
		value := make([]byte, 32)
		_, err := io.ReadFull(rand.Reader, value)
		assert.Equal(t, err, nil)

		c, _ := cells.SumCID(cid.Prefix{
			Version:  1,
			Codec:    cid.EthStateTrie,
			MhType:   multihash.KECCAK_256,
			MhLength: 32,
		}, value)
		hash := c.Digest()

		db := Wrap(ethdb.NewMemDatabase())
		err = db.Put(hash, value)
		assert.Equal(t, err, nil)

		db = Wrap(ethdb.NewMemDatabase())
		res, err := db.Get(hash)
		assert.Equal(t, err, nil)
		assert.Equal(t, res, value)
	}
}
