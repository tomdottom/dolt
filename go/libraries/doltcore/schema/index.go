// Copyright 2020 Liquidata, Inc.
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

package schema

type Index interface {
	// Name returns the name of the index.
	Name() string
	// FullName returns the full dolt name of the index.
	FullName(tableName string) string
	// Tags returns the tags of the columns in the index.
	Tags() []uint64
	// AllTags returns the tags of the columns in the entire index, including the primary keys.
	// The result is equivalent to a schema check on the associated dolt index table.
	AllTags() []uint64
	// PrimaryKeys returns the primary keys of the indexed table, in the order that they're stored for that table.
	PrimaryKeys() []uint64
}

var _ Index = (*indexImpl)(nil)

type indexImpl struct {
	name    string
	tags    []uint64
	allTags []uint64
	pks     []uint64
}

func (ix *indexImpl) Name() string {
	return ix.name
}

func (ix *indexImpl) FullName(tableName string) string {
	return "dolt_index_" + tableName + "_" + ix.name
}

func (ix *indexImpl) Tags() []uint64 {
	return ix.tags
}

func (ix *indexImpl) AllTags() []uint64 {
	return ix.allTags
}

func (ix *indexImpl) PrimaryKeys() []uint64 {
	return ix.pks
}
