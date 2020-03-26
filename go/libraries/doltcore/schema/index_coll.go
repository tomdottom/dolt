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

import "fmt"

//TODO: column renames need to change colToTag
//TODO: tag changes need to do...a lot
type IndexCollection interface {
	// AddIndexCol adds an index with the given name, columns (in index order), and table primary keys (also in order).
	// Ex. For table {k1, k2, k3} PK(k1, k2) IDX(k3, k2), columns should be (k3, k2) and pks (k1, k2)
	AddIndexCol(indexName string, cols []string, pks []string) (Index, error)
	// AddIndexTag adds an index with the given name, column allTags (in index order), and table pk allTags (also in order).
	AddIndexTag(indexName string, tags []uint64, pks []uint64) (Index, error)
	// AllIndexes returns a slice containing all of the indexes in this collection.
	AllIndexes() []Index
	// BestMatch takes a collection of columns and finds the index that best matches, if one exists.
	BestMatch(cols ...string) Index
	// Contains returns whether the given index name already exists for this table.
	Contains(indexName string) bool
	// ContainsColumnCollection returns whether the collection contains an index that has this exact collection and ordering of columns.
	ContainsColumnCollection(cols ...string) bool
	// ContainsColumnTagCollection returns whether the collection contains an index that has this exact collection and ordering of columns.
	ContainsColumnTagCollection(tags ...uint64) bool
	// Count returns the number of indexes in this collection.
	Count() int
	// Get returns the index with the given name, or nil if it does not exist.
	Get(indexName string) Index
	// DropIndex removes an index from the table metadata.
	DropIndex(indexName string) (Index, error)
	// HasIndexes returns whether this collection has any indexes.
	HasIndexes() bool
	// RenameIndex renames an index in the table metadata.
	RenameIndex(oldName, newName string) (Index, error)
}

type indexCollectionImpl struct {
	indexes       map[string]*indexImpl
	colTagToIndex map[uint64][]*indexImpl
	colToTag      map[string]uint64
}

func NewIndexCollection(cols *ColCollection) IndexCollection {
	ixc := &indexCollectionImpl{
		indexes:       make(map[string]*indexImpl),
		colTagToIndex: make(map[uint64][]*indexImpl),
		colToTag:      make(map[string]uint64),
	}
	if cols != nil {
		for tag, col := range cols.TagToCol {
			ixc.colToTag[col.Name] = tag
			ixc.colTagToIndex[tag] = nil
		}
	}
	return ixc
}

func (ixc *indexCollectionImpl) AddIndexCol(indexName string, cols []string, pks []string) (Index, error) {
	if ixc.Contains(indexName) {
		return nil, fmt.Errorf("`%s` already exists as an index for this table", indexName)
	}
	if ixc.ContainsColumnCollection(cols...) {
		return nil, fmt.Errorf("cannot create a duplicate index on this table")
	}
	index := &indexImpl{
		name: indexName,
	}
	ixc.indexes[indexName] = index
	tags := make([]uint64, len(cols))
	for i, col := range cols {
		tag := ixc.colToTag[col]
		tags[i] = tag
		ixc.colTagToIndex[tag] = append(ixc.colTagToIndex[tag], index)
	}
	index.tags = tags
	pkTags := make([]uint64, len(pks))
	for i, pk := range pks {
		tag := ixc.colToTag[pk]
		pkTags[i] = tag
	}
	index.pks = pkTags
	index.allTags = combineAllTags(tags, pkTags)
	return index, nil
}

func (ixc *indexCollectionImpl) AddIndexTag(indexName string, tags []uint64, pks []uint64) (Index, error) {
	if ixc.Contains(indexName) {
		return nil, fmt.Errorf("`%s` already exists as an index for this table", indexName)
	}
	if ixc.ContainsColumnTagCollection(tags...) {
		return nil, fmt.Errorf("cannot create a duplicate index on this table")
	}
	index := &indexImpl{
		name: indexName,
		tags: tags,
		allTags: combineAllTags(tags, pks),
		pks:  pks,
	}
	ixc.indexes[indexName] = index
	for _, tag := range tags {
		ixc.colTagToIndex[tag] = append(ixc.colTagToIndex[tag], index)
	}
	return index, nil
}

func (ixc *indexCollectionImpl) AllIndexes() []Index {
	indexes := make([]Index, len(ixc.indexes))
	i := 0
	for _, index := range ixc.indexes {
		indexes[i] = index
		i++
	}
	return indexes
}

func (ixc *indexCollectionImpl) BestMatch(cols ...string) Index {
	//TODO: actually determine the best index based on the given columns, which can include non-indexed columns
	if len(cols) == 0 {
		return nil
	}
	match := ixc.colTagToIndex[ixc.colToTag[cols[0]]]
	if len(match) != 0 {
		return match[0]
	}
	return nil
}

func (ixc *indexCollectionImpl) Contains(indexName string) bool {
	_, ok := ixc.indexes[indexName]
	return ok
}

func (ixc *indexCollectionImpl) ContainsColumnCollection(cols ...string) bool {
	tags := make([]uint64, len(cols))
	for i, col := range cols {
		tag, ok := ixc.colToTag[col]
		if !ok {
			return false
		}
		tags[i] = tag
	}
	return ixc.ContainsColumnTagCollection(tags...)
}

func (ixc *indexCollectionImpl) ContainsColumnTagCollection(tags ...uint64) bool {
	tagCount := len(tags)
	for _, idx := range ixc.indexes {
		if tagCount == len(idx.tags) {
			allMatch := true
			for i, idxTag := range idx.tags {
				if tags[i] != idxTag {
					allMatch = false
					break
				}
			}
			if allMatch {
				return true
			}
		}
	}
	return false
}

func (ixc *indexCollectionImpl) Count() int {
	return len(ixc.indexes)
}

func (ixc *indexCollectionImpl) Get(indexName string) Index {
	return ixc.indexes[indexName]
}

func (ixc *indexCollectionImpl) DropIndex(indexName string) (Index, error) {
	if !ixc.Contains(indexName) {
		return nil, fmt.Errorf("`%s` does not exist as an index for this table", indexName)
	}
	index := ixc.indexes[indexName]
	delete(ixc.indexes, indexName)
	for _, tag := range index.tags {
		indexesRefThisCol := ixc.colTagToIndex[tag]
		for i, comparisonIndex := range indexesRefThisCol {
			if comparisonIndex == index {
				indexesRefThisCol = append(indexesRefThisCol[:i], indexesRefThisCol[i+1:]...)
				break
			}
		}
	}
	return index, nil
}

func (ixc *indexCollectionImpl) HasIndexes() bool {
	if len(ixc.indexes) > 0 {
		return true
	}
	return false
}

func (ixc *indexCollectionImpl) RenameIndex(oldName, newName string) (Index, error) {
	if !ixc.Contains(oldName) {
		return nil, fmt.Errorf("`%s` does not exist as an index for this table", oldName)
	}
	if ixc.Contains(newName) {
		return nil, fmt.Errorf("`%s` already exists as an index for this table", newName)
	}
	index := ixc.indexes[oldName]
	delete(ixc.indexes, oldName)
	index.name = newName
	ixc.indexes[newName] = index
	return index, nil
}

func combineAllTags(tags []uint64, pks []uint64) []uint64 {
	allTags :=  make([]uint64, len(tags))
	_ = copy(allTags, tags)
	foundPKCols := make(map[uint64]bool)
	for _, pk := range pks {
		foundPKCols[pk] = false
	}
	for _, tag := range tags {
		if _, ok := foundPKCols[tag]; ok {
			foundPKCols[tag] = true
		}
	}
	for _, pk := range pks {
		if !foundPKCols[pk] {
			allTags = append(allTags, pk)
		}
	}
	return allTags
}