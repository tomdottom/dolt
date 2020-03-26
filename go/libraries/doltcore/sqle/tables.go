// Copyright 2019 Liquidata, Inc.
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

package sqle

import (
	"context"
	"errors"
	"fmt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"io"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/alterschema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// DoltTable implements the sql.Table interface and gives access to dolt table rows and schema.
type DoltTable struct {
	name   string
	table  *doltdb.Table
	sch    schema.Schema
	sqlSch sql.Schema
	db     *Database
}

var _ sql.Table = (*DoltTable)(nil)
var _ sql.IndexAlterableTable = (*DoltTable)(nil)

// Implements sql.IndexableTable
func (t *DoltTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	dil, ok := lookup.(*doltIndexLookup)
	if !ok {
		panic(fmt.Sprintf("Unrecognized indexLookup %T", lookup))
	}

	return &IndexedDoltTable{
		table:       t,
		indexLookup: dil,
	}
}

// Implements sql.IndexableTable
func (t *DoltTable) IndexKeyValues(*sql.Context, []string) (sql.PartitionIndexKeyValueIter, error) {
	return nil, errors.New("creating new indexes not supported")
}

// Implements sql.IndexableTable
func (t *DoltTable) IndexLookup() sql.IndexLookup {
	panic("IndexLookup called on DoltTable, should be on IndexedDoltTable")
}

// Name returns the name of the table.
func (t *DoltTable) Name() string {
	return t.name
}

// Not sure what the purpose of this method is, so returning the name for now.
func (t *DoltTable) String() string {
	return t.name
}

// Schema returns the schema for this table.
func (t *DoltTable) Schema() sql.Schema {
	return t.sqlSchema()
}

func (t *DoltTable) sqlSchema() sql.Schema {
	if t.sqlSch != nil {
		return t.sqlSch
	}

	// TODO: fix panics
	sqlSch, err := doltSchemaToSqlSchema(t.name, t.sch)
	if err != nil {
		panic(err)
	}

	t.sqlSch = sqlSch
	return sqlSch
}

// Returns the partitions for this table. We return a single partition, but could potentially get more performance by
// returning multiple.
func (t *DoltTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return &doltTablePartitionIter{}, nil
}

// Returns the table rows for the partition given (all rows of the table).
func (t *DoltTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return newRowIterator(t, ctx)
}

func (t *DoltTable) AddIndex(ctx *sql.Context, indexName string, using sql.IndexUsing, constraint sql.IndexConstraint, columns []sql.IndexColumn, comment string) error {
	if !doltdb.IsValidTableName(indexName) {
		return fmt.Errorf("invalid index name `%s` as they must match the regular expression %s", indexName, doltdb.TableNameRegexStr)
	}

	var cols []schema.Column
	var realColNames []string
	var pkNames []string
	currentTag := uint64(0)
	foundPKCols := make(map[string]struct{})
	allTableCols := t.sch.GetAllCols()
	for _, indexCol := range columns {
		tableCol, ok := allTableCols.GetByName(indexCol.Name)
		if !ok {
			tableCol, ok = allTableCols.GetByNameCaseInsensitive(indexCol.Name)
			if !ok {
				return fmt.Errorf("column `%s` does not exist for the table", indexCol.Name)
			}
		}
		if tableCol.IsPartOfPK {
			foundPKCols[tableCol.Name] = struct{}{}
		}
		realColNames = append(realColNames, tableCol.Name)
		cols = append(cols, schema.Column{
			Name:        tableCol.Name,
			Tag:         currentTag,
			Kind:        tableCol.TypeInfo.NomsKind(),
			IsPartOfPK:  true,
			TypeInfo:    tableCol.TypeInfo,
			Constraints: nil,
		})
		currentTag++
	}
	_ = t.sch.GetPKCols().Iter(func(tag uint64, tableCol schema.Column) (bool, error) {
		pkNames = append(pkNames, tableCol.Name)
		if _, ok := foundPKCols[tableCol.Name]; !ok {
			cols = append(cols, schema.Column{
				Name:        tableCol.Name,
				Tag:         currentTag,
				Kind:        tableCol.TypeInfo.NomsKind(),
				IsPartOfPK:  true,
				TypeInfo:    tableCol.TypeInfo,
				Constraints: nil,
			})
			currentTag++
		}
		return false, nil
	})

	colColl, err := schema.NewColCollection(cols...)
	if err != nil {
		return err
	}
	indexSch := schema.SchemaFromCols(colColl)
	index, err := t.sch.Indexes().AddIndexCol(indexName, realColNames, pkNames)
	if err != nil {
		return err
	}

	newSchemaVal, err := encoding.MarshalAsNomsValue(ctx, t.table.ValueReadWriter(), t.sch)
	if err != nil {
		return err
	}
	rowData, err := t.table.GetRowData(ctx)
	if err != nil {
		return err
	}
	newTable, err := doltdb.NewTable(ctx, t.table.ValueReadWriter(), newSchemaVal, rowData)
	if err != nil {
		return err
	}
	newRoot, err := t.db.root.PutTable(ctx, t.name, newTable)
	if err != nil {
		return err
	}

	indexData, err := types.NewMap(ctx, newRoot.VRW())
	if err != nil {
		return err
	}
	indexDataEditor := indexData.Edit()
	err = rowData.IterAll(ctx, func(key, value types.Value) error {
		dRow, err := row.FromNoms(t.sch, key.(types.Tuple), value.(types.Tuple))
		if err != nil {
			return err
		}
		indexRow, err := dRow.ReduceToIndex(index)
		if err != nil {
			return err
		}
		indexKey, err := indexRow.NomsMapKey(indexSch).Value(ctx)
		if err != nil {
			return err
		}
		indexDataEditor = indexDataEditor.Set(indexKey, dRow.NomsMapValue(indexSch))
		return nil
	})
	if err != nil {
		return err
	}
	indexData, err = indexDataEditor.Map(ctx)
	if err != nil {
		return err
	}

	indexSchVal, err := encoding.MarshalAsNomsValue(ctx, newRoot.VRW(), indexSch)
	if err != nil {
		return err
	}
	indexTbl, err := doltdb.NewTable(ctx, newRoot.VRW(), indexSchVal, indexData)
	if err != nil {
		return err
	}
	newRoot, err = newRoot.PutTable(ctx, index.FullName(t.name), indexTbl)
	if err != nil {
		return err
	}

	t.db.SetRoot(newRoot)
	return nil
}

func (t *DoltTable) DropIndex(ctx *sql.Context, indexName string) error {
	index, err := t.sch.Indexes().DropIndex(indexName)
	if err != nil {
		return err
	}
	newRoot, err := t.saveSchemaChanges(ctx, t.db.root)
	if err != nil {
		return err
	}
	newRoot, err = newRoot.RemoveTables(ctx, index.FullName(t.name))
	if err != nil {
		return err
	}
	t.db.SetRoot(newRoot)
	return nil
}

func (t *DoltTable) RenameIndex(ctx *sql.Context, fromIndexName string, toIndexName string) error {
	index := t.sch.Indexes().Get(fromIndexName)
	if index == nil {
		return fmt.Errorf("`%s` does not exist as an index for this table", fromIndexName)
	}
	fromFullName := index.FullName(t.name)

	index, err := t.sch.Indexes().RenameIndex(fromIndexName, toIndexName)
	if err != nil {
		return err
	}
	newRoot, err := t.saveSchemaChanges(ctx, t.db.root)
	if err != nil {
		return err
	}

	idxTable, ok, err := newRoot.GetTable(ctx, fromFullName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("failed to get index table `%s`", fromFullName)
	}

	newRoot, err = newRoot.PutTable(ctx, index.FullName(t.name), idxTable)
	if err != nil {
		return err
	}
	newRoot, err = newRoot.RemoveTables(ctx, fromFullName)
	if err != nil {
		return err
	}

	t.db.SetRoot(newRoot)
	return nil
}

func (t *DoltTable) saveSchemaChanges(ctx *sql.Context, root *doltdb.RootValue) (*doltdb.RootValue, error) {
	newSchemaVal, err := encoding.MarshalAsNomsValue(ctx, t.table.ValueReadWriter(), t.sch)
	if err != nil {
		return nil, err
	}
	rowData, err := t.table.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	newTable, err := doltdb.NewTable(ctx, t.table.ValueReadWriter(), newSchemaVal, rowData)
	if err != nil {
		return nil, err
	}
	newRoot, err := root.PutTable(ctx, t.name, newTable)
	if err != nil {
		return nil, err
	}
	t.table = newTable
	return newRoot, nil
}

// WritableDoltTable allows updating, deleting, and inserting new rows. It implements sql.UpdatableTable and friends.
type WritableDoltTable struct {
	DoltTable
	ed *tableEditor
}

var _ sql.UpdatableTable = (*WritableDoltTable)(nil)
var _ sql.DeletableTable = (*WritableDoltTable)(nil)
var _ sql.InsertableTable = (*WritableDoltTable)(nil)
var _ sql.ReplaceableTable = (*WritableDoltTable)(nil)

// Inserter implements sql.InsertableTable
func (t *WritableDoltTable) Inserter(ctx *sql.Context) sql.RowInserter {
	return t.getTableEditor(ctx)
}

func (t *WritableDoltTable) getTableEditor(ctx *sql.Context) *tableEditor {
	if t.db.batchMode == batched {
		if t.ed != nil {
			return t.ed
		}
		t.ed = newTableEditor(ctx, t)
		return t.ed
	}
	return newTableEditor(ctx, t)
}

func (t *WritableDoltTable) flushBatchedEdits(ctx context.Context) error {
	if t.ed != nil {
		err := t.ed.flush(ctx)
		t.ed = nil
		return err
	}
	return nil
}

// Deleter implements sql.DeletableTable
func (t *WritableDoltTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	return t.getTableEditor(ctx)
}

// Replacer implements sql.ReplaceableTable
func (t *WritableDoltTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return t.getTableEditor(ctx)
}

// Updater implements sql.UpdatableTable
func (t *WritableDoltTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return t.getTableEditor(ctx)
}

// doltTablePartitionIter, an object that knows how to return the single partition exactly once.
type doltTablePartitionIter struct {
	sql.PartitionIter
	i int
}

// Close is required by the sql.PartitionIter interface. Does nothing.
func (itr *doltTablePartitionIter) Close() error {
	return nil
}

// Next returns the next partition if there is one, or io.EOF if there isn't.
func (itr *doltTablePartitionIter) Next() (sql.Partition, error) {
	if itr.i > 0 {
		return nil, io.EOF
	}
	itr.i++

	return &doltTablePartition{}, nil
}

// A table partition, currently an unused layer of abstraction but required for the framework.
type doltTablePartition struct {
	sql.Partition
}

const partitionName = "single"

// Key returns the key for this partition, which must uniquely identity the partition. We have only a single partition
// per table, so we use a constant.
func (p doltTablePartition) Key() []byte {
	return []byte(partitionName)
}

func (t *DoltTable) updateTable(ctx context.Context, mapEditor *types.MapEditor) error {
	updated, err := mapEditor.Map(ctx)
	if err != nil {
		return errhand.BuildDError("failed to modify table").AddCause(err).Build()
	}

	newTable, err := t.table.UpdateRows(ctx, updated)
	if err != nil {
		return errhand.BuildDError("failed to update rows").AddCause(err).Build()
	}

	newRoot, err := doltdb.PutTable(ctx, t.db.root, t.db.root.VRW(), t.name, newTable)
	if err != nil {
		return errhand.BuildDError("failed to write table back to database").AddCause(err).Build()
	}

	t.table = newTable
	t.db.root = newRoot
	return nil
}

// AlterableDoltTable allows altering the schema of the table. It implements sql.AlterableTable.
type AlterableDoltTable struct {
	WritableDoltTable
}

var _ sql.AlterableTable = (*AlterableDoltTable)(nil)

// AddColumn implements sql.AlterableTable
func (t *AlterableDoltTable) AddColumn(ctx *sql.Context, column *sql.Column, order *sql.ColumnOrder) error {
	table, _, err := t.db.Root().GetTable(ctx, t.name)
	if err != nil {
		return err
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return err
	}

	tag := extractTag(column)
	if tag == schema.InvalidTag {
		tag = schema.AutoGenerateTag(sch)
	}

	col, err := SqlColToDoltCol(tag, column)
	if err != nil {
		return err
	}

	if col.IsPartOfPK {
		return errors.New("adding primary keys is not supported")
	}

	nullable := alterschema.NotNull
	if col.IsNullable() {
		nullable = alterschema.Null
	}

	var defVal types.Value
	if column.Default != nil {
		defVal, err = col.TypeInfo.ConvertValueToNomsValue(column.Default)
		if err != nil {
			return err
		}
	}

	updatedTable, err := alterschema.AddColumnToTable(ctx, table, col.Tag, col.Name, col.TypeInfo, nullable, defVal, orderToOrder(order))
	if err != nil {
		return err
	}

	newRoot, err := t.db.Root().PutTable(ctx, t.name, updatedTable)
	if err != nil {
		return err
	}

	t.db.SetRoot(newRoot)
	return nil
}

func orderToOrder(order *sql.ColumnOrder) *alterschema.ColumnOrder {
	if order == nil {
		return nil
	}
	return &alterschema.ColumnOrder{
		First: order.First,
		After: order.AfterColumn,
	}
}

// DropColumn implements sql.AlterableTable
func (t *AlterableDoltTable) DropColumn(ctx *sql.Context, columnName string) error {
	table, _, err := t.db.Root().GetTable(ctx, t.name)
	if err != nil {
		return err
	}

	updatedTable, err := alterschema.DropColumn(ctx, table, columnName)
	if err != nil {
		return err
	}

	newRoot, err := t.db.Root().PutTable(ctx, t.name, updatedTable)
	if err != nil {
		return err
	}

	t.db.SetRoot(newRoot)
	return nil
}

// ModifyColumn implements sql.AlterableTable
func (t *AlterableDoltTable) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	table, _, err := t.db.Root().GetTable(ctx, t.name)
	if err != nil {
		return err
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return err
	}

	existingCol, ok := sch.GetAllCols().GetByName(columnName)
	if !ok {
		panic(fmt.Sprintf("Column %s not found. This is a bug.", columnName))
	}

	tag := extractTag(column)
	if tag == schema.InvalidTag {
		tag = existingCol.Tag
	}

	col, err := SqlColToDoltCol(tag, column)
	if err != nil {
		return err
	}

	var defVal types.Value
	if column.Default != nil {
		defVal, err = col.TypeInfo.ConvertValueToNomsValue(column.Default)
		if err != nil {
			return err
		}
	}

	updatedTable, err := alterschema.ModifyColumn(ctx, table, existingCol, col, defVal, orderToOrder(order))
	if err != nil {
		return err
	}

	newRoot, err := t.db.Root().PutTable(ctx, t.name, updatedTable)
	if err != nil {
		return err
	}

	t.db.SetRoot(newRoot)
	return nil
}
