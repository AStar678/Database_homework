package integrity

import (
	"testing"

	"github.com/ruanko/dbms/pkg/catalog"
	"github.com/ruanko/dbms/pkg/common"
	"github.com/ruanko/dbms/pkg/storage"
	"github.com/ruanko/dbms/pkg/types"
)

func TestCheckInsertAcceptsValidRecord(t *testing.T) {
	checker := NewChecker()
	fields := testFields()
	record := testRecord(1, "Alice")

	if err := checker.CheckInsert(record, fields, nil); err != nil {
		t.Fatalf("expected valid record to pass integrity check, got %v", err)
	}
}

func TestCheckInsertRejectsNotNullViolation(t *testing.T) {
	checker := NewChecker()
	fields := testFields()
	record := &storage.Record{
		Values: []types.Value{
			&types.IntValue{V: 1},
			&types.NullValue{},
		},
	}

	err := checker.CheckInsert(record, fields, nil)
	assertDBErrorCode(t, err, 4001)
}

func TestCheckInsertRejectsPrimaryKeyNull(t *testing.T) {
	checker := NewChecker()
	fields := testFields()
	record := &storage.Record{
		Values: []types.Value{
			&types.NullValue{},
			&types.VarcharValue{V: "Alice"},
		},
	}

	err := checker.CheckInsert(record, fields, nil)
	assertDBErrorCode(t, err, 4002)
}

func TestCheckInsertRejectsUniqueViolation(t *testing.T) {
	checker := NewChecker()
	fields := testFields()
	record := testRecord(1, "Alice")
	existingRecords := []*storage.Record{
		testRecord(1, "Bob"),
	}

	err := checker.CheckInsert(record, fields, existingRecords)
	assertDBErrorCode(t, err, 4003)
}

func TestCheckUpdateUsesInsertIntegrityRules(t *testing.T) {
	checker := NewChecker()
	fields := testFields()
	record := testRecord(2, "Alice")
	existingRecords := []*storage.Record{
		testRecord(1, "Alice"),
	}

	err := checker.CheckUpdate(record, fields, existingRecords)
	assertDBErrorCode(t, err, 4003)
}

func testFields() []*catalog.FieldBlock {
	return []*catalog.FieldBlock{
		{
			Name:        "id",
			Type:        int32(types.INTEGER),
			Integrities: catalog.ConstraintPrimaryKey,
		},
		{
			Name:        "name",
			Type:        int32(types.VARCHAR),
			Param:       20,
			Integrities: catalog.ConstraintNotNull | catalog.ConstraintUnique,
		},
	}
}

func testRecord(id int32, name string) *storage.Record {
	return &storage.Record{
		Values: []types.Value{
			&types.IntValue{V: id},
			&types.VarcharValue{V: name},
		},
	}
}

func assertDBErrorCode(t *testing.T, err error, wantCode int) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected DB error code %d, got nil", wantCode)
	}

	dbErr, ok := err.(*common.DBError)
	if !ok {
		t.Fatalf("expected *common.DBError, got %T: %v", err, err)
	}
	if dbErr.Code != wantCode {
		t.Fatalf("expected DB error code %d, got %d: %v", wantCode, dbErr.Code, err)
	}
}
