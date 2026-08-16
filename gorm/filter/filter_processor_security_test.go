package filter

import (
	"strings"
	"testing"

	"gorm.io/gorm"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// TestProcess_HostileFieldNeutralized 验证携带 SQL 注入载荷的过滤字段名
// 无法进入 WHERE：要么被拒绝（AddError，fail-closed），要么经归一化后载荷被破坏。
func TestProcess_HostileFieldNeutralized(t *testing.T) {
	db := openTestDB(t)
	poc := NewProcessor()

	hostile := []string{
		"name` --",
		"(select version())",
		"id = 1 OR 1=1",
		"a'b",
		"1abc", // 数字开头：不合法标识符，必须报错
	}
	// 注意：gorm 自身会输出 `users`（表名反引号）和 =（比较符），
	// 这里只检查与注入载荷相关的字符。
	dangerous := []string{"(", "'", "--", ";"}

	for _, field := range hostile {
		tx := db.Session(&gorm.Session{DryRun: true})
		tx = poc.Process(tx.Model(&User{}), paginationV1.Operator_EQ, field, "v", nil)

		var out []User
		err := tx.Find(&out).Error
		if err != nil {
			continue // 被拒绝，安全
		}
		sql := tx.Statement.SQL.String()
		for _, d := range dangerous {
			if strings.Contains(sql, d) {
				t.Errorf("hostile field %q leaked %q into SQL: %q", field, d, sql)
			}
		}
	}
}

// TestBuildSelectors_HostileFieldNeutralized 在 StructuredFilter 整链路上验证：
// 敌意字段名经闭包应用后，最终 SQL 不含任何注入载荷。
func TestBuildSelectors_HostileFieldNeutralized(t *testing.T) {
	db := openTestDB(t)
	sf := NewStructuredFilter()

	hostile := []string{
		"name` --",
		"(select version())",
		"id = 1 OR 1=1",
		"preferences.x' = 'y' OR '1'='1", // 敌意 JSON key：JsonbFieldExpr 校验失败，条件不落入 SQL
	}

	for _, field := range hostile {
		expr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{Field: field, Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "v"}},
			},
		}
		sels, err := sf.BuildSelectors(expr)
		if err != nil {
			continue // 直接报错也是安全结局
		}

		tx := db.Session(&gorm.Session{DryRun: true}).Model(&User{})
		for _, s := range sels {
			tx = s(tx)
		}
		var out []User
		if err := tx.Find(&out).Error; err != nil {
			continue // AddError 拒绝，安全
		}
		sql := tx.Statement.SQL.String()
		for _, d := range []string{"(", "'", "--", ";"} {
			if strings.Contains(sql, d) {
				t.Errorf("hostile field %q leaked %q into SQL: %q", field, d, sql)
			}
		}
		if strings.Contains(sql, "version()") {
			t.Errorf("hostile field %q leaked subquery into SQL: %q", field, sql)
		}
	}
}

// TestProcess_ValidFieldAndJsonExprStillWork 验证合法列名与 JSON 表达式不受守卫影响。
func TestProcess_ValidFieldAndJsonExprStillWork(t *testing.T) {
	db := openTestDB(t)
	poc := NewProcessor()

	tx := db.Session(&gorm.Session{DryRun: true})
	tx = poc.Process(tx.Model(&User{}), paginationV1.Operator_EQ, "Name", "bob", nil)
	var out []User
	if err := tx.Find(&out).Error; err != nil {
		t.Fatalf("valid field should not error: %v", err)
	}
	if sql := tx.Statement.SQL.String(); !strings.Contains(sql, "name = ?") {
		t.Errorf("expected 'name = ?' in SQL, got %q", sql)
	}

	// JsonbFieldExpr 产出的表达式应原样通过（不被 snake_case 拆散、不被守卫拒绝）
	expr := poc.JsonbField(db, "daily_email", "preferences")
	if expr == "" {
		t.Fatal("JsonbFieldExpr should produce expression for valid input")
	}
	tx2 := db.Session(&gorm.Session{DryRun: true})
	tx2 = poc.Process(tx2.Model(&User{}), paginationV1.Operator_EQ, expr, "1", nil)
	var out2 []User
	if err := tx2.Find(&out2).Error; err != nil {
		t.Fatalf("json expr field should not error: %v", err)
	}
	if sql := tx2.Statement.SQL.String(); !strings.Contains(sql, "preferences ->> 'daily_email'") {
		t.Errorf("expected JSON expression preserved in SQL, got %q", sql)
	}
}

// TestBuildSelectors_OrGroupActuallyFilters 验证 OR 组过滤真实生效：
// 此前 OR 用 func 闭包传给 gorm，闭包永不执行导致整个 OR 过滤静默消失
// （无过滤查询）。现在应生成 (a = ? OR b = ?) 并正确过滤行。
func TestBuildSelectors_OrGroupActuallyFilters(t *testing.T) {
	db := openTestDB(t)
	if err := db.Create(&[]User{
		{Name: "alice", Status: "active"},
		{Name: "bob", Status: "disabled"},
		{Name: "carol", Status: "pending"},
	}).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	expr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_OR,
		Conditions: []*paginationV1.FilterCondition{
			{Field: "name", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "alice"}},
			{Field: "status", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "disabled"}},
		},
	}
	sels, err := NewStructuredFilter().BuildSelectors(expr)
	if err != nil {
		t.Fatalf("BuildSelectors: %v", err)
	}

	tx := db.Model(&User{})
	for _, s := range sels {
		tx = s(tx)
	}
	var rows []User
	if err := tx.Find(&rows).Error; err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("OR filter must return 2 rows (alice or disabled), got %d: %+v", len(rows), rows)
	}
}

// TestBuildSelectors_OrGroupHostileFieldFailsClosed 验证 OR 组内的非法字段
// 使整个查询报错（fail-closed），而非静默丢弃过滤。
func TestBuildSelectors_OrGroupHostileFieldFailsClosed(t *testing.T) {
	db := openTestDB(t)

	expr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_OR,
		Conditions: []*paginationV1.FilterCondition{
			{Field: "id) OR (1=1 --", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "1"}},
		},
	}
	sels, err := NewStructuredFilter().BuildSelectors(expr)
	if err != nil {
		t.Fatalf("BuildSelectors: %v", err)
	}

	tx := db.Model(&User{})
	for _, s := range sels {
		tx = s(tx)
	}
	var rows []User
	err = tx.Find(&rows).Error
	if err == nil {
		t.Fatalf("hostile field in OR group must fail the query, got %d rows", len(rows))
	}
}

// TestBuildSelectors_EmptyValueSkipped 验证空值条件被跳过（回归保护）：
// 892425b 曾使空值生成 field = ” / NOT (field = ”) / LIKE '%%'，
// 现应与各算子方法一致：空值不添加条件。
func TestBuildSelectors_EmptyValueSkipped(t *testing.T) {
	db := openTestDB(t)
	if err := db.Create(&[]User{
		{Name: "alice", Status: "active"},
		{Name: "", Status: "active"},
	}).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	// EQ 空值：不添加条件 → 返回全部行（含 name 为空的行）
	expr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{Field: "name", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: ""}},
		},
	}
	sels, err := NewStructuredFilter().BuildSelectors(expr)
	if err != nil {
		t.Fatalf("BuildSelectors: %v", err)
	}
	tx := db.Model(&User{})
	for _, s := range sels {
		tx = s(tx)
	}
	var rows []User
	if err := tx.Find(&rows).Error; err != nil {
		t.Fatalf("query: %v", err)
	}
	// 空值条件被跳过 = 无过滤：name 为空的记录也应返回
	foundEmpty := false
	for _, r := range rows {
		if r.Name == "" {
			foundEmpty = true
			break
		}
	}
	if !foundEmpty {
		t.Fatalf("empty-value EQ must be skipped (no filter), got rows without empty-name: %+v", rows)
	}

	// CONTAINS 空值：不得生成 LIKE '%%'
	expr2 := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{Field: "name", Op: paginationV1.Operator_CONTAINS, ValueOneof: &paginationV1.FilterCondition_Value{Value: ""}},
		},
	}
	sels2, err := NewStructuredFilter().BuildSelectors(expr2)
	if err != nil {
		t.Fatalf("BuildSelectors: %v", err)
	}
	tx2 := db.Session(&gorm.Session{DryRun: true}).Model(&User{})
	for _, s := range sels2 {
		tx2 = s(tx2)
	}
	var out []User
	if err := tx2.Find(&out).Error; err != nil {
		t.Fatalf("dry-run: %v", err)
	}
	if strings.Contains(tx2.Statement.SQL.String(), "LIKE") {
		t.Errorf("empty-value CONTAINS must be skipped, got SQL: %q", tx2.Statement.SQL.String())
	}
}

// TestBuildSelectors_InvalidFieldEmptyValueFailsClosed 验证非法字段即使空值
// 也报错（校验在空值跳过之前，与各算子方法一致）。
func TestBuildSelectors_InvalidFieldEmptyValueFailsClosed(t *testing.T) {
	db := openTestDB(t)
	expr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{Field: "idÿ' OR '1'='1", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: ""}},
		},
	}
	sels, err := NewStructuredFilter().BuildSelectors(expr)
	if err != nil {
		t.Fatalf("BuildSelectors: %v", err)
	}
	tx := db.Model(&User{})
	for _, s := range sels {
		tx = s(tx)
	}
	var rows []User
	if err := tx.Find(&rows).Error; err == nil {
		t.Fatalf("hostile field with empty value must fail the query, got %d rows", len(rows))
	}
}
