package field

import (
	"testing"

	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tx7do/go-crud/mongodb/query"
)

// TestBuildSelector_ProjectionApplied 验证 FieldMask 真正设置 projection
// （此前反引号包裹被 fieldNameRegexp 拒绝，投影静默失效返回全文档）。
func TestBuildSelector_ProjectionApplied(t *testing.T) {
	b := query.NewQueryBuilder()
	_, err := NewFieldSelector().BuildSelector(b, []string{"name", "age"})
	if err != nil {
		t.Fatalf("BuildSelector: %v", err)
	}
	_, opts := b.Build()
	if opts == nil || opts.Projection == nil {
		t.Fatal("projection must be set")
	}
	proj, ok := opts.Projection.(bsonV2.M)
	if !ok {
		t.Fatalf("projection type: %T", opts.Projection)
	}
	if _, ok := proj["name"]; !ok {
		t.Errorf("projection must include name, got %v", proj)
	}
	if _, ok := proj["age"]; !ok {
		t.Errorf("projection must include age, got %v", proj)
	}
}

// TestNormalizePaths_InvalidDropped 验证含元字符路径整条置空。
func TestNormalizePaths_InvalidDropped(t *testing.T) {
	out := NormalizePaths([]string{"name", "user.age", "id) OR (1=1 --", "a`,b"})
	if out[0] != "name" || out[1] != "user.age" {
		t.Errorf("valid paths must pass through, got %v", out)
	}
	if out[2] != "" || out[3] != "" {
		t.Errorf("hostile paths must be dropped, got %v", out)
	}
}
