package mixin

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTenantID_Bounds int64 → uint32 有损转换边界：
// 非正数与超出 uint32 范围的值视为无租户标识。
func TestTenantID_Bounds(t *testing.T) {
	var m TenantID
	assert.Nil(t, m.GetTenantID())
	m.TenantID = -1
	assert.Nil(t, m.GetTenantID())
	m.TenantID = int64(math.MaxUint32) + 1
	assert.Nil(t, m.GetTenantID())
	m.SetTenantID(7)
	got := m.GetTenantID()
	require.NotNil(t, got)
	assert.Equal(t, uint32(7), *got)
	m.TenantID = math.MaxUint32
	got = m.GetTenantID()
	require.NotNil(t, got)
	assert.Equal(t, uint32(math.MaxUint32), *got)
}
