package mixin

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTenantID_Bounds 指针语义边界：nil / 0 / 超界视为无租户标识，
// SetTenantID 正常落值。
func TestTenantID_Bounds(t *testing.T) {
	var m TenantID
	assert.Nil(t, m.GetTenantID())
	m.SetTenantID(7)
	got := m.GetTenantID()
	require.NotNil(t, got)
	assert.Equal(t, uint32(7), *got)

	zero := uint32(0)
	m.TenantID = &zero
	got = m.GetTenantID()
	require.NotNil(t, got)
	assert.Equal(t, uint32(0), *got, "pointer semantics keep the zero value distinct from nil")

	over := uint32(math.MaxUint32)
	m.TenantID = &over
	got = m.GetTenantID()
	require.NotNil(t, got)
	assert.Equal(t, uint32(math.MaxUint32), *got)
}
