package mixin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTenantID_RoundTrip 指针语义：未设置 → nil；设置后可读回。
func TestTenantID_RoundTrip(t *testing.T) {
	var m TenantID
	assert.Nil(t, m.GetTenantID())
	m.SetTenantID(7)
	got := m.GetTenantID()
	require.NotNil(t, got)
	assert.Equal(t, uint32(7), *got)
}
