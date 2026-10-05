package milvus

import (
	"fmt"
	"reflect"

	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

// ─────────────────────────────────────────────────────────────────────────────
// 列 → 实体还原。
//
// 官方 SDK 的 ResultSet.Unmarshal 经 parseCandidates 取行字段映射，而
// parseCandidates 跳过匿名嵌入字段（见 schema.go 头注），mixin 中的
// tenant_id 将无法还原。本还原器按本模块自有的拍平映射表（collectFieldSpecs）
// 逐列填充，主键列（search 的 IDs）与 tenant_id 列一并还原，语义与写侧
// （reflectValueCandi）对称。
// ─────────────────────────────────────────────────────────────────────────────

// entityFromColumns 把结果集中的第 rowIdx 行还原进实体。
// 列缺失或类型不符的字段保持零值（跳过而非报错，容忍 schema 演进）。
func entityFromColumns[ENTITY any](cols []entity.Column, rowIdx int, dst *ENTITY) error {
	if dst == nil {
		return fmt.Errorf("%w: nil entity destination", ErrColumnConversion)
	}
	var e ENTITY
	specs, err := collectFieldSpecs(reflect.TypeOf(&e).Elem())
	if err != nil {
		return err
	}
	rv := reflect.ValueOf(dst).Elem()
	for _, spec := range specs {
		if spec.isVector {
			// 向量数据不回读（检索场景无需，且 query 不输出向量列）。
			continue
		}
		var col entity.Column
		for _, c := range cols {
			if c != nil && c.Name() == spec.name {
				col = c
				break
			}
		}
		if col == nil {
			continue
		}
		v, gerr := col.Get(rowIdx)
		if gerr != nil {
			continue
		}
		setFieldValue(rv.FieldByIndex(spec.path), v)
	}
	return nil
}

// setFieldValue 按列值类型填充目标字段（列值类型与字段类型须精确一致）。
func setFieldValue(fv reflect.Value, v any) {
	if !fv.CanSet() {
		return
	}
	switch val := v.(type) {
	case bool:
		if fv.Kind() == reflect.Bool {
			fv.SetBool(val)
		}
	case int8:
		if fv.Kind() == reflect.Int8 {
			fv.SetInt(int64(val))
		}
	case int16:
		if fv.Kind() == reflect.Int16 {
			fv.SetInt(int64(val))
		}
	case int32:
		if fv.Kind() == reflect.Int32 {
			fv.SetInt(int64(val))
		}
	case int64:
		if fv.Kind() == reflect.Int64 {
			fv.SetInt(val)
		}
	case float32:
		if fv.Kind() == reflect.Float32 {
			fv.SetFloat(float64(val))
		}
	case float64:
		if fv.Kind() == reflect.Float64 {
			fv.SetFloat(val)
		}
	case string:
		if fv.Kind() == reflect.String {
			fv.SetString(val)
		}
	}
}
