package gorm

import "sync"

var (
	registeredMigrateModelsMu sync.RWMutex
	registeredMigrateModels   []any
)

// RegisterMigrateModel 注册用于数据库迁移的数据库模型
func RegisterMigrateModel(model any) {
	if model == nil {
		return
	}
	registeredMigrateModelsMu.Lock()
	defer registeredMigrateModelsMu.Unlock()
	registeredMigrateModels = append(registeredMigrateModels, model)
}

// RegisterMigrateModels 注册用于数据库迁移的数据库模型
func RegisterMigrateModels(models ...any) {
	if len(models) == 0 {
		return
	}
	registeredMigrateModelsMu.Lock()
	defer registeredMigrateModelsMu.Unlock()
	registeredMigrateModels = append(registeredMigrateModels, models...)
}

// getRegisteredMigrateModels 返回已注册的包级模型副本（线程安全）
func getRegisteredMigrateModels() []any {
	registeredMigrateModelsMu.RLock()
	defer registeredMigrateModelsMu.RUnlock()
	if len(registeredMigrateModels) == 0 {
		return nil
	}
	dup := make([]any, len(registeredMigrateModels))
	copy(dup, registeredMigrateModels)
	return dup
}
