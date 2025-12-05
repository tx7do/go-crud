package gorm

import "sync"

var (
	registeredMigrateModelsMu sync.RWMutex
	registeredMigrateModels   []interface{}
)

// RegisterMigrateModel 注册用于数据库迁移的数据库模型
func RegisterMigrateModel(model interface{}) {
	if model == nil {
		return
	}
	registeredMigrateModelsMu.Lock()
	defer registeredMigrateModelsMu.Unlock()
	registeredMigrateModels = append(registeredMigrateModels, model)
}

// RegisterMigrateModels 注册用于数据库迁移的数据库模型
func RegisterMigrateModels(models ...interface{}) {
	if len(models) == 0 {
		return
	}
	registeredMigrateModelsMu.Lock()
	defer registeredMigrateModelsMu.Unlock()
	registeredMigrateModels = append(registeredMigrateModels, models...)
}

// getRegisteredMigrateModels 返回已注册的包级模型副本（线程安全）
func getRegisteredMigrateModels() []interface{} {
	registeredMigrateModelsMu.RLock()
	defer registeredMigrateModelsMu.RUnlock()
	if len(registeredMigrateModels) == 0 {
		return nil
	}
	dup := make([]interface{}, len(registeredMigrateModels))
	copy(dup, registeredMigrateModels)
	return dup
}
