package entgo

import (
	"context"
	"fmt"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/go-crud/entgo/ent"
)

type TxFunc func(ctx context.Context, tx *ent.Tx) error

// RunInTx 在 data 层统一管理 ent 事务。
// entClient/log 必须由调用方传入以便复用该 helper。
// fn 在同一 tx 上执行应用逻辑：fn(ctx, tx) error。
// 规则：
// - 如果开启 tx 失败，记录日志并返回内部错误（示例使用 identityV1.ErrorInternalServerError，实际可按 repo 需要替换）
// - 如果 fn 返回 error，则回滚并返回该 error（Preserve business error）
// - 如果 fn panic，则回滚并 panic 上抛（保持原行为）
// - 如果 fn 返回 nil，则提交 tx；如果 commit 失败，记录日志并返回内部错误
func RunInTx(ctx context.Context, entClient *EntClient[*ent.Client], logger *log.Helper, fn TxFunc) (err error) {
	var tx *ent.Tx
	tx, err = entClient.Client().Tx(ctx)
	if err != nil {
		logger.Errorf("start transaction failed: %s", err.Error())
		return fmt.Errorf("start transaction failed")
	}

	// ensure rollback on panic
	defer func() {
		if p := recover(); p != nil {
			// try rollback, but keep panic for caller
			if rbErr := tx.Rollback(); rbErr != nil {
				logger.Errorf("transaction rollback failed after panic: %s", rbErr.Error())
			}
			panic(p)
		}
	}()

	// execute callback
	if err = fn(ctx, tx); err != nil {
		// business error -> rollback and return it
		if rbErr := tx.Rollback(); rbErr != nil {
			logger.Errorf("transaction rollback failed: %s", rbErr.Error())
		}
		return err
	}

	// commit if callback succeeded
	if commitErr := tx.Commit(); commitErr != nil {
		logger.Errorf("transaction commit failed: %s", commitErr.Error())
		return fmt.Errorf("transaction commit failed")
	}

	return nil
}
