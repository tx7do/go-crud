// Package log provides a legacy-compatible logging shim backed by
// github.com/tx7do/go-wind/log.
//
// It re-exports the subset of the legacy log API (Helper, NewHelper,
// DefaultLogger, package-level Errorf/Warn/Info/Debug/Fatal, With,
// NewStdLogger) that go-crud depends on, so callers can use this package
// without other changes.
//
// The backend is the go-wind global logger (see windlog.SetLogger). All calls
// use context.Background(): this API has no context parameter, so the
// request-scoped context that go-wind carries is not preserved here. Field
// attachment via With is forwarded to go-wind and preserved along the chain.
package log

import (
	"context"
	"fmt"
	"io"
	"os"

	windlog "github.com/tx7do/go-wind/log"
)

// Logger is the shim logger interface. It exposes per-level emit
// methods plus With; implementations route to the go-wind backend or to an
// io.Writer.
type Logger interface {
	With(args ...any) Logger

	Debug(args ...any)
	Debugf(format string, args ...any)
	Info(args ...any)
	Infof(format string, args ...any)
	Warn(args ...any)
	Warnf(format string, args ...any)
	Error(args ...any)
	Errorf(format string, args ...any)
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

// Helper wraps a Logger to provide the same method set, matching the
// log.Helper shape.
type Helper struct {
	l Logger
}

// NewHelper returns a Helper bound to the given Logger.
func NewHelper(l Logger) *Helper {
	return &Helper{l: l}
}

// Package-level entrypoints. Errorf/Warn/... are free functions backed by the
// global logger; these route through DefaultLogger, which proxies the live
// go-wind global logger.

func Debug(args ...any)            { DefaultLogger.Debug(args...) }
func Debugf(f string, a ...any)    { DefaultLogger.Debugf(f, a...) }
func Info(args ...any)             { DefaultLogger.Info(args...) }
func Infof(f string, a ...any)     { DefaultLogger.Infof(f, a...) }
func Warn(args ...any)             { DefaultLogger.Warn(args...) }
func Warnf(f string, a ...any)     { DefaultLogger.Warnf(f, a...) }
func Error(args ...any)            { DefaultLogger.Error(args...) }
func Errorf(f string, a ...any)    { DefaultLogger.Errorf(f, a...) }
func Fatal(args ...any)            { DefaultLogger.Fatal(args...) }
func Fatalf(f string, a ...any)    { DefaultLogger.Fatalf(f, a...) }

func (h *Helper) Debug(args ...any)       { h.l.Debug(args...) }
func (h *Helper) Debugf(f string, a ...any) { h.l.Debugf(f, a...) }
func (h *Helper) Info(args ...any)        { h.l.Info(args...) }
func (h *Helper) Infof(f string, a ...any)  { h.l.Infof(f, a...) }
func (h *Helper) Warn(args ...any)        { h.l.Warn(args...) }
func (h *Helper) Warnf(f string, a ...any) { h.l.Warnf(f, a...) }
func (h *Helper) Error(args ...any)       { h.l.Error(args...) }
func (h *Helper) Errorf(f string, a ...any) { h.l.Errorf(f, a...) }
func (h *Helper) Fatal(args ...any)       { h.l.Fatal(args...) }
func (h *Helper) Fatalf(f string, a ...any) { h.l.Fatalf(f, a...) }

// DefaultLogger proxies the current go-wind global logger on every call, so
// changes made via windlog.SetLogger are reflected without reassignment.
var DefaultLogger Logger = defaultLogger{}

// With returns a Logger with the given key/value pairs attached.
func With(l Logger, args ...any) Logger { return l.With(args...) }

// NewStdLogger returns a Logger that writes "LEVEL: msg" lines to w. This
// mirrors the legacy log.NewStdLogger, used by tests that want log output on
// a controlled sink.
func NewStdLogger(w io.Writer) Logger { return &writerLogger{w: w} }

// defaultLogger forwards to the live go-wind global logger. It holds no state
// so it always reflects the most recent SetLogger.
type defaultLogger struct{}

func (defaultLogger) With(args ...any) Logger {
	return &boundLogger{w: windlog.GetLogger().With(args...)}
}

func (defaultLogger) Debug(args ...any) {
	windlog.GetLogger().Debug(context.Background(), fmt.Sprint(args...))
}
func (defaultLogger) Debugf(f string, a ...any) {
	windlog.GetLogger().Debug(context.Background(), f, a...)
}
func (defaultLogger) Info(args ...any) {
	windlog.GetLogger().Info(context.Background(), fmt.Sprint(args...))
}
func (defaultLogger) Infof(f string, a ...any) {
	windlog.GetLogger().Info(context.Background(), f, a...)
}
func (defaultLogger) Warn(args ...any) {
	windlog.GetLogger().Warn(context.Background(), fmt.Sprint(args...))
}
func (defaultLogger) Warnf(f string, a ...any) {
	windlog.GetLogger().Warn(context.Background(), f, a...)
}
func (defaultLogger) Error(args ...any) {
	windlog.GetLogger().Error(context.Background(), fmt.Sprint(args...))
}
func (defaultLogger) Errorf(f string, a ...any) {
	windlog.GetLogger().Error(context.Background(), f, a...)
}
func (defaultLogger) Fatal(args ...any) {
	windlog.GetLogger().Error(context.Background(), fmt.Sprint(args...))
	os.Exit(1)
}
func (defaultLogger) Fatalf(f string, a ...any) {
	windlog.GetLogger().Error(context.Background(), f, a...)
	os.Exit(1)
}

// boundLogger wraps a specific go-wind Logger instance, typically one returned
// by With with fields attached. It routes calls to that instance.
type boundLogger struct {
	w windlog.Logger
}

func (b *boundLogger) With(args ...any) Logger {
	return &boundLogger{w: b.w.With(args...)}
}

func (b *boundLogger) Debug(args ...any)       { b.w.Debug(context.Background(), fmt.Sprint(args...)) }
func (b *boundLogger) Debugf(f string, a ...any) { b.w.Debug(context.Background(), f, a...) }
func (b *boundLogger) Info(args ...any)        { b.w.Info(context.Background(), fmt.Sprint(args...)) }
func (b *boundLogger) Infof(f string, a ...any)  { b.w.Info(context.Background(), f, a...) }
func (b *boundLogger) Warn(args ...any)        { b.w.Warn(context.Background(), fmt.Sprint(args...)) }
func (b *boundLogger) Warnf(f string, a ...any) { b.w.Warn(context.Background(), f, a...) }
func (b *boundLogger) Error(args ...any)       { b.w.Error(context.Background(), fmt.Sprint(args...)) }
func (b *boundLogger) Errorf(f string, a ...any) { b.w.Error(context.Background(), f, a...) }
func (b *boundLogger) Fatal(args ...any) {
	b.w.Error(context.Background(), fmt.Sprint(args...))
	os.Exit(1)
}
func (b *boundLogger) Fatalf(f string, a ...any) {
	b.w.Error(context.Background(), f, a...)
	os.Exit(1)
}

// writerLogger emits plain text lines to an io.Writer, reproducing the legacy
// log.NewStdLogger behavior. With is a no-op: structured fields are ignored.
type writerLogger struct {
	w io.Writer
}

func (l *writerLogger) With(_ ...any) Logger { return l }

func (l *writerLogger) write(level, msg string) {
	fmt.Fprintf(l.w, "%s: %s\n", level, msg)
}

func (l *writerLogger) Debug(args ...any)       { l.write("DEBUG", fmt.Sprint(args...)) }
func (l *writerLogger) Debugf(f string, a ...any) { l.write("DEBUG", fmt.Sprintf(f, a...)) }
func (l *writerLogger) Info(args ...any)        { l.write("INFO", fmt.Sprint(args...)) }
func (l *writerLogger) Infof(f string, a ...any)  { l.write("INFO", fmt.Sprintf(f, a...)) }
func (l *writerLogger) Warn(args ...any)        { l.write("WARN", fmt.Sprint(args...)) }
func (l *writerLogger) Warnf(f string, a ...any) { l.write("WARN", fmt.Sprintf(f, a...)) }
func (l *writerLogger) Error(args ...any)       { l.write("ERROR", fmt.Sprint(args...)) }
func (l *writerLogger) Errorf(f string, a ...any) { l.write("ERROR", fmt.Sprintf(f, a...)) }
func (l *writerLogger) Fatal(args ...any) {
	l.write("ERROR", fmt.Sprint(args...))
	os.Exit(1)
}
func (l *writerLogger) Fatalf(f string, a ...any) {
	l.write("ERROR", fmt.Sprintf(f, a...))
	os.Exit(1)
}
