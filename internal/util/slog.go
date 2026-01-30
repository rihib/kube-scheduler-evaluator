package util

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

func SetupLogger(slogLevel slog.Level, evaluationID string) error {
	handlers := []slog.Handler{
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			AddSource: false,
			Level:     slogLevel,
		}),
	}

	logFile := fmt.Sprintf("log/%s.log", evaluationID)
	if logFile != "" {
		if err := os.MkdirAll(filepath.Dir(logFile), 0755); err != nil {
			return fmt.Errorf("util.SetupLogger: failed to create log directory: %w", err)
		}
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return fmt.Errorf("util.SetupLogger: failed to open log file: %w", err)
		}
		handlers = append(handlers, slog.NewJSONHandler(file, &slog.HandlerOptions{
			AddSource: false,
			Level:     slogLevel,
		}))
	}

	multiHandler := NewMultiHandler(handlers...)
	contextHandler := NewContextHandler(multiHandler)
	slog.SetDefault(slog.New(contextHandler))
	return nil
}

type MultiHandler struct {
	handlers []slog.Handler
}

func NewMultiHandler(handlers ...slog.Handler) *MultiHandler {
	return &MultiHandler{handlers: handlers}
}

func (h *MultiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h *MultiHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, handler := range h.handlers {
		if err := handler.Handle(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (h *MultiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newHandlers := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		newHandlers[i] = handler.WithAttrs(attrs)
	}
	return NewMultiHandler(newHandlers...)
}

func (h *MultiHandler) WithGroup(name string) slog.Handler {
	newHandlers := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		newHandlers[i] = handler.WithGroup(name)
	}
	return NewMultiHandler(newHandlers...)
}

type logAttrsKey struct{}

func WithLogAttrs(ctx context.Context, key string, value any) context.Context {
	attrs, _ := ctx.Value(logAttrsKey{}).([]slog.Attr)
	newAttrs := make([]slog.Attr, 0, len(attrs))
	found := false
	for _, attr := range attrs {
		if attr.Key == key {
			newAttrs = append(newAttrs, slog.Any(key, value))
			found = true
		} else {
			newAttrs = append(newAttrs, attr)
		}
	}
	if !found {
		newAttrs = append(newAttrs, slog.Any(key, value))
	}
	return context.WithValue(ctx, logAttrsKey{}, newAttrs)
}

func attrsFromContext(ctx context.Context) ([]slog.Attr, bool) {
	attrs, ok := ctx.Value(logAttrsKey{}).([]slog.Attr)
	return attrs, ok
}

type ContextHandler struct {
	slog.Handler
}

func NewContextHandler(h slog.Handler) *ContextHandler {
	return &ContextHandler{Handler: h}
}

func (h *ContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if attrs, ok := attrsFromContext(ctx); ok {
		r.AddAttrs(attrs...)
	}
	return h.Handler.Handle(ctx, r)
}
