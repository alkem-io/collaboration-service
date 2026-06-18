package inmemory

import (
	"context"
	"errors"
	"testing"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

func TestSaveBumpsVersionOnUpsert(t *testing.T) {
	s := New()
	ctx := context.Background()
	meta := model.Metadata{ID: "d1", ContentType: model.ContentTypeMemo}

	if err := s.Save(ctx, meta); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	first, _ := s.Load(ctx, "d1")
	if first.Version != 1 {
		t.Errorf("first version = %d, want 1", first.Version)
	}

	if err := s.Save(ctx, meta); err != nil {
		t.Fatalf("second Save: %v", err)
	}
	second, _ := s.Load(ctx, "d1")
	if second.Version != 2 {
		t.Errorf("second version = %d, want 2", second.Version)
	}
	if !second.CreatedAt.Equal(first.CreatedAt) {
		t.Errorf("CreatedAt changed across upsert: %v -> %v", first.CreatedAt, second.CreatedAt)
	}
}

func TestLoadMissingIsNotFound(t *testing.T) {
	if _, err := New().Load(context.Background(), "absent"); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Load(absent) error = %v, want ErrNotFound", err)
	}
}
