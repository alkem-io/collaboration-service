package inline

import (
	"context"
	"errors"
	"testing"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

func TestPutGetRoundTrip(t *testing.T) {
	s := New()
	ctx := context.Background()
	want := []byte{0x01, 0x02, 0x03}

	if _, err := s.Put(ctx, "p1", "", want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(ctx, "p1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("Get = %v, want %v", got, want)
	}
}

func TestGetMissingIsNotFound(t *testing.T) {
	if _, err := New().Get(context.Background(), "absent"); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Get(absent) error = %v, want ErrNotFound", err)
	}
}

func TestPutCopiesInput(t *testing.T) {
	s := New()
	ctx := context.Background()
	in := []byte{0xAA}
	if _, err := s.Put(ctx, "p", "", in); err != nil {
		t.Fatalf("Put: %v", err)
	}
	in[0] = 0xBB // mutate caller's slice after Put

	got, err := s.Get(ctx, "p")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got[0] != 0xAA {
		t.Errorf("stored byte = %#x, want 0xAA (Put must defensively copy)", got[0])
	}
}

func TestDeleteIsIdempotent(t *testing.T) {
	s := New()
	ctx := context.Background()
	if err := s.Delete(ctx, "never-existed"); err != nil {
		t.Errorf("Delete(absent) = %v, want nil", err)
	}
}
