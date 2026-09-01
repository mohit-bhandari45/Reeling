package job

import "testing"

func TestPostgresSaveManual(t *testing.T) {
	connString := "postgres://reeling:reeling@localhost:5432/reeling"
	db, err := NewPostgresConn(connString)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewPostgresStore(db)

	j := &Job{
		ID:       "test-job-1",
		InputKey: "some-input-key.mp4",
		Status:   StatusQueued,
	}

	if err := store.Save(j); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresGetManual(t *testing.T) {
	connString := "postgres://reeling:reeling@localhost:5432/reeling"
	db, err := NewPostgresConn(connString)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewPostgresStore(db)

	j, err := store.Get("test-job-1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("got job: %+v", j)

	_, err = store.Get("does-not-exist")
	if err == nil {
		t.Fatal("expected error for missing job, got nil")
	}
	t.Logf("correctly got error for missing job: %v", err)
}