package log

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

// Success and failure markers.
const (
	success = "\u2713"
	failed  = "\u2717"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStore(t *testing.T) {

	t.Run("testAppendRead", testAppendRead)
	t.Run("testClose", testClose)
}

func testAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	if err != nil {
		t.Fatalf("\t\t\t%s Should expect no error when creating temp file.", failed)
	}
	defer os.Remove(f.Name())

	s, err := newStore(f)
	if err != nil {
		t.Fatalf("\t\t\t%s Should expect no error when creating a store.", failed)
	}

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// We create the store again and we test reading from it again, to verify
	// that our service will recover its state after restart.
	s, err = newStore(f)
	if err != nil {
		t.Fatalf("\t\t\t%s Should expect no error when creating the store again.", failed)
	}
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()

	t.Log("Given the need to append a record to the store.")
	{
		for i := uint64(1); i < 4; i++ {
			t.Logf("\t\tTest %d:\t When appending a new record:", i)
			{
				n, pos, err := s.Append(write)

				if err != nil {
					t.Fatalf("\t\t\t%s Should expect no error: %s", failed, err.Error())
				}
				t.Logf("\t\t\t%s Should expect no error.", success)

				if pos+n != width*i {
					t.Fatalf("\t\t\t%s Should expect record to be appended.", failed)
				}
				t.Logf("\t\t\t%s Should expect record to be appended.", success)
			}
		}
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()

	t.Log("Given the need to read a record from the store.")
	{

		var pos uint64
		for i := uint64(1); i < 4; i++ {
			t.Logf("\t\tTest %d:\t When reading a record from the store:", i)
			{
				read, err := s.Read(pos)

				if err != nil {
					t.Fatalf("\t\t\t%s Should expect no error: %s", failed, err.Error())
				}
				t.Logf("\t\t\t%s Should expect no error.", success)

				cmp := bytes.Compare(write, read)
				if cmp != 0 {
					t.Fatalf("\t\t\t%s Should expect: \"%s\" got: \"%s\"", failed, string(write), string(read))
				}
				t.Logf("\t\t\t%s Should read the expected record.", success)

				pos += width
			}
		}
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()

	t.Log("Given the need to read a sequence of bytes from a specific offset.")
	{
		for i, off := uint64(1), int64(0); i < 4; i++ {
			t.Logf("\t\tTest %d:\t When reading a sequence:", i)
			{

				b := make([]byte, lenWidth)
				n, err := s.ReadAt(b, off)

				if err != nil {
					t.Fatalf("\t\t\t%s Should expect no error: %s", failed, err.Error())
				}
				t.Logf("\t\t\t%s Should expect no error.", success)

				if lenWidth != n {
					t.Fatalf("\t\t\t%s Should expect %d bytes to be read but got %d", failed, lenWidth, n)
				}
				size := enc.Uint64(b)
				t.Logf("\t\t\t%s Should expect %d bytes to be read, value: %d", success, lenWidth, size)
				off += int64(n)

				b = make([]byte, size)
				n, err = s.ReadAt(b, off)

				if err != nil {
					t.Fatalf("\t\t\t%s Should expect no error: %s", failed, err.Error())
				}
				t.Logf("\t\t\t%s Should expect no error.", success)

				cmp := bytes.Compare(write, b)
				if cmp != 0 {
					t.Fatalf("\t\t\t%s Should expect %s to be read: got %s", failed, string(write), string(b))
				}
				t.Logf("\t\t\t%s Should expect %s to be read", success, string(write))

				if int(size) != n {
					t.Fatalf("\t\t\t%s Should expect %s to be read.", failed, string(write))
				}
				off += int64(n)
			}
		}
	}
}

func testClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	if err != nil {
		t.Fatalf("\t\t\t%s Should expect no error when creating temp file.", failed)
	}
	defer os.Remove(f.Name())

	s, err := newStore(f)
	if err != nil {
		t.Fatalf("\t\t\t%s Should expect no error when creating a store.", failed)
	}

	_, _, err = s.Append(write)
	if err != nil {
		t.Fatalf("\t\t\t%s Should expect no error when appending to store: %s", failed, err.Error())
	}

	f, beforeSize, err := openFile(f.Name())
	if err != nil {
		t.Fatalf("\t\t\t%s Should expect no error when retrieving the store file size: %s", failed, err.Error())
	}

	t.Log("Given the need to close the store.")
	{
		testID := 1
		t.Logf("\t\tTest %d:\t When closing the store:", testID)
		{
			err := s.Close()
			if err != nil {
				t.Fatalf("\t\t\t%s Should expect no error: %s", failed, err.Error())
			}
			t.Logf("\t\t\t%s Should expect no error.", success)

			_, afterSize, err := openFile(f.Name())
			if err != nil {
				t.Fatalf("\t\t\t%s Should expect no error when retrieving the store file size after closing & re-open: %s", failed, err.Error())
			}
			t.Logf("\t\t\t%s Should expect no error when retrieving the store file size after closing & re-open.", success)
			if !(afterSize > beforeSize) {
				t.Fatalf("\t\t\t%s Should expect the same size of the file after closing & re-open : %s", failed, err.Error())
			}
			t.Logf("\t\t\t%s Should expect the same size of the file after closing & re-open.", success)
		}
	}
}

func openFile(name string) (*os.File, int64, error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, fi.Size(), nil
}
