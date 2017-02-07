package test

import (
	crand "crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/l-vitaly/consul"
	"github.com/l-vitaly/consul/testutil"
	"github.com/l-vitaly/gounit"
)

func makeTestClient() (consul.Client, error) {
	return testutil.NewClient()
}

func testKey() string {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("Failed to read random bytes: %v", err))
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}

func TestWatchPut(t *testing.T) {
	u := gounit.New(t)

	key := testKey()

	client, err := makeTestClient()
	u.AssertNotError(err, "")

	value := "hello"

	_, err = client.Put(key, value)
	u.AssertNotError(err, "")

	kv, _, err := client.Get(key)
	u.AssertNotError(err, "")
	u.AssertNotNil(kv, "")
}

func TestWatchGet(t *testing.T) {
	u := gounit.New(t)

	key := testKey()

	client, err := makeTestClient()
	u.AssertNotError(err, "")

	ch := client.WatchGet(key)

	value := "test"

	go func() {
		time.Sleep(100 * time.Millisecond)

		_, err := client.Put(key, value)
		u.AssertNotError(err, "put error")
	}()

	kv := <-ch

	u.AssertNotNil(kv, "key/value")
	u.AssertEquals(value, string(kv.Value), "")
}
