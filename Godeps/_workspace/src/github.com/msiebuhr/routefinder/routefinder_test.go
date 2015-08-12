package routefinder

import (
	"flag"
	"fmt"
	"testing"
)

func Example() {
	r, _ := NewRoutefinder("/shop/:item", "/shop/:item/rate", "/shop/:item/buy")

	fmt.Println(r.Lookup("/shop/gopher/rate"))
	// Output: /shop/:item/rate map[item:gopher]
}

func TestBasic(t *testing.T) {
	r, err := NewRoutefinder("/foo/:id/a", "/foo/:id/b", "/foo/:id", "/foo")

	if err != nil {
		t.Fatal("Unexpected error creating routes", err)
	}

	tests := []struct {
		p  string
		t  string
		kv map[string]string
	}{
		{
			p:  "/",
			t:  "",
			kv: map[string]string{},
		},
		{
			p:  "/foo/foo/a",
			t:  "/foo/:id/a",
			kv: map[string]string{"id": "foo"},
		},
		{
			p:  "/foo",
			t:  "/foo",
			kv: map[string]string{},
		},
		{
			p:  "/fooo",
			t:  "",
			kv: map[string]string{},
		},
	}

	for _, tt := range tests {

		templ, meta := r.Lookup(tt.p)

		if templ != tt.t {
			t.Errorf("Expected to get route %s, got %s", tt.t, templ)
		}

		for key, value := range tt.kv {
			if data, ok := meta[key]; !ok || data != value {
				t.Errorf("Expected to get %+v, got %+v", tt.kv, meta)
			}
		}
	}
}

func TestPrecedence(t *testing.T) {
	r, _ := NewRoutefinder("/:a", "/:b")

	templ, _ := r.Lookup("/xxxxx")

	if templ != "/:a" {
		t.Errorf("Expected to get route /:a, got %s", templ)
	}
}

func TestStringer(t *testing.T) {
	r, _ := NewRoutefinder("/:a", "/:b")

	if r.String() != "/:a,/:b" {
		t.Errorf("Expected to get `/:a,/:b`, got %s", r.String())
	}
}

func TestSet(t *testing.T) {
	r, _ := NewRoutefinder()

	r.Set("/a,/a/:id")

	if r.String() != "/a,/a/:id" {
		t.Errorf("Expected `/a,/a/:id`, got `%s`", r.String())
	}

	templ, params := r.Lookup("/a/123")
	if data, ok := params["id"]; templ != "/a/:id" && !ok && data == "123" {
		t.Errorf("Expected `/a/:id` and id=123, got `%s` and `%v`", templ, params)
	}

	r.Set("/foo/:bar")

	if r.String() != "/a,/a/:id,/foo/:bar" {
		t.Errorf("Expected `/a,/a/:id,/foo/:bar`, got `%s`", r.String())
	}
}

func ExampleLookup_Set() {
	// Create a Routefinder and set it up as a Var-flag
	var routes Routefinder
	flag.Var(&routes, "routes", "comma-separated list of intervals")

	// Pretend the user added -routes "/u,/u/:id"
	flag.Set("routes", "/u,/u/:id")

	// Parse the flags and try it out with a small example
	flag.Parse()
	route, id := routes.Lookup("/u/123")
	fmt.Println(route, id)
	// Output: /u/:id map[id:123]
}

func BenchmarkLookupLast(b *testing.B) {
	r, _ := NewRoutefinder("/item/:id", "/item/:id/share/:network", "/item/:id/buy")

	for i := 0; i < b.N; i++ {
		r.Lookup("/item/123/buy")
	}
}

func BenchmarkLookupHitFirst(b *testing.B) {
	r, _ := NewRoutefinder("/item/:id", "/item/:id/share/:network", "/item/:id/buy")

	for i := 0; i < b.N; i++ {
		r.Lookup("/item/123")
	}
}

func BenchmarkLookupMiss(b *testing.B) {
	r, _ := NewRoutefinder("/item/:id", "/item/:id/share/:network", "/item/:id/buy", "/o")

	for i := 0; i < b.N; i++ {
		r.Lookup("/other")
	}
}
