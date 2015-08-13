package routefinder

import (
	"flag"
	"fmt"
	"testing"
)

func Example() {
	r, _ := NewRoutefinder(
		// Plain, static paths.
		"/",
		"/welcome",

		// Parse out a variable from the name.
		"/pay/:cardName",

		// Match all paths under /shop/:item/ and include the path as if the
		// rule for it was written out here.
		"/shop/:item/...",

		// The above rule doesn't catch `/shop/:item` (without the slash), so
		// we need to add that manually for now...
		"/shop/:item/...",

		// Match everything under /static, but ignore the trailing path.
		"/static/???",
	)

	// Plain path
	paths := []string{
		"/",
		"/returns-nothing",
		"/pay/visa",
		"/shop/gopher",
		"/shop/gopher/thumbnail",
		"/static/141029384.css",
	}

	for _, url := range paths {
		path, meta := r.Lookup(url)
		fmt.Printf("%s -> %s %+v\n", url, path, meta)
	}

	// / -> / map[]
	// /returns-nothing ->  map[]
	// /pay/visa -> /pay/:cardName map[cardName:visa]
	// /shop/gopher -> /shop/:item map[]
	// /shop/gopher/thumbnail -> /shop/:item/thumbnail map[item:gopher]
	// /static/141029384.css -> /static/??? map[]
}

func TestBasic(t *testing.T) {
	r, err := NewRoutefinder("/foo/:id/...", "/foo/:id", "/foo", "/bar/...", "/discard-trail/:foo/???")

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
		{
			p:  "/bar/baz",
			t:  "/bar/baz",
			kv: map[string]string{},
		},
		{
			p:  "/bar/",
			t:  "/bar/",
			kv: map[string]string{},
		},
		/* Would love to get this case in, but it does look to cause some
		 * corner-cases that I'm too tired to reason about for now...
		        {
					p:  "/bar",
					t:  "/bar",
					kv: map[string]string{},
				},
		*/

		{
			p:  "/foo?abc=def",
			t:  "/foo",
			kv: map[string]string{},
		},

		// Discard ???-paths
		{
			p:  "/discard-trail/123",
			t:  "/discard-trail/:foo/???",
			kv: map[string]string{"foo": "123"},
		},
		{
			p:  "/discard-trail/123/",
			t:  "/discard-trail/:foo/???",
			kv: map[string]string{"foo": "123"},
		},
		{
			p:  "/discard-trail/123/a/b/c",
			t:  "/discard-trail/:foo/???",
			kv: map[string]string{"foo": "123"},
		},
	}

	for _, tt := range tests {
		templ, meta := r.Lookup(tt.p)

		if templ != tt.t {
			t.Errorf("Expected to get route `%s` from `%s`, got `%s`", tt.t, tt.p, templ)
		}

		for key, value := range tt.kv {
			if data, ok := meta[key]; !ok || data != value {
				t.Errorf("Expected to get `%+v`, got `%+v`", tt.kv, meta)
			}
		}

		for key, value := range meta {
			if data, ok := tt.kv[key]; !ok || data != value {
				t.Errorf("Unexpected `%+v`, should have `%+v`", meta, tt.kv)
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

	// Adding empty things shouldn't change a thing
	r.Set("")
	r.Set("")
	if r.String() != "" {
		t.Errorf("Expected ``, got `%s`", r.String())
	}

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

func ExampleRoutefinder_Set() {
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
