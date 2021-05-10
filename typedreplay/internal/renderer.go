package internal

import (
	"bytes"
	_ "embed"
	"go/format"
	"regexp"
	"strings"
	"text/template"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"golang.org/x/tools/imports"
)

//go:embed template.go.tpl

var tpl []byte

func Render(ns Namespace, filename string, debug bool) ([]byte, error) {
	funcMap := template.FuncMap{
		"inc": func(i int) int {
			return i + 1
		},
	}

	t, err := template.New("").Funcs(funcMap).Parse(string(tpl))
	if err != nil {
		return nil, errors.Wrap(err, "parse template")
	}

	var buf bytes.Buffer
	err = t.Execute(&buf, ns)
	if err != nil {
		return nil, errors.Wrap(err, "exec template")
	}

	src, err := format.Source(buf.Bytes())
	if err != nil {
		if debug {
			return buf.Bytes(), nil
		}
		return nil, errors.Wrap(err, "gofmt fail (maybe try with -debug=true)")
	}

	src, err = imports.Process(filename, src, &imports.Options{Comments: true})
	if err != nil {
		if debug {
			return buf.Bytes(), nil
		}
		return nil, errors.Wrap(err, "imports failed (maybe try with -debug=true)")
	}

	return src, nil

}

func Validate(ns Namespace) error {
	if len(ns.Workflows) == 0 {
		return errors.New("no workflows found")
	}

	type name struct {
		Label string
		Value string
	}

	type typ struct {
		Label string
		Value PBType
	}

	var (
		names []name
		descs []name
		types []typ
	)
	names = append(names, name{Label: "namespace", Value: ns.Name})

	for _, w := range ns.Workflows {
		names = append(names, name{Label: "workflow", Value: w.Name})
		descs = append(descs, name{Label: "workflow " + w.Name, Value: w.Description})
		types = append(types, typ{Label: "workflow " + w.Name + " input", Value: w.Input})

		uniq := make(map[int]struct{})
		for _, s := range w.Signals {
			if _, ok := uniq[s.Enum]; ok {
				return errors.New("duplicate signal enum", j.MKV{"signal": s.Name, "enum": s.Enum})
			} else if s.Enum <= 0 {
				return errors.New("non-positive signal enum", j.MKV{"signal": s.Name, "enum": s.Enum})
			}
			uniq[s.Enum] = struct{}{}

			names = append(names, name{Label: "signal", Value: s.Name})
			descs = append(descs, name{Label: "signal " + s.Name, Value: s.Description})
			types = append(types, typ{Label: "signal " + s.Name + " message", Value: s.Message})
		}
	}

	for _, a := range ns.Activities {
		names = append(names, name{Label: "activity", Value: a.Name})
		descs = append(descs, name{Label: "activity " + a.Name, Value: a.Description})
		types = append(types, typ{Label: "activity " + a.Name + " input", Value: a.Input})
		types = append(types, typ{Label: "activity " + a.Name + " output", Value: a.Output})
		if a.FuncName == "" {
			return errors.New("activity function empty", j.MKS{"activity": a.Name})
		}
		// TODO(corver): Validate activity function
		// TODO(corver): Ensure all activity backends are compatible
	}

	uniq := make(map[string]struct{})
	for _, n := range names {
		if n.Value == "" {
			return errors.New("name empty", j.MKS{"label": n.Label})
		} else if _, ok := uniq[n.Value]; ok {
			return errors.New("duplicate name", j.MKV{"name": n.Value})
		}
		uniq[n.Value] = struct{}{}

		snake := toSnake(n.Value)
		if snake != n.Value {
			return errors.New("name not snake case", j.MKS{"label": n.Label, "suggest": snake})
		}
	}

	for _, d := range descs {
		if d.Value == "" {
			return errors.New("description empty", j.MKS{"label": d.Label})
		}
	}

	for _, t := range types {
		if t.Value.String() == "" {
			return errors.New("type empty", j.MKS{"label": t.Label})
		}
	}

	return nil
}

var link = regexp.MustCompile("(^[A-Za-z])|_([A-Za-z])")
var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func toSnake(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func toPascal(str string) string {
	return link.ReplaceAllStringFunc(str, func(s string) string {
		return strings.ToUpper(strings.Replace(s, "_", "", -1))
	})
}

func toCamel(str string) string {
	str = toPascal(str)
	if len(str) <= 1 {
		return str
	}

	return strings.ToLower(string(str[0])) + str[1:]
}
