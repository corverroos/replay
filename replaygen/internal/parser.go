package internal

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
)

func Parse(file string) (Namespace, error) {
	fset := token.NewFileSet()

	f, err := os.Open(file)
	if err != nil {
		return Namespace{}, err
	}
	defer f.Close()

	afile, err := parser.ParseFile(fset, "", f, 0)
	if err != nil {
		return Namespace{}, err
	}

	for _, decl := range afile.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		if gd.Tok != token.VAR {
			continue
		}
		for _, spec := range gd.Specs {
			val, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}

			cl, ok := val.Values[0].(*ast.CompositeLit)
			if !ok {
				continue
			}

			if !isSelector(cl.Type, "replaygen", "Namespace") {
				continue
			}

			return parseNamespace(fset, cl, afile.Name.Name)
		}
	}

	return Namespace{}, nil
}

func parseNamespace(fset *token.FileSet, cl *ast.CompositeLit, pkgName string) (Namespace, error) {
	res := Namespace{PackageName: pkgName}
	var err error

	for _, elt := range cl.Elts {
		kv := elt.(*ast.KeyValueExpr)

		switch kv.Key.(*ast.Ident).Name {
		case "Name":
			res.Name, err = parseName(kv.Value)
			if err != nil {
				return Namespace{}, errors.Wrap(err, "namespace name")
			}
		case "Workflows":
			res.Workflows, err = parseWorkflows(fset, kv.Value)
			if err != nil {
				return Namespace{}, errors.Wrap(err, "workflows")
			}
		case "Activities":
			res.Activities, err = parseActivities(fset, kv.Value)
			if err != nil {
				return Namespace{}, errors.Wrap(err, "activities")
			}
		default:
			return Namespace{}, errors.New("unknown field", j.KS("field", gofmt(fset, kv.Key)))
		}
	}

	return res, nil
}

func parseWorkflows(fset *token.FileSet, ex ast.Expr) ([]Workflow, error) {
	cl, ok := ex.(*ast.CompositeLit)
	if !ok {
		return nil, errors.New("workflows not composite literal")
	}
	typ, ok := cl.Type.(*ast.ArrayType)
	if !ok || !isSelector(typ.Elt, "replaygen", "Workflow") {
		return nil, errors.New("workflows not slice composite literal")
	}
	var res []Workflow
	for _, ex := range cl.Elts {
		act, err := parseWorkflow(fset, ex)
		if err != nil {
			return nil, err
		}
		res = append(res, act)
	}
	return res, nil
}

func parseWorkflow(fset *token.FileSet, ex ast.Expr) (Workflow, error) {
	cl, ok := ex.(*ast.CompositeLit)
	if !ok {
		return Workflow{}, errors.New("workflow not composite literal")
	}

	var (
		res Workflow
		err error
	)
	for _, elt := range cl.Elts {
		kv := elt.(*ast.KeyValueExpr)

		switch kv.Key.(*ast.Ident).Name {
		case "Name":
			res.Name, err = parseName(kv.Value)
			if err != nil {
				return Workflow{}, errors.Wrap(err, "workflow name")
			}
		case "Input":
			res.Input, err = parseType(kv.Value)
			if err != nil {
				return Workflow{}, errors.Wrap(err, "workflow input", j.KS("workflow", res.Name))
			}
		case "Signals":
			res.Signals, err = parseSignals(fset, kv.Value)
			if err != nil {
				return Workflow{}, errors.Wrap(err, "workflow signals", j.KS("workflow", res.Name))
			}
		case "Description":
			res.Description, err = parseName(kv.Value)
			if err != nil {
				return Workflow{}, errors.Wrap(err, "workflow description")
			}
		default:
			return Workflow{}, errors.New("unknown field", j.KS("field", gofmt(fset, kv.Key)))
		}
	}

	return res, nil
}

func parseSignals(fset *token.FileSet, ex ast.Expr) ([]Signal, error) {
	cl, ok := ex.(*ast.CompositeLit)
	if !ok {
		return nil, errors.New("signals not composite literal")
	}
	typ, ok := cl.Type.(*ast.ArrayType)
	if !ok || !isSelector(typ.Elt, "replaygen", "Signal") {
		return nil, errors.New("signals not slice composite literal")
	}
	var res []Signal
	for _, ex := range cl.Elts {
		sig, err := parseSignal(fset, ex)
		if err != nil {
			return nil, err
		}
		res = append(res, sig)
	}
	return res, nil
}

func parseSignal(fset *token.FileSet, ex ast.Expr) (Signal, error) {
	cl, ok := ex.(*ast.CompositeLit)
	if !ok {
		return Signal{}, errors.New("signal not composite literal")
	}

	opt := j.KS("signal", gofmt(fset, cl))
	var (
		res Signal
		err error
	)

	for _, elt := range cl.Elts {
		kv := elt.(*ast.KeyValueExpr)

		switch kv.Key.(*ast.Ident).Name {
		case "Name":
			res.Name, err = parseName(kv.Value)
			if err != nil {
				return Signal{}, errors.Wrap(err, "signal name", opt)
			}
		case "Enum":
			res.Enum, err = parseInt(kv.Value)
			if err != nil {
				return Signal{}, errors.Wrap(err, "signal enum", opt)
			}
		case "Message":
			res.Message, err = parseType(kv.Value)
			if err != nil {
				return Signal{}, errors.Wrap(err, "signal message", opt)
			}
		case "Description":
			res.Description, err = parseName(kv.Value)
			if err != nil {
				return Signal{}, errors.Wrap(err, "signal description")
			}
		default:
			return Signal{}, errors.New("unknown field", j.KS("field", gofmt(fset, kv.Key)))
		}
	}

	return res, nil
}

func parseActivities(fset *token.FileSet, ex ast.Expr) ([]Activity, error) {
	cl, ok := ex.(*ast.CompositeLit)
	if !ok {
		return nil, errors.New("activities not composite literal")
	}
	typ, ok := cl.Type.(*ast.ArrayType)
	if !ok || !isSelector(typ.Elt, "replaygen", "Activity") {
		return nil, errors.New("activities not slice composite literal")
	}
	var res []Activity
	for _, ex := range cl.Elts {
		act, err := parseActivity(fset, ex)
		if err != nil {
			return nil, err
		}
		res = append(res, act)
	}
	return res, nil
}

func parseActivity(fset *token.FileSet, ex ast.Expr) (Activity, error) {
	cl, ok := ex.(*ast.CompositeLit)
	if !ok {
		return Activity{}, errors.New("activity not composite literal")
	}

	opt := j.KS("activity", gofmt(fset, cl))
	var (
		res Activity
		err error
	)

	for _, elt := range cl.Elts {
		kv := elt.(*ast.KeyValueExpr)

		switch kv.Key.(*ast.Ident).Name {
		case "Name":
			res.Name, err = parseName(kv.Value)
			if err != nil {
				return Activity{}, errors.Wrap(err, "activity name", opt)
			}
		case "Func":
			ident, ok := kv.Value.(*ast.Ident)
			if !ok || ident.Obj == nil {
				return Activity{}, errors.New("activity func not defined in same file",
					j.KS("activity", gofmt(fset, cl)))
			}

			res.FuncName = ident.Name
			res.Input, res.Output, err = parseActivityFunc(fset, ident.Obj)
			if err != nil {
				return Activity{}, errors.Wrap(err, "activity func", j.KS("name", ident.Name))
			}
		case "Description":
			res.Description, err = parseName(kv.Value)
			if err != nil {
				return Activity{}, errors.Wrap(err, "activity description")
			}
		default:
			return Activity{}, errors.New("unknown field", j.KS("field", gofmt(fset, kv.Key)))
		}
	}
	return res, nil
}

func parseActivityFunc(fset *token.FileSet, obj *ast.Object) (input PBType, output PBType, err error) {
	fd, ok := obj.Decl.(*ast.FuncDecl)
	if obj.Kind != ast.Fun || !ok || fd.Type == nil {
		return PBType{}, PBType{}, errors.New("activity not a function")
	}

	if fd.Type.Params.NumFields() != 4 {
		return PBType{}, PBType{}, errors.New("activity function not 4 input params")
	} else if fd.Type.Results.NumFields() != 2 {
		return PBType{}, PBType{}, errors.New("activity function not 2 output params")
	}

	input, err = parseParamType(fset, fd.Type.Params.List[3])
	if err != nil {
		return PBType{}, PBType{}, errors.Wrap(err, "input param")
	}

	output, err = parseParamType(fset, fd.Type.Results.List[0])
	if err != nil {
		return PBType{}, PBType{}, errors.Wrap(err, "output param")
	}

	return input, output, err
}

func parseParamType(fset *token.FileSet, field *ast.Field) (PBType, error) {
	ste, ok := field.Type.(*ast.StarExpr)
	if !ok {
		return PBType{}, errors.New("proto param type not a pointer")
	}

	if ident, ok := ste.X.(*ast.Ident); ok {
		return PBType{Name: ident.Name}, nil
	}

	se, ok := ste.X.(*ast.SelectorExpr)
	if !ok {
		return PBType{}, errors.New("proto param invalid")
	}

	pkgIdent, ok := se.X.(*ast.Ident)
	if !ok {
		return PBType{}, errors.New("proto param invalidPBType)")
	}

	return PBType{
		Package: pkgIdent.Name,
		Name:    se.Sel.Name,
	}, nil
}

func parseName(ex ast.Expr) (string, error) {
	bl, ok := ex.(*ast.BasicLit)
	if !ok || bl.Kind != token.STRING {
		return "", errors.New("name not string literal")
	}
	return strings.Trim(bl.Value, "\""), nil
}

func parseInt(ex ast.Expr) (int, error) {
	bl, ok := ex.(*ast.BasicLit)
	if !ok || bl.Kind != token.INT {
		return 0, errors.New("int not int literal")
	}
	return strconv.Atoi(strings.Trim(bl.Value, "\""))
}

func parseType(ex ast.Expr) (PBType, error) {
	c, ok := ex.(*ast.CallExpr)
	if !ok || !isIdent(c.Fun, "new") || len(c.Args) != 1 {
		return PBType{}, errors.New("type not defined as new(pb.PBType)")
	}

	if ident, ok := c.Args[0].(*ast.Ident); ok {
		return PBType{Name: ident.Name}, nil
	}

	se, ok := c.Args[0].(*ast.SelectorExpr)
	if !ok {
		return PBType{}, errors.New("type not defined as new(pb.PBType)")
	}

	pkgIdent, ok := se.X.(*ast.Ident)
	if !ok {
		return PBType{}, errors.New("type not defined as new(pb.PBType)")
	}

	return PBType{
		Package: pkgIdent.Name,
		Name:    se.Sel.Name,
	}, nil
}

func gofmt(fset *token.FileSet, node ast.Node) string {
	if node == nil {
		return "<nil>"
	}

	var b bytes.Buffer
	err := format.Node(&b, fset, node)
	if err != nil {
		panic(errors.Wrap(err, "error formatting node"))
	}
	return b.String()
}

func isIdent(ex ast.Expr, name string) bool {
	ident, ok := ex.(*ast.Ident)
	if !ok {
		return false
	}

	return ident.Name == name
}

func isSelector(ex ast.Expr, x, sel string) bool {
	se, ok := ex.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	if !isIdent(se.X, x) {
		return false
	}

	return se.Sel.Name == sel
}
