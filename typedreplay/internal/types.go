package internal

import "strings"

// Namespace describes a replay namespace to generate.
type Namespace struct {
	PackageName string
	Name        string
	Workflows   []Workflow
	Activities  []Activity
	Imports     map[string]string
}

// Workflow describes a parsed typedreplay.Workflow instance.
type Workflow struct {
	Name        string
	Description string
	Input       PBType
	Signals     []Signal
	Outputs     []Output
}

// Activity describes a parsed typedreplay.Activity instance.
type Activity struct {
	Name        string
	Description string
	FuncName    string
	Input       PBType
	Output      PBType
}

// Signal describes a parsed typedreplay.Signal instance.
type Signal struct {
	Name        string
	Description string
	Enum        int
	Message     PBType
}

// Output describes a parsed typedreplay.Output instance.
type Output struct {
	Name        string
	Description string
	Message     PBType
}

// PBType describes a parsed protobuf type.
type PBType struct {
	Package string
	Name    string
}

func (w Workflow) Pascal() string {
	return toPascal(w.Name)
}

func (w Workflow) Camel() string {
	return toCamel(w.Name)
}

func (a Activity) Pascal() string {
	return toPascal(a.Name)
}

func (a Activity) Camel() string {
	return toCamel(a.Name)
}

func (a Activity) FuncTitle() string {
	return strings.Title(a.FuncName)
}

func (s Signal) Camel() string {
	return toCamel(s.Name)
}

func (s Signal) Pascal() string {
	return toPascal(s.Name)
}

func (o Output) Pascal() string {
	return toPascal(o.Name)
}

func (t PBType) String() string {
	if t.Package == "" {
		return t.Name
	}
	return t.Package + "." + t.Name
}
