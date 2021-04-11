package replay

type options struct {
	nameFunc func(interface{}) string
}

type option func(*options)

// WithName returns an option to explicitly define a workflow or activity name.
// Default behavior infers function names via reflection.
func WithName(name string) option {
	return func(o *options) {
		o.nameFunc = func(_ interface{}) string {
			return name
		}
	}
}

func defaultOptions() options {
	return options{
		nameFunc: getFunctionName,
	}
}
