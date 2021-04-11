package replay

type options struct {
	nameFunc func(interface{}) string
}

type option func(*options)

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
