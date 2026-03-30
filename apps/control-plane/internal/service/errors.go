package service

type ErrInvalidArgument struct {
	Message string
}

func (e ErrInvalidArgument) Error() string {
	return e.Message
}

type ErrNotFound struct {
	Resource string
}

func (e ErrNotFound) Error() string {
	if e.Resource == "" {
		return "resource not found"
	}
	return e.Resource + " not found"
}
